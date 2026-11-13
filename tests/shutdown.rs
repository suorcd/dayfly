//! Process-level tests for graceful shutdown in `serve` mode.
//!
//! The tests start the real binary against the fake S3 service. One test
//! holds an upload inside S3, sends `SIGTERM`, and checks the drain, both
//! listeners, and the exit status.

#![cfg(unix)]

mod common;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, ChildStderr, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use common::{FakeS3, Gate};

/// A running `dayfly serve` process that stops on drop.
struct DayflyServer {
    child: Option<Child>,
    stderr: Option<ChildStderr>,
}

impl DayflyServer {
    fn spawn(fake: &FakeS3, public_port: u16, metrics_port: Option<u16>) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_dayfly"));
        command
            .env_clear()
            .arg("--verbose")
            .arg("serve")
            .arg("--bind")
            .arg(format!("127.0.0.1:{public_port}"))
            .env("S3_ENDPOINT", &fake.endpoint)
            .env("S3_BUCKET", &fake.bucket)
            .env("S3_REGION", "us-east-1")
            .env("S3_ACCESS_KEY_ID", "test-access-key")
            .env("S3_SECRET_ACCESS_KEY", "test-secret-key")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        if let Some(metrics_port) = metrics_port {
            command.env("DAYFLY_METRICS_BIND", format!("127.0.0.1:{metrics_port}"));
        }
        let mut child = command.spawn().expect("dayfly serve must start");
        let stderr = child.stderr.take().expect("stderr must be piped");
        Self {
            child: Some(child),
            stderr: Some(stderr),
        }
    }

    fn id(&self) -> u32 {
        self.child.as_ref().expect("child must be present").id()
    }

    fn try_wait(&mut self) -> Option<ExitStatus> {
        self.child
            .as_mut()
            .expect("child must be present")
            .try_wait()
            .expect("the child status must be readable")
    }

    fn wait_for_exit(&mut self, timeout: Duration) -> ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.try_wait() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "dayfly did not exit within {timeout:?}"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn read_stderr(&mut self) -> String {
        let Some(mut stderr) = self.stderr.take() else {
            return String::new();
        };
        let mut text = String::new();
        stderr
            .read_to_string(&mut text)
            .expect("stderr must be readable");
        text
    }
}

impl Drop for DayflyServer {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Reserve two ephemeral loopback ports.
fn free_ports() -> (u16, u16) {
    let first = TcpListener::bind("127.0.0.1:0").expect("a loopback port must bind");
    let second = TcpListener::bind("127.0.0.1:0").expect("a second loopback port must bind");
    (
        first.local_addr().expect("port must resolve").port(),
        second.local_addr().expect("port must resolve").port(),
    )
}

fn address(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn wait_until(mut condition: impl FnMut() -> bool, message: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < deadline, "{message}");
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_until_listening(port: u16) {
    wait_until(
        || TcpStream::connect_timeout(&address(port), Duration::from_millis(100)).is_ok(),
        &format!("dayfly did not listen on port {port}"),
    );
}

/// Return true when a new request cannot reach a live listener.
fn listener_has_stopped(port: u16) -> bool {
    let Ok(mut stream) = TcpStream::connect_timeout(&address(port), Duration::from_millis(100))
    else {
        return true;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
    let _ = stream.write_all(b"GET / HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n");
    let mut byte = [0u8; 1];
    matches!(stream.read(&mut byte), Ok(0) | Err(_))
}

/// Send one upload and wait for the complete response.
fn upload(port: u16, body: &[u8]) -> std::io::Result<String> {
    let mut stream = TcpStream::connect_timeout(&address(port), Duration::from_secs(5))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    write!(
        stream,
        "POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
    Ok(String::from_utf8_lossy(&response).into_owned())
}

#[test]
fn sigterm_drains_an_in_flight_request_and_stops_both_listeners() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("test runtime must build");
    let fake = runtime.block_on(FakeS3::start());
    let (public_port, metrics_port) = free_ports();
    let mut server = DayflyServer::spawn(&fake, public_port, Some(metrics_port));

    wait_until_listening(public_port);
    wait_until_listening(metrics_port);

    fake.hold(Gate::PutObject);
    let request = thread::spawn(move || upload(public_port, b"graceful shutdown"));
    runtime.block_on(fake.wait_until_blocked(Gate::PutObject));

    let signal = unsafe { libc::kill(server.id() as i32, libc::SIGTERM) };
    assert_eq!(signal, 0, "the SIGTERM must reach the server");

    wait_until(
        || listener_has_stopped(public_port),
        "the public listener must stop accepting new connections",
    );
    wait_until(
        || listener_has_stopped(metrics_port),
        "the metrics listener must stop accepting new connections",
    );
    assert!(
        server.try_wait().is_none(),
        "the server must stay alive while it drains the in-flight request"
    );

    fake.release();
    let response = request
        .join()
        .expect("the upload thread must not panic")
        .expect("the upload request must complete during the drain");
    assert!(
        response.starts_with("HTTP/1.1 201 Created"),
        "the drained upload must succeed: {response}"
    );
    assert!(
        response.contains("/v1/pastes/"),
        "the drained upload must return a paste URL: {response}"
    );

    let status = server.wait_for_exit(Duration::from_secs(10));
    let stderr = server.read_stderr();
    assert_eq!(
        status.code(),
        Some(0),
        "a requested shutdown must exit 0: {status:?}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("SIGTERM"),
        "the log must name the shutdown reason: {stderr}"
    );
    assert!(
        stderr.contains("drain"),
        "the log must report the drain result: {stderr}"
    );
}

#[test]
fn sigint_stops_the_public_listener_and_exits_zero() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("test runtime must build");
    let fake = runtime.block_on(FakeS3::start());
    let (public_port, _) = free_ports();
    let mut server = DayflyServer::spawn(&fake, public_port, None);

    wait_until_listening(public_port);
    let signal = unsafe { libc::kill(server.id() as i32, libc::SIGINT) };
    assert_eq!(signal, 0, "the SIGINT must reach the server");

    wait_until(
        || listener_has_stopped(public_port),
        "the public listener must stop accepting new connections",
    );
    let status = server.wait_for_exit(Duration::from_secs(10));
    let stderr = server.read_stderr();
    assert_eq!(
        status.code(),
        Some(0),
        "a requested shutdown must exit 0: {status:?}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("SIGINT"),
        "the log must name the shutdown reason: {stderr}"
    );
}
