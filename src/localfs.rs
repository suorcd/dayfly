//! Race-safe open of server-local paths under a configured root.
//!
//! A path check and a later open are two separate steps. A symlink can
//! change between them, so the check alone cannot hold. The functions
//! here close that gap: they open the path, then verify the opened
//! descriptor itself. A descriptor pins its inode, so a later swap
//! cannot change what it reads, and a swap before the open is visible
//! in the descriptor path.
//!
//! A download writes a temporary file next to its destination and
//! renames it onto the destination only after a successful flush. A
//! failed download leaves no partial file, and an existing destination
//! is never truncated before the download succeeds.

use std::ffi::{CString, OsStr};
use std::fs::File;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Failure to open or create a local path under a root.
#[derive(Debug)]
pub enum LocalFsError {
    /// The path escapes the configured root.
    OutsideRoot,
    /// An operating system error.
    Io(io::Error),
}

impl From<io::Error> for LocalFsError {
    fn from(error: io::Error) -> Self {
        LocalFsError::Io(error)
    }
}

impl std::fmt::Display for LocalFsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LocalFsError::OutsideRoot => write!(f, "path escapes the configured root"),
            LocalFsError::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for LocalFsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LocalFsError::OutsideRoot => None,
            LocalFsError::Io(error) => Some(error),
        }
    }
}

/// Open `path` for read under `root`.
///
/// The open follows symlinks, then the opened descriptor is verified
/// against the root. A symlink that points outside the root fails the
/// check. A symlink that stays inside the root is allowed: the
/// descriptor pins the file it opened.
pub fn open_read_under_root(root: &Path, path: &Path) -> Result<File, LocalFsError> {
    let canon_root = root.canonicalize()?;
    let file = File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(LocalFsError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("not a regular file: {}", path.display()),
        )));
    }
    verify_under_root(&file, &canon_root)?;
    Ok(file)
}

/// Check that an opened descriptor names a path under `root`.
///
/// The descriptor pins its inode, so a swap after the open cannot
/// change what it reads. A swap before the open shows in the
/// descriptor path and fails this check.
fn verify_under_root(file: &File, root: &Path) -> Result<(), LocalFsError> {
    let real = descriptor_path(file).map_err(|_| LocalFsError::OutsideRoot)?;
    if real.starts_with(root) {
        Ok(())
    } else {
        Err(LocalFsError::OutsideRoot)
    }
}

/// A temporary file next to its destination.
///
/// The file is renamed onto the destination only by [`TempDest::commit`].
/// A drop without a commit removes the temporary file, so a failed
/// download leaves no partial file behind.
#[derive(Debug)]
pub struct TempDest {
    dir: Option<File>,
    tmp_name: String,
    dest_name: std::ffi::OsString,
    dest: PathBuf,
    committed: bool,
}

/// Counter for temporary file names, so two downloads to one directory
/// never share a name.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A temporary file name next to `dest`, hidden and unique.
fn temp_name(dest: &Path) -> Result<String, LocalFsError> {
    let name = dest.file_name().ok_or_else(|| {
        LocalFsError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "destination has no file name",
        ))
    })?;
    let count = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let random: u64 = rand::random();
    Ok(format!(
        ".{}.dayfly-tmp-{}-{count:016x}-{random:016x}",
        name.to_string_lossy(),
        std::process::id()
    ))
}

impl TempDest {
    /// Create a temporary file next to `dest`.
    ///
    /// With a root, the parent directory is opened under that root and
    /// the temporary file is created inside the opened directory, so a
    /// symlink swap of the parent cannot move the file outside the
    /// root. Without a root, the temporary file is created next to the
    /// destination by path.
    ///
    /// Returns the open temporary file and the guard that owns the
    /// rename. Drop the guard without a commit to remove the file.
    pub fn create(root: Option<&Path>, dest: &Path) -> Result<(File, Self), LocalFsError> {
        let dest_name = dest.file_name().ok_or_else(|| {
            LocalFsError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "destination has no file name",
            ))
        })?;
        let parent = dest
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        if let Some(root) = root {
            let canon_root = root.canonicalize()?;
            let dir = open_dir(&parent)?;
            verify_under_root(&dir, &canon_root)?;
            for _ in 0..8 {
                let tmp_name = temp_name(dest)?;
                match create_in_dir(&dir, &tmp_name) {
                    Ok(file) => {
                        return Ok((
                            file,
                            Self {
                                dir: Some(dir),
                                tmp_name,
                                dest_name: dest_name.to_os_string(),
                                dest: dest.to_path_buf(),
                                committed: false,
                            },
                        ));
                    }
                    Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                    Err(e) => return Err(e.into()),
                }
            }
            return Err(LocalFsError::Io(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "no free temporary file name",
            )));
        }
        for _ in 0..8 {
            let tmp_name = temp_name(dest)?;
            let tmp_path = parent.join(&tmp_name);
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp_path)
            {
                Ok(file) => {
                    return Ok((
                        file,
                        Self {
                            dir: None,
                            tmp_name,
                            dest_name: dest_name.to_os_string(),
                            dest: dest.to_path_buf(),
                            committed: false,
                        },
                    ));
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Err(LocalFsError::Io(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "no free temporary file name",
        )))
    }

    /// Rename the temporary file onto the destination.
    ///
    /// A rename replaces the destination itself. When the destination
    /// is a symlink, the link is replaced, not its target.
    pub fn commit(mut self) -> Result<(), LocalFsError> {
        if let Some(dir) = self.dir.as_ref() {
            rename_in_dir(dir, &self.tmp_name, &self.dest_name)?;
            self.committed = true;
            return Ok(());
        }
        let tmp_path = self
            .dest
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
            .join(&self.tmp_name);
        std::fs::rename(&tmp_path, &self.dest)?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for TempDest {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        if let Some(dir) = self.dir.as_ref() {
            let name = CString::new(self.tmp_name.as_str()).unwrap_or_default();
            unsafe {
                libc::unlinkat(dir.as_raw_fd(), name.as_ptr(), 0);
            }
            return;
        }
        let tmp_path = self
            .dest
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
            .join(&self.tmp_name);
        let _ = std::fs::remove_file(&tmp_path);
    }
}

/// Open a directory with `O_DIRECTORY`, so a regular file is refused.
fn open_dir(path: &Path) -> io::Result<File> {
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
        .open(path)
}

/// Create a file inside an opened directory, without following a link.
fn create_in_dir(dir: &File, name: &str) -> io::Result<File> {
    let cname = CString::new(name)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "name holds a NUL byte"))?;
    let fd = unsafe {
        libc::openat(
            dir.as_raw_fd(),
            cname.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o666,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

/// Rename one name onto another inside one opened directory.
fn rename_in_dir(dir: &File, from: &str, to: &OsStr) -> io::Result<()> {
    let cfrom = CString::new(from)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "name holds a NUL byte"))?;
    let cto = CString::new(to.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "name holds a NUL byte"))?;
    let rc = unsafe {
        libc::renameat(
            dir.as_raw_fd(),
            cfrom.as_ptr(),
            dir.as_raw_fd(),
            cto.as_ptr(),
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// The real path of an opened descriptor, for the root check.
fn descriptor_path(file: &File) -> io::Result<PathBuf> {
    std::fs::canonicalize(format!("/proc/self/fd/{}", file.as_raw_fd()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn test_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("dayfly-localfs-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("test directory must be creatable");
        dir
    }

    fn rooted(dir: &Path) -> PathBuf {
        dir.canonicalize().expect("test root must canonicalize")
    }

    #[test]
    fn read_under_root_opens_a_file_inside_the_root() {
        let root = test_dir("read-ok");
        let inside = root.join("note.txt");
        std::fs::write(&inside, b"payload").expect("test file must be writable");
        let mut file =
            open_read_under_root(&rooted(&root), &inside).expect("a file under the root must open");
        let mut text = String::new();
        use std::io::Read;
        file.read_to_string(&mut text)
            .expect("file must be readable");
        assert_eq!(text, "payload");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn read_under_root_refuses_a_symlink_to_a_file_outside_the_root() {
        let root = test_dir("read-escape");
        let outside = test_dir("read-escape-outside");
        let target = outside.join("secret.txt");
        std::fs::write(&target, b"secret").expect("test file must be writable");
        let link = root.join("escape.txt");
        std::os::unix::fs::symlink(&target, &link).expect("symlink must be creatable");
        match open_read_under_root(&rooted(&root), &link) {
            Err(LocalFsError::OutsideRoot) => {}
            other => panic!("a symlink out of the root must be refused, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&outside);
    }

    #[test]
    fn read_under_root_refuses_a_symlink_in_a_parent_component() {
        let root = test_dir("read-parent-escape");
        let outside = test_dir("read-parent-escape-outside");
        let target = outside.join("secret.txt");
        std::fs::write(&target, b"secret").expect("test file must be writable");
        let link = root.join("subdir");
        std::os::unix::fs::symlink(&outside, &link).expect("symlink must be creatable");
        let path = link.join("secret.txt");
        match open_read_under_root(&rooted(&root), &path) {
            Err(LocalFsError::OutsideRoot) => {}
            other => panic!("a symlinked parent out of the root must be refused, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&outside);
    }

    #[test]
    fn read_under_root_allows_a_symlink_that_stays_inside_the_root() {
        let root = test_dir("read-inside-link");
        let target = root.join("real.txt");
        std::fs::write(&target, b"inside").expect("test file must be writable");
        let link = root.join("alias.txt");
        std::os::unix::fs::symlink(&target, &link).expect("symlink must be creatable");
        let mut file = open_read_under_root(&rooted(&root), &link)
            .expect("a symlink inside the root must open");
        let mut text = String::new();
        use std::io::Read;
        file.read_to_string(&mut text)
            .expect("file must be readable");
        assert_eq!(text, "inside");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn read_under_root_refuses_a_path_outside_the_root() {
        let root = test_dir("read-outside");
        let outside = test_dir("read-outside-file");
        let target = outside.join("secret.txt");
        std::fs::write(&target, b"secret").expect("test file must be writable");
        match open_read_under_root(&rooted(&root), &target) {
            Err(LocalFsError::OutsideRoot) => {}
            other => panic!("an absolute path outside the root must be refused, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&outside);
    }

    #[test]
    fn read_under_root_refuses_a_directory() {
        let root = test_dir("read-dir");
        match open_read_under_root(&rooted(&root), &root) {
            Err(LocalFsError::Io(_)) => {}
            other => panic!("a directory must be refused, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn temp_dest_commit_renames_onto_the_destination() {
        let dir = test_dir("commit");
        let dest = dir.join("out.bin");
        std::fs::write(&dest, b"old").expect("destination must be writable");
        let (mut file, guard) = TempDest::create(None, &dest).expect("temp file must be creatable");
        file.write_all(b"new").expect("temp file must be writable");
        drop(file);
        assert_eq!(
            std::fs::read(&dest).expect("destination must still exist"),
            b"old",
            "the destination is unchanged before commit"
        );
        guard.commit().expect("commit must rename");
        assert_eq!(
            std::fs::read(&dest).expect("destination must exist"),
            b"new"
        );
        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .expect("directory must be readable")
            .map(|entry| entry.expect("entry must be readable").file_name())
            .collect();
        assert_eq!(leftovers, vec![std::ffi::OsString::from("out.bin")]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn temp_dest_drop_removes_the_temporary_file() {
        let dir = test_dir("drop");
        let dest = dir.join("out.bin");
        let (mut file, guard) = TempDest::create(None, &dest).expect("temp file must be creatable");
        file.write_all(b"partial")
            .expect("temp file must be writable");
        drop(file);
        drop(guard);
        assert!(
            !dest.exists(),
            "a dropped guard must not create the destination"
        );
        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .expect("directory must be readable")
            .map(|entry| entry.expect("entry must be readable").file_name())
            .collect();
        assert!(
            leftovers.is_empty(),
            "a dropped guard must leave no file, found {leftovers:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn temp_dest_under_root_refuses_a_symlinked_parent() {
        let root = test_dir("write-parent-escape");
        let outside = test_dir("write-parent-escape-outside");
        let link = root.join("subdir");
        std::os::unix::fs::symlink(&outside, &link).expect("symlink must be creatable");
        let dest = link.join("out.bin");
        match TempDest::create(Some(&rooted(&root)), &dest) {
            Err(LocalFsError::OutsideRoot) => {}
            other => panic!("a symlinked parent out of the root must be refused, got {other:?}"),
        }
        assert!(
            !outside.join("out.bin").exists(),
            "no file may appear outside the root"
        );
        let leftovers: Vec<_> = std::fs::read_dir(&outside)
            .expect("directory must be readable")
            .map(|entry| entry.expect("entry must be readable").file_name())
            .collect();
        assert!(
            leftovers.is_empty(),
            "nothing may be written outside, found {leftovers:?}"
        );
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&outside);
    }

    #[test]
    fn temp_dest_under_root_rename_replaces_a_symlink_not_its_target() {
        let root = test_dir("write-link-dest");
        let outside = test_dir("write-link-dest-outside");
        let target = outside.join("target.txt");
        std::fs::write(&target, b"keep me").expect("test file must be writable");
        let dest = root.join("out.bin");
        std::os::unix::fs::symlink(&target, &dest).expect("symlink must be creatable");
        let (mut file, guard) =
            TempDest::create(Some(&rooted(&root)), &dest).expect("temp file must be creatable");
        file.write_all(b"new").expect("temp file must be writable");
        drop(file);
        guard.commit().expect("commit must rename");
        assert_eq!(
            std::fs::read(&target).expect("target must still exist"),
            b"keep me",
            "the rename must replace the link, not its target"
        );
        assert_eq!(
            std::fs::read(&dest).expect("destination must exist"),
            b"new"
        );
        assert!(
            !std::fs::symlink_metadata(&dest)
                .expect("destination metadata must be readable")
                .file_type()
                .is_symlink(),
            "the destination must be a regular file after commit"
        );
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&outside);
    }
}
