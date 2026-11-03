//! Resolve the client address for per-client policy.
//!
//! The HTTP listener reports the socket peer. Behind a reverse proxy that
//! peer is the proxy, so a per-client limit built on it would put every
//! visitor in one bucket. `DAYFLY_TRUSTED_PROXIES` names the peers whose
//! `X-Forwarded-For` header is believed; with the setting unset the
//! socket peer is the client and the header is ignored.
//!
//! The chain is walked from the right, which is the hop the nearest
//! trusted proxy appended. The first address that is not a trusted proxy
//! is the client. Anything to the left was supplied by the client and is
//! ignored, so a forged leading hop cannot change the result.

use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;

use axum::http::HeaderMap;

/// The most forwarding hops accepted in one request.
///
/// A longer chain is an abuse or misconfiguration signal, not a client
/// list. The request still reaches the service, attributed to the socket
/// peer, and the resolver reports [`ForwardedError::TooManyHops`].
pub const MAX_FORWARDED_HOPS: usize = 32;

/// One CIDR block. A bare address is accepted as a single-host block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cidr {
    network: IpAddr,
    prefix: u8,
}

impl Cidr {
    /// Whether `address` falls inside this block.
    pub fn contains(&self, address: IpAddr) -> bool {
        match (self.network, address) {
            (IpAddr::V4(network), IpAddr::V4(address)) => {
                let mask = ipv4_mask(self.prefix);
                u32::from(network) & mask == u32::from(address) & mask
            }
            (IpAddr::V6(network), IpAddr::V6(address)) => {
                let mask = ipv6_mask(self.prefix);
                u128::from(network) & mask == u128::from(address) & mask
            }
            _ => false,
        }
    }
}

fn ipv4_mask(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    }
}

fn ipv6_mask(prefix: u8) -> u128 {
    if prefix == 0 {
        0
    } else {
        u128::MAX << (128 - prefix)
    }
}

impl FromStr for Cidr {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if value.is_empty() {
            return Err("expected an IP address or CIDR block".to_string());
        }
        let (network, prefix) = match value.split_once('/') {
            Some((network, prefix)) => {
                let network: IpAddr = network
                    .trim()
                    .parse()
                    .map_err(|_| format!("`{network}` is not an IP address"))?;
                let prefix: u8 = prefix
                    .trim()
                    .parse()
                    .map_err(|_| format!("`{prefix}` is not a prefix length"))?;
                (network, prefix)
            }
            None => {
                let network: IpAddr = value
                    .parse()
                    .map_err(|_| format!("`{value}` is not an IP address"))?;
                (network, max_prefix(network))
            }
        };
        let max = max_prefix(network);
        if prefix > max {
            return Err(format!(
                "prefix length {prefix} is too large for {network} (maximum {max})"
            ));
        }
        Ok(Self { network, prefix })
    }
}

fn max_prefix(address: IpAddr) -> u8 {
    match address {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    }
}

/// Why a forwarded chain was not used.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForwardedError {
    /// A header value was not a comma-separated list of IP addresses.
    Malformed,
    /// The chain had more than [`MAX_FORWARDED_HOPS`] entries.
    TooManyHops,
    /// Every hop was itself a trusted proxy, so no client is named.
    NotAttributable,
}

impl fmt::Display for ForwardedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed => write!(f, "malformed X-Forwarded-For header"),
            Self::TooManyHops => write!(f, "X-Forwarded-For header has too many hops"),
            Self::NotAttributable => write!(f, "X-Forwarded-For names only trusted proxies"),
        }
    }
}

/// Resolve the client address for one request.
///
/// `peer` is the socket peer. When it is not a trusted proxy, it is the
/// client and the forwarding header is ignored. When it is trusted, the
/// `X-Forwarded-For` chain is walked from the right; the first address
/// that is not a trusted proxy is returned.
///
/// A missing header falls back to `peer`, which keeps a trusted proxy's
/// health check or a direct connection working. A malformed chain or one
/// that names only trusted proxies returns an error, and the caller is
/// expected to fall back to `peer` with a warning.
pub fn resolve(
    peer: IpAddr,
    headers: &HeaderMap,
    trusted: &[Cidr],
) -> Result<IpAddr, ForwardedError> {
    if !is_trusted(peer, trusted) {
        return Ok(peer);
    }
    let chain = forwarded_chain(headers)?;
    if chain.is_empty() {
        return Ok(peer);
    }
    chain
        .iter()
        .rev()
        .copied()
        .find(|address| !is_trusted(*address, trusted))
        .ok_or(ForwardedError::NotAttributable)
}

fn is_trusted(address: IpAddr, trusted: &[Cidr]) -> bool {
    trusted.iter().any(|block| block.contains(address))
}

fn forwarded_chain(headers: &HeaderMap) -> Result<Vec<IpAddr>, ForwardedError> {
    let mut chain = Vec::new();
    for value in headers.get_all("x-forwarded-for") {
        let value = value.to_str().map_err(|_| ForwardedError::Malformed)?;
        for part in value.split(',') {
            let part = part.trim();
            if part.is_empty() {
                return Err(ForwardedError::Malformed);
            }
            let address: IpAddr = part.parse().map_err(|_| ForwardedError::Malformed)?;
            chain.push(address);
            if chain.len() > MAX_FORWARDED_HOPS {
                return Err(ForwardedError::TooManyHops);
            }
        }
    }
    Ok(chain)
}

/// The resolved client address, attached to the request once.
///
/// The auth log, the rate limiter, and any future per-client quota read
/// this same extension, so they cannot disagree about who the client is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientAddr(pub IpAddr);

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", HeaderValue::from_str(value).unwrap());
        headers
    }

    fn trusted(values: &[&str]) -> Vec<Cidr> {
        values
            .iter()
            .map(|value| value.parse().expect("test CIDR must parse"))
            .collect()
    }

    #[test]
    fn cidr_contains_a_host_and_a_range() {
        let host: Cidr = "10.0.0.5".parse().unwrap();
        assert!(host.contains("10.0.0.5".parse().unwrap()));
        assert!(!host.contains("10.0.0.6".parse().unwrap()));

        let range: Cidr = "10.0.0.0/24".parse().unwrap();
        assert!(range.contains("10.0.0.0".parse().unwrap()));
        assert!(range.contains("10.0.0.255".parse().unwrap()));
        assert!(!range.contains("10.0.1.0".parse().unwrap()));
    }

    #[test]
    fn cidr_contains_ipv6() {
        let range: Cidr = "fd00::/8".parse().unwrap();
        assert!(range.contains("fd12:3456::1".parse().unwrap()));
        assert!(!range.contains("fe80::1".parse().unwrap()));
    }

    #[test]
    fn cidr_accepts_the_whole_address_space() {
        let v4: Cidr = "0.0.0.0/0".parse().unwrap();
        assert!(v4.contains("203.0.113.9".parse().unwrap()));
        let v6: Cidr = "::/0".parse().unwrap();
        assert!(v6.contains("2001:db8::1".parse().unwrap()));
    }

    #[test]
    fn cidr_rejects_a_prefix_that_is_too_large() {
        let error = "10.0.0.0/33".parse::<Cidr>().unwrap_err();
        assert!(error.contains("too large"), "error: {error}");
        let error = "::1/129".parse::<Cidr>().unwrap_err();
        assert!(error.contains("too large"), "error: {error}");
    }

    #[test]
    fn cidr_rejects_garbage() {
        assert!("not-an-address".parse::<Cidr>().is_err());
        assert!("10.0.0.0/nope".parse::<Cidr>().is_err());
        assert!("".parse::<Cidr>().is_err());
    }

    #[test]
    fn untrusted_peer_ignores_the_forwarded_header() {
        let peer = "198.51.100.7".parse().unwrap();
        let address = resolve(peer, &headers("203.0.113.9"), &trusted(&["10.0.0.0/8"]))
            .expect("an untrusted peer is the client");
        assert_eq!(address, peer);
    }

    #[test]
    fn trusted_peer_uses_the_forwarded_address() {
        let address = resolve(
            "10.0.0.5".parse().unwrap(),
            &headers("203.0.113.9"),
            &trusted(&["10.0.0.0/8"]),
        )
        .expect("a trusted peer forwards the client");
        assert_eq!(address, "203.0.113.9".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn trusted_peer_without_a_header_falls_back_to_itself() {
        let peer = "10.0.0.5".parse().unwrap();
        let address = resolve(peer, &HeaderMap::new(), &trusted(&["10.0.0.0/8"]))
            .expect("a missing header falls back to the peer");
        assert_eq!(address, peer);
    }

    #[test]
    fn a_prepended_hop_is_ignored() {
        // The client wrote `1.2.3.4` before its real address. The proxy
        // appended `203.0.113.9`, which is the rightmost non-proxy hop.
        let address = resolve(
            "10.0.0.5".parse().unwrap(),
            &headers("1.2.3.4, 203.0.113.9"),
            &trusted(&["10.0.0.0/8"]),
        )
        .expect("the forged leftmost hop must not win");
        assert_eq!(address, "203.0.113.9".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn several_trusted_hops_walk_left_to_the_client() {
        let address = resolve(
            "10.0.0.5".parse().unwrap(),
            &headers("203.0.113.9, 10.0.0.6, 10.0.0.7"),
            &trusted(&["10.0.0.0/8"]),
        )
        .expect("the walk skips every trusted hop");
        assert_eq!(address, "203.0.113.9".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn a_chain_of_only_proxies_is_not_attributable() {
        let error = resolve(
            "10.0.0.5".parse().unwrap(),
            &headers("10.0.0.6, 10.0.0.7"),
            &trusted(&["10.0.0.0/8"]),
        )
        .unwrap_err();
        assert_eq!(error, ForwardedError::NotAttributable);
    }

    #[test]
    fn a_malformed_chain_is_rejected() {
        let error = resolve(
            "10.0.0.5".parse().unwrap(),
            &headers("203.0.113.9, not-an-address"),
            &trusted(&["10.0.0.0/8"]),
        )
        .unwrap_err();
        assert_eq!(error, ForwardedError::Malformed);

        let error = resolve(
            "10.0.0.5".parse().unwrap(),
            &headers("203.0.113.9,"),
            &trusted(&["10.0.0.0/8"]),
        )
        .unwrap_err();
        assert_eq!(error, ForwardedError::Malformed);
    }

    #[test]
    fn an_over_long_chain_is_rejected() {
        let value = (0..=MAX_FORWARDED_HOPS)
            .map(|index| format!("10.0.0.{}", index % 250 + 1))
            .collect::<Vec<_>>()
            .join(", ");
        let error = resolve(
            "10.0.0.5".parse().unwrap(),
            &headers(&value),
            &trusted(&["10.0.0.0/8"]),
        )
        .unwrap_err();
        assert_eq!(error, ForwardedError::TooManyHops);
    }

    #[test]
    fn multiple_header_lines_are_read_in_order() {
        let mut headers = HeaderMap::new();
        headers.append("x-forwarded-for", HeaderValue::from_static("203.0.113.9"));
        headers.append("x-forwarded-for", HeaderValue::from_static("10.0.0.6"));
        let address = resolve(
            "10.0.0.5".parse().unwrap(),
            &headers,
            &trusted(&["10.0.0.0/8"]),
        )
        .expect("both header lines form one chain");
        assert_eq!(address, "203.0.113.9".parse::<IpAddr>().unwrap());
    }
}
