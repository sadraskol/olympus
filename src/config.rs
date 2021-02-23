use std::fs::read_to_string;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    host: String,
    port: u16,
    internal_port: u16,
    pub peers: Vec<Peer>,
    pub id: u16,
}

impl Config {
    pub fn client_addr(&self) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::from_str(&self.host).unwrap(),
            self.port,
        ))
    }

    pub fn hermes_addr(&self) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::from_str(&self.host).unwrap(),
            self.internal_port,
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Peer {
    host: String,
    port: u16,
    pub id: u16,
}

impl Peer {
    pub fn addr(&self) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::from_str(&self.host).unwrap(),
            self.port,
        ))
    }
}

pub fn cfg() -> std::io::Result<Config> {
    let args: Vec<_> = std::env::args().collect();
    let result = read_to_string(&args[1])?;
    Ok(toml::from_str(&result)?)
}
