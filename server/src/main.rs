use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Condvar, Mutex};

use log::LevelFilter;
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Root};

use crate::config::{cfg, Config};

use crate::client_communication_manager::client_listener;
use crate::hermes::{HMessage, Hermes, RequestId};
use crate::peer_communication_manager::hermes_listener;
use crate::state::Member;
use client_interface::client::Answer;

mod client_communication_manager;
mod config;
mod hermes;
mod paxos;
mod peer_communication_manager;
mod proto;
mod proto_ser;
mod state;

// Simplify Arc Mutex pattern
type Shared<T> = Arc<Mutex<T>>;

// Latch to send an answer to a client. Note that this latch is single-use only
type AnswerLatch = Arc<(Mutex<Option<Answer>>, Condvar)>;

#[derive(Clone)]
struct ClientGen {
    current: Arc<AtomicU32>,
}

impl ClientGen {
    fn new() -> Self {
        ClientGen {
            current: Arc::new(AtomicU32::new(0)),
        }
    }

    fn gen(&self) -> RequestId {
        RequestId(
            self.current
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        )
    }
}

#[derive(Clone)]
pub struct Membership {
    list: Shared<Vec<(Member, SocketAddr)>>,
}

impl Membership {
    fn new(cfg: &Config) -> Self {
        let peers = cfg
            .peers
            .iter()
            .map(|p| (Member(p.id as u32), p.addr()))
            .collect();

        Membership {
            list: Arc::new(Mutex::new(peers)),
        }
    }

    fn addr_by(&self, key: &Member) -> SocketAddr {
        let guard = self.list.lock().unwrap();
        for (mem, addr) in &*guard {
            if mem == key {
                return *addr;
            }
        }
        panic!("No peer of number {:?}", key);
    }
}

#[derive(Clone)]
pub struct SharedState {
    cfg: Config,
    answer_latches: Shared<HashMap<RequestId, AnswerLatch>>,
    hermes: Shared<Hermes>,
    peers: Membership,
    client_gen: ClientGen,
}

impl SharedState {
    fn new() -> Self {
        let config = cfg().unwrap();
        let membership = Membership::new(&config);
        let hermes = Hermes::new(&config);
        SharedState {
            cfg: config,
            answer_latches: Arc::new(Mutex::new(HashMap::new())),
            hermes: Arc::new(Mutex::new(hermes)),
            peers: membership,
            client_gen: ClientGen::new(),
        }
    }

    fn run(&self, message: HMessage) -> Vec<HMessage> {
        let mut guard = self.hermes.lock().unwrap();
        guard.run(message)
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let state = SharedState::new();

    let file = FileAppender::builder()
        .build(format!("replica-{}.log", state.cfg.id))
        .unwrap();

    let config = log4rs::Config::builder()
        .appender(Appender::builder().build("file", Box::new(file)))
        .build(Root::builder().appender("file").build(LevelFilter::Info))
        .unwrap();

    log4rs::init_config(config).unwrap();

    let client_handle = client_listener(state.clone()).await;
    let hermes_handle = hermes_listener(state).await;

    client_handle.await?;
    hermes_handle.await?;

    Ok(())
}
