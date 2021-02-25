use log::{info, LevelFilter};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Condvar, Mutex};

use protobuf::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use olympus::proto::hermes::HermesMessage;
use olympus::proto::queries::{Answers, Commands};
use olympus::config::{cfg, Config};

use crate::hermes::{ClientId, HMessage, Hermes};
use crate::state::Member;
use tokio::task::JoinHandle;
use log4rs::append::file::FileAppender;
use log4rs::config::{Root, Appender};

mod hermes;
mod state;

// Simplify Arc Mutex pattern
type Shared<T> = Arc<Mutex<T>>;

// Latch to send an answer to a client. Note that this latch is single-use only
type AnswerLatch = Arc<(Mutex<Option<Answers>>, Condvar)>;

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

    fn gen(&self) -> ClientId {
        ClientId(
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
            .map(|p| (Member(p.id as i32), p.addr()))
            .collect();

        Membership {
            list: Arc::new(Mutex::new(peers)),
        }
    }

    fn members(&self) -> HashSet<Member> {
        self.list.lock().unwrap()
            .iter()
            .map(|(m, _)| m.clone())
            .collect()
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
    answer_latches: Shared<HashMap<ClientId, AnswerLatch>>,
    hermes: Shared<Hermes>,
    peers: Membership,
    client_gen: ClientGen,
}

impl SharedState {
    fn new() -> Self {
        let config = cfg().unwrap();
        let membership = Membership::new(&config);
        let mut hermes = Hermes::new(config.id as u32);
        hermes.update_members(membership.members());
        SharedState {
            cfg: config.clone(),
            answer_latches: Arc::new(Mutex::new(HashMap::new())),
            hermes: Arc::new(Mutex::new(hermes)),
            peers: membership,
            client_gen: ClientGen::new(),
        }
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

async fn hermes_listener(shared_state: SharedState) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("listen to peers requests");

        let listener = TcpListener::bind(shared_state.cfg.hermes_addr())
            .await
            .unwrap();

        loop {
            if let Ok((stream, _)) = listener.accept().await {
                let state = shared_state.clone();
                tokio::spawn(async move {
                    peer_socket_handler(state, stream).await.unwrap();
                });
            }
        }
    })
}

async fn peer_socket_handler(state: SharedState, mut stream: TcpStream) -> std::io::Result<()> {
    let messages = {
        let size = stream.read_u64().await?;
        let mut buf = vec![0; size as usize];
        stream.read_exact(&mut buf).await?;

        let message = HermesMessage::parse_from_bytes(&buf).unwrap();
        let peer_id = Member(message.get_sender_id() as i32);

        let mut guard = state.hermes.lock().unwrap();
        guard.run(HMessage::Sync(peer_id, message))
    };
    for message in messages {
        match message {
            HMessage::Sync(member, mut msg) => {
                let socket = state.peers.addr_by(&member);
                msg.set_sender_id(state.cfg.id as u32);
                tokio::spawn(async move {
                    send_sync_message(&socket, msg).await.unwrap();
                });
            }
            HMessage::Client(_, _) => {
                panic!("client message in return to a hermes run??");
            }
            HMessage::Answer(client, answer) => {
                let map = state.answer_latches.lock().unwrap();
                let pair = map.get(&client).unwrap().clone();
                let (m, c) = &*pair;
                let mut inbox = m.lock().unwrap();
                *inbox = Some(answer);
                c.notify_all();
            }
        }
    }
    Ok(())
}

async fn client_listener(shared_state: SharedState) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("listen to client requests");

        let listener = TcpListener::bind(shared_state.cfg.client_addr())
            .await
            .unwrap();

        loop {
            if let Ok((stream, _)) = listener.accept().await {
                let state = shared_state.clone();
                tokio::spawn(async move {
                    client_socket_handler(state, stream).await.unwrap();
                });
            }
            // else skip socket
        }
    })
}

async fn client_socket_handler(state: SharedState, mut stream: TcpStream) -> std::io::Result<()> {
    let client_id = state.client_gen.gen();
    let pair = Arc::new((Mutex::new(None), Condvar::new()));
    let local_pair = pair.clone();
    {
        let mut x = state.answer_latches.lock().unwrap();
        x.insert(client_id.clone(), pair);
    }

    let messages = {
        let size = stream.read_u64().await?;
        let mut buf = vec![0; size as usize];
        stream.read_exact(&mut buf).await?;

        let command = Commands::parse_from_bytes(&buf).unwrap();
        let mut guard = state.hermes.lock().unwrap();
        guard.run(HMessage::Client(client_id, command))
    };

    let mut maybe_response = None;
    for message in messages {
        match message {
            HMessage::Client(_, _) => {
                panic!("client message in return to a hermes run??");
            }
            HMessage::Answer(_, response) => {
                maybe_response = Some(response);
            }
            HMessage::Sync(member, mut msg) => {
                let socket = state.peers.addr_by(&member);
                msg.set_sender_id(state.cfg.id as u32);
                tokio::spawn(async move {
                    send_sync_message(&socket, msg).await.unwrap();
                }).await?;
            }
        }
    }

    let res = if let Some(response) = maybe_response {
        response.write_to_bytes()?
    } else {
        let (lock, cvar) = &*local_pair;
        let mut guard = lock.lock().unwrap();
        while (*guard).is_none() {
            guard = cvar.wait(guard).unwrap();
        }
        if let Some(answer) = &*guard {
            answer.write_to_bytes()?
        } else {
            panic!("no answer available, lock poisoned???");
        }
    };

    stream.write_u64(res.len() as u64).await?;
    stream.write_all(&res).await?;

    Ok(())
}

async fn send_sync_message(
    peer_socket: &SocketAddr,
    message: HermesMessage,
) -> std::io::Result<()> {
    info!("sending sync message {:?}", message);

    let mut stream = TcpStream::connect(peer_socket).await?;
    let vec = message.write_to_bytes()?;
    stream.write_u64(vec.len() as u64).await?;
    stream.write_all(&vec).await
}
