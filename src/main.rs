use std::collections::HashMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Condvar, Mutex};

use log::{info, LevelFilter};
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Root};
use protobuf::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use olympus::config::{cfg, Config};
use olympus::proto::hermes::{PeerMessage, PeerMessage_Type};
use olympus::proto::queries::Commands;

use crate::hermes::{Answer, ClientId, HMessage, Hermes};
use crate::paxos::LeaseState;
use crate::proto_ser::{Proto, ToPeerMessage};
use crate::state::Member;
use std::time::Duration;

mod hermes;
mod paxos;
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
    answer_latches: Shared<HashMap<ClientId, AnswerLatch>>,
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

enum Action {
    Wait(Duration),
    Send(Vec<HMessage>),
}

async fn hermes_listener(shared_state: SharedState) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("listen to peers requests");

        let listener = TcpListener::bind(shared_state.cfg.hermes_addr())
            .await
            .unwrap();

        let membership_loop = shared_state.clone();
        tokio::spawn(async move {
            loop {
                let action = {
                    let mut guard = membership_loop.hermes.lock().unwrap();
                    match guard.lease_state() {
                        LeaseState::Expired => Action::Send(guard.start()),
                        LeaseState::Renewing(duration) => Action::Wait(duration),
                        LeaseState::PendingUntil(duration) => Action::Wait(duration),
                    }
                };

                match action {
                    Action::Wait(duration) => {
                        tokio::time::sleep(duration).await;
                    }
                    Action::Send(membership_messages) => {
                        for message in membership_messages {
                            if let HMessage::Paxos(member, msg) = message {
                                let socket = membership_loop.peers.addr_by(&member);
                                let self_id = membership_loop.cfg.id as u32;

                                tokio::spawn(async move {
                                    send_sync_message(&socket, self_id, msg).await.unwrap();
                                });
                            }
                        }
                    }
                }
            }
        });
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

        let message = PeerMessage::parse_from_bytes(&buf).unwrap();
        let hmessage = match message.get_field_type() {
            PeerMessage_Type::Paxos => {
                let peer_id = Member(message.get_paxos().get_sender_id());
                HMessage::Paxos(peer_id, Proto::from_proto(message.get_paxos()))
            }
            PeerMessage_Type::Hermes => {
                let peer_id = Member(message.get_hermes().get_sender_id());
                HMessage::Sync(peer_id, Proto::from_proto(message.get_hermes()))
            }
        };

        let mut guard = state.hermes.lock().unwrap();
        guard.run(hmessage)
    };
    for message in messages {
        match message {
            HMessage::Sync(member, msg) => {
                let socket = state.peers.addr_by(&member);
                let self_id = state.cfg.id as u32;
                tokio::spawn(async move {
                    send_sync_message(&socket, self_id, msg).await.unwrap();
                });
            }
            HMessage::Paxos(member, msg) => {
                let socket = state.peers.addr_by(&member);
                let self_id = state.cfg.id as u32;
                tokio::spawn(async move {
                    send_sync_message(&socket, self_id, msg).await.unwrap();
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
            HMessage::Sync(member, msg) => {
                let socket = state.peers.addr_by(&member);
                let self_id = state.cfg.id as u32;
                tokio::spawn(async move {
                    send_sync_message(&socket, self_id, msg).await.unwrap();
                })
                .await?;
            }
            HMessage::Paxos(member, msg) => {
                let socket = state.peers.addr_by(&member);
                let self_id = state.cfg.id as u32;
                tokio::spawn(async move {
                    send_sync_message(&socket, self_id, msg).await.unwrap();
                })
                .await?;
            }
        }
    }

    let self_id = state.cfg.id as u32;

    let res = if let Some(response) = maybe_response {
        response.to_proto(self_id).write_to_bytes()?
    } else {
        let (lock, cvar) = &*local_pair;
        let mut guard = lock.lock().unwrap();
        while (*guard).is_none() {
            guard = cvar.wait(guard).unwrap();
        }
        if let Some(answer) = &*guard {
            answer.to_proto(self_id).write_to_bytes()?
        } else {
            panic!("no answer available, lock poisoned???");
        }
    };

    stream.write_u64(res.len() as u64).await?;
    stream.write_all(&res).await?;

    Ok(())
}

async fn send_sync_message<T: ToPeerMessage + Debug>(
    peer_socket: &SocketAddr,
    node_id: u32,
    message: T,
) -> std::io::Result<()> {
    info!("sending sync message {:?}", message);

    let mut stream = TcpStream::connect(peer_socket).await?;
    let vec = message.as_peer(node_id).write_to_bytes()?;
    stream.write_u64(vec.len() as u64).await?;
    stream.write_all(&vec).await
}
