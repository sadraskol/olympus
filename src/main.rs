use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Condvar, Mutex};

use protobuf::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use olympus::proto::hermes::HermesMessage;
use olympus::proto::queries::{Answers, Commands};

use crate::hermes::{ClientId, HMessage, Hermes};
use crate::state::Member;

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
    fn new() -> Self {
        let peers_in = vec![
            (Member(1), SocketAddr::from_str("127.0.0.1:12301").unwrap()),
            (Member(2), SocketAddr::from_str("127.0.0.1:12302").unwrap()),
        ];

        Membership {
            list: Arc::new(Mutex::new(peers_in)),
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

    fn member_by(&self, key: &SocketAddr) -> Member {
        let guard = self.list.lock().unwrap();
        for (mem, addr) in &*guard {
            if key == addr {
                return mem.clone();
            }
        }
        panic!("No peer of number {:?}", key);
    }
}

#[derive(Clone)]
pub struct SharedState {
    clients: Shared<HashMap<ClientId, AnswerLatch>>,
    hermes: Shared<Hermes>,
    peers: Membership,
    client_gen: ClientGen,
}

impl SharedState {
    fn new() -> Self {
        SharedState {
            clients: Arc::new(Mutex::new(HashMap::new())),
            hermes: Arc::new(Mutex::new(Hermes::new(1))),
            peers: Membership::new(),
            client_gen: ClientGen::new(),
        }
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let state = SharedState::new();

    client_listener(state.clone()).await;
    hermes_listener(state).await;

    Ok(())
}

async fn hermes_listener(shared_state: SharedState) {
    tokio::spawn(async move {
        let listener = TcpListener::bind("127.0.0.1:12346").await.unwrap();

        loop {
            if let Ok((stream, _)) = listener.accept().await {
                let state = shared_state.clone();
                tokio::spawn(async move {
                    peer_socket_handler(state, stream).await.unwrap();
                });
            }
        }
    });
}

async fn peer_socket_handler(state: SharedState, mut stream: TcpStream) -> std::io::Result<()> {
    let peer_id = state.peers.member_by(&stream.peer_addr()?);

    let messages = {
        let size = stream.read_u64().await?;
        let mut buf = vec![0; size as usize];
        stream.read_exact(&mut buf).await?;

        let message = HermesMessage::parse_from_bytes(&buf).unwrap();
        let mut guard = state.hermes.lock().unwrap();
        guard.receive(HMessage::Sync(peer_id, message));
        guard.run()
    };
    for message in messages {
        match message {
            HMessage::Sync(member, msg) => {
                let socket = state.peers.addr_by(&member);
                tokio::spawn(async move {
                    send_sync_message(&socket, msg).await.unwrap();
                });
            }
            HMessage::Client(_, _) => {
                panic!("client message in return to a hermes run??");
            }
            HMessage::Answer(client, answer) => {
                let map = state.clients.lock().unwrap();
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

async fn client_listener(shared_state: SharedState) {
    tokio::spawn(async move {
        let listener = TcpListener::bind("127.0.0.1:12345").await.unwrap();

        loop {
            if let Ok((stream, _)) = listener.accept().await {
                let state = shared_state.clone();
                tokio::spawn(async move {
                    client_socket_handler(state, stream).await.unwrap();
                });
            }
            // else skip socket
        }
    });
}

async fn client_socket_handler(state: SharedState, mut stream: TcpStream) -> std::io::Result<()> {
    let client_id = state.client_gen.gen();
    let pair = Arc::new((Mutex::new(None), Condvar::new()));
    let local_pair = pair.clone();
    {
        let mut x = state.clients.lock().unwrap();
        x.insert(client_id.clone(), pair);
    }

    let messages = {
        let size = stream.read_u64().await?;
        let mut buf = vec![0; size as usize];
        stream.read_exact(&mut buf).await?;

        let command = Commands::parse_from_bytes(&buf).unwrap();
        let mut guard = state.hermes.lock().unwrap();
        guard.receive(HMessage::Client(client_id, command));
        guard.run()
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
                tokio::spawn(async move {
                    send_sync_message(&socket, msg).await.unwrap();
                });
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
    let mut stream = TcpStream::connect(peer_socket).await?;
    let vec = message.write_to_bytes()?;
    stream.write_u64(vec.len() as u64).await?;
    stream.write_all(&vec).await
}
