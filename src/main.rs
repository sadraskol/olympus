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

    fn gen(&mut self) -> ClientId {
        ClientId(
            self.current
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        )
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut peers_in = HashMap::new();
    peers_in.insert(Member(1), SocketAddr::from_str("127.0.0.1:12301").unwrap());
    peers_in.insert(Member(2), SocketAddr::from_str("127.0.0.1:12302").unwrap());

    let client_gen = ClientGen::new();
    let clients = Arc::new(Mutex::new(HashMap::new()));
    let hermes_intern = Hermes::new(1);
    let hermes = Arc::new(Mutex::new(hermes_intern));
    let peers = Arc::new(peers_in);

    let listener = TcpListener::bind("127.0.0.1:12345").await?;

    loop {
        let (stream, _) = listener.accept().await?;

        let arc = peers.clone();
        let mut gen = client_gen.clone();
        let mut c = clients.clone();
        let mut h = hermes.clone();
        tokio::spawn(async move {
            socket_handler(&arc, &mut gen, &mut c, &mut h, stream)
                .await
                .unwrap();
        });
    }
    Ok(())
}

async fn socket_handler(
    peers: &Arc<HashMap<Member, SocketAddr>>,
    client_gen: &mut ClientGen,
    clients: &mut Shared<HashMap<ClientId, AnswerLatch>>,
    hermes: &mut Shared<Hermes>,
    mut stream: TcpStream,
) -> std::io::Result<()> {
    let client_id = client_gen.gen();
    let pair = Arc::new((Mutex::new(None), Condvar::new()));
    let local_pair = pair.clone();
    {
        let mut x = clients.lock().unwrap();
        x.insert(client_id.clone(), pair);
    }

    let messages = {
        let size = stream.read_u64().await?;
        let mut buf = vec![0; size as usize];
        stream.read_exact(&mut buf).await?;

        let command = Commands::parse_from_bytes(&buf).unwrap();
        let mut guard = hermes.lock().unwrap();
        guard.receive(HMessage::Client(client_id, command));
        guard.run()
    };

    let mut maybe_response = None;
    for message in messages {
        match message {
            HMessage::Client(_, _) => {}
            HMessage::Answer(_, response) => {
                maybe_response = Some(response);
            }
            HMessage::Sync(member, msg) => {
                let socket = *peers.get(&member).unwrap();
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
