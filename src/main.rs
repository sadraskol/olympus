use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Condvar, Mutex};

use protobuf::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::hermes::{ClientId, HMessage, Hermes};
use olympus::proto::queries::{Answers, Commands};

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
    let client_id = ClientGen::new();
    let clients = Arc::new(Mutex::new(HashMap::new()));
    let hermes = Arc::new(Mutex::new(Hermes::new(1)));

    let listener = TcpListener::bind("127.0.0.1:12345").await?;

    loop {
        let (stream, _) = listener.accept().await?;

        let mut client_gen = client_id.clone();
        let mut client_map = clients.clone();
        let mut local_hermes = hermes.clone();
        tokio::spawn(async move {
            socket_handler(&mut client_gen, &mut client_map, &mut local_hermes, stream)
                .await
                .unwrap();
        });
    }
    Ok(())
}

async fn socket_handler(
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

    {
        let size = stream.read_u64().await?;
        let mut buf = vec![0; size as usize];
        stream.read_exact(&mut buf).await?;

        let command = Commands::parse_from_bytes(&buf).unwrap();
        let mut guard = hermes.lock().unwrap();
        guard.receive(HMessage::Client(client_id, command));
    }

    let vec = {
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

    stream.write_u64(vec.len() as u64).await?;
    stream.write_all(&vec).await?;

    Ok(())
}
