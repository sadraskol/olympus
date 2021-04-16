use crate::hermes::HMessage;
use crate::proto_ser::ToPeerMessage;
use crate::state::Member;
use crate::SharedState;
use client_interface::client::{read_from, write_to, Query};
use log::{debug, error, info};
use protobuf::{Message, ProtobufError};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::{Arc, Condvar, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinError, JoinHandle};

#[derive(Debug)]
pub enum ClientError {
    NoAnswerInLatch,
    IoError(std::io::Error),
    JoinError(JoinError),
    ProtobufError(ProtobufError),
}

impl From<std::io::Error> for ClientError {
    fn from(err: std::io::Error) -> Self {
        ClientError::IoError(err)
    }
}

impl From<JoinError> for ClientError {
    fn from(err: JoinError) -> Self {
        ClientError::JoinError(err)
    }
}

impl From<ProtobufError> for ClientError {
    fn from(err: ProtobufError) -> Self {
        ClientError::ProtobufError(err)
    }
}

type Result<T> = std::result::Result<T, ClientError>;

pub async fn client_listener(shared_state: SharedState) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("listen to client requests");

        let listener = TcpListener::bind(shared_state.cfg.client_addr())
            .await
            .unwrap();

        loop {
            if let Ok((stream, _)) = listener.accept().await {
                let state = shared_state.clone();
                tokio::spawn(async move {
                    match client_socket_handler(state, stream).await {
                        Ok(_) => debug!("client socket ok"),
                        Err(err) => error!("client socket error: {:?}", err),
                    }
                });
            }
            // else skip socket
        }
    })
}

async fn client_socket_handler(state: SharedState, mut stream: TcpStream) -> Result<()> {
    loop {
        let query: Query = match read_from(&mut stream).await {
            Ok(q) => q,
            Err(err) => {
                error!("error while reading {:?}", err);
                return Ok(());
            }
        };

        let pair = Arc::new((Mutex::new(None), Condvar::new()));
        let local_pair = pair.clone();
        let req_id = state.client_gen.gen();
        {
            let mut x = state.answer_latches.lock().unwrap();
            x.insert(req_id.clone(), pair);
        }

        let messages = state.run(HMessage::Client(req_id, query));

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
                    send_message(state.clone(), member, msg.clone()).await;
                }
                HMessage::Paxos(member, msg) => {
                    send_message(state.clone(), member, msg.clone()).await;
                }
            }
        }

        let res = if let Some(response) = maybe_response {
            response
        } else {
            let (lock, cvar) = &*local_pair;
            let mut guard = lock.lock().unwrap();
            while (*guard).is_none() {
                guard = cvar.wait(guard).unwrap();
            }
            if let Some(answer) = &*guard {
                answer.clone()
            } else {
                return Err(ClientError::NoAnswerInLatch);
            }
        };

        match write_to(&mut stream, &res).await {
            Ok(_) => {}
            Err(err) => {
                error!("error while writing {:?}", err);
                return Ok(());
            }
        }
        stream.flush().await?;
    }
}

async fn send_message<M: ToPeerMessage + Debug + Clone>(
    shared_state: SharedState,
    member: Member,
    msg: M,
) {
    let peer_addr = shared_state.peers.addr_by(&member);
    match send_sync_message(&peer_addr, msg.clone()).await {
        Ok(_) => debug!("send_sync_message successful {:?}", msg),
        Err(err) => {
            error!("failing member {:?} with reason: {:?}", member, err);
            let mut guard = shared_state.hermes.lock().unwrap();
            guard.failing_member(member);
        }
    }
}

async fn send_sync_message<T: ToPeerMessage + Debug>(
    peer_socket: &SocketAddr,
    message: T,
) -> Result<()> {
    info!("sending sync message {:?}", message);

    let mut stream = TcpStream::connect(peer_socket).await?;
    let vec = message.as_peer().write_to_bytes()?;
    stream.write_u64(vec.len() as u64).await?;
    stream.write_all(&vec).await?;
    stream.flush().await?;

    Ok(())
}
