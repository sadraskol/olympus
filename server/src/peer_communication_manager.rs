use crate::hermes::HMessage;
use crate::paxos::LeaseState;
use crate::proto::hermes::{PeerMessage, PeerMessage_Type};
use crate::proto_ser::ToPeerMessage;
use crate::state::Member;
use crate::SharedState;
use client_interface::client::Proto;
use log::{debug, error, info};
use protobuf::{Message, ProtobufError};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinError, JoinHandle};

#[derive(Debug)]
pub enum PeerError {
    IoError(std::io::Error),
    JoinError(JoinError),
    ProtobufError(ProtobufError),
}

impl From<std::io::Error> for PeerError {
    fn from(err: std::io::Error) -> Self {
        PeerError::IoError(err)
    }
}

impl From<JoinError> for PeerError {
    fn from(err: JoinError) -> Self {
        PeerError::JoinError(err)
    }
}

impl From<ProtobufError> for PeerError {
    fn from(err: ProtobufError) -> Self {
        PeerError::ProtobufError(err)
    }
}

type Result<T> = std::result::Result<T, PeerError>;

enum Action {
    Wait(Duration),
    Send(Vec<HMessage>),
}

pub async fn hermes_listener(shared_state: SharedState) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("listen to peers requests");

        let listener = TcpListener::bind(shared_state.cfg.hermes_addr())
            .await
            .unwrap();

        let shared_state_for_membership_loop = shared_state.clone();
        tokio::spawn(async move {
            loop {
                let action = {
                    let mut guard = shared_state_for_membership_loop.hermes.lock().unwrap();
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
                                let shared_state_for_request =
                                    shared_state_for_membership_loop.clone();
                                tokio::spawn(async move {
                                    send_message(shared_state_for_request, member, msg.clone())
                                        .await;
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

        state.run(hmessage)
    };
    for message in messages {
        match message {
            HMessage::Sync(member, msg) => {
                send_message(state.clone(), member, msg.clone()).await;
            }
            HMessage::Paxos(member, msg) => {
                send_message(state.clone(), member, msg.clone()).await;
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
