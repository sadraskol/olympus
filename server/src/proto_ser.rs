use olympus_server::proto;
use olympus_server::proto::hermes::{
    AckOrVal, HermesMessage_HermesType, PaxosMessage_PaxosType, PeerMessage, PeerMessage_Type,
};
use client_interface::client::{Proto, Key, Value};

use crate::hermes::HermesMessage;
use crate::paxos::{Content, PaxosMessage};
use crate::state::{Member, Timestamp};

impl Proto<proto::hermes::Timestamp> for Timestamp {
    fn from_proto(msg: &proto::hermes::Timestamp) -> Self {
        Timestamp::new(msg.get_version(), msg.get_cid())
    }

    fn to_proto(&self) -> proto::hermes::Timestamp {
        let mut ts = proto::hermes::Timestamp::new();
        ts.set_cid(self.c_id);
        ts.set_version(self.version);
        ts
    }
}

impl Proto<olympus_server::proto::hermes::HermesMessage> for HermesMessage {
    fn from_proto(msg: &olympus_server::proto::hermes::HermesMessage) -> Self {
        match msg.get_field_type() {
            HermesMessage_HermesType::Inv => {
                let inv = msg.get_inv();
                HermesMessage::Inv {
                    sender_id: msg.get_sender_id(),
                    epoch_id: inv.get_epoch(),
                    key: Key(inv.get_key().to_vec()),
                    value: Value(inv.get_value().to_vec()),
                    ts: Proto::from_proto(inv.get_ts()),
                }
            }
            HermesMessage_HermesType::Val => {
                let val = msg.get_ack_or_val();
                HermesMessage::Val {
                    sender_id: msg.get_sender_id(),
                    epoch_id: val.get_epoch(),
                    key: Key(val.get_key().to_vec()),
                    ts: Proto::from_proto(val.get_ts()),
                }
            }
            HermesMessage_HermesType::Ack => {
                let ack = msg.get_ack_or_val();
                HermesMessage::Ack {
                    sender_id: msg.get_sender_id(),
                    epoch_id: ack.get_epoch(),
                    key: Key(ack.get_key().to_vec()),
                    ts: Proto::from_proto(ack.get_ts()),
                }
            }
        }
    }

    fn to_proto(&self) -> olympus_server::proto::hermes::HermesMessage {
        let mut msg = olympus_server::proto::hermes::HermesMessage::new();
        match self {
            HermesMessage::Inv {
                sender_id,
                epoch_id,
                key,
                value,
                ts: timestamp,
            } => {
                msg.set_sender_id(*sender_id);
                msg.set_field_type(HermesMessage_HermesType::Inv);
                let mut inval = proto::hermes::Inv::new();
                inval.set_epoch(*epoch_id);
                inval.set_key(key.0.clone());
                inval.set_value(value.0.clone());
                inval.set_ts(timestamp.to_proto());
                msg.set_inv(inval);
            }
            HermesMessage::Ack {
                sender_id,
                epoch_id,
                key,
                ts: timestamp,
            } => {
                msg.set_sender_id(*sender_id);
                msg.set_field_type(HermesMessage_HermesType::Ack);
                let mut acking = AckOrVal::new();
                acking.set_epoch(*epoch_id);
                acking.set_key(key.0.clone());
                acking.set_ts(timestamp.to_proto());
                msg.set_ack_or_val(acking);
            }
            HermesMessage::Val {
                sender_id,
                epoch_id,
                key,
                ts: timestamp,
            } => {
                msg.set_sender_id(*sender_id);
                msg.set_field_type(HermesMessage_HermesType::Val);
                let mut valid = AckOrVal::new();
                valid.set_epoch(*epoch_id);
                valid.set_key(key.0.clone());
                valid.set_ts(timestamp.to_proto());
                msg.set_ack_or_val(valid);
            }
        }
        msg
    }
}

impl Proto<proto::hermes::PaxosMessage> for PaxosMessage {
    fn from_proto(msg: &proto::hermes::PaxosMessage) -> Self {
        let content = match msg.get_field_type() {
            PaxosMessage_PaxosType::P1a => Content::P1a,
            PaxosMessage_PaxosType::P1b => {
                Content::P1b(msg.get_value().iter().map(|m| Member(*m)).collect())
            }
            PaxosMessage_PaxosType::P2a => {
                Content::P2a(msg.get_value().iter().map(|m| Member(*m)).collect())
            }
            PaxosMessage_PaxosType::P2b => Content::P2b,
            PaxosMessage_PaxosType::Leasing => {
                Content::Leasing(msg.get_value().iter().map(|m| Member(*m)).collect())
            }
        };
        PaxosMessage::new(msg.get_epoch_id(), Member(msg.get_sender_id()), content)
    }

    fn to_proto(&self) -> proto::hermes::PaxosMessage {
        let mut msg = proto::hermes::PaxosMessage::new();
        msg.set_epoch_id(self.epoch_id);
        msg.set_sender_id(self.sender.0);
        match &self.content {
            Content::P1a => msg.set_field_type(PaxosMessage_PaxosType::P1a),
            Content::P1b(value) => {
                msg.set_field_type(PaxosMessage_PaxosType::P1b);
                msg.set_value(value.iter().map(|m| m.0).collect())
            }
            Content::P2a(value) => {
                msg.set_field_type(PaxosMessage_PaxosType::P2a);
                msg.set_value(value.iter().map(|m| m.0).collect())
            }
            Content::P2b => msg.set_field_type(PaxosMessage_PaxosType::P2b),
            Content::Leasing(value) => {
                msg.set_field_type(PaxosMessage_PaxosType::Leasing);
                msg.set_value(value.iter().map(|m| m.0).collect())
            }
        }
        msg
    }
}

pub trait ToPeerMessage {
    fn as_peer(&self) -> PeerMessage;
}

impl ToPeerMessage for HermesMessage {
    fn as_peer(&self) -> PeerMessage {
        let mut msg = PeerMessage::new();
        msg.set_field_type(PeerMessage_Type::Hermes);
        msg.set_hermes(self.to_proto());
        msg
    }
}

impl ToPeerMessage for PaxosMessage {
    fn as_peer(&self) -> PeerMessage {
        let mut msg = PeerMessage::new();
        msg.set_field_type(PeerMessage_Type::Paxos);
        msg.set_paxos(self.to_proto());
        msg
    }
}
