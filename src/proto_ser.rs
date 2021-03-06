use protobuf::Message;

use olympus::proto;
use olympus::proto::hermes::{
    AckOrVal, HermesMessage_HermesType, PaxosMessage_PaxosType, PeerMessage, PeerMessage_Type,
};
use olympus::proto::queries::{
    Answers, Answers_AnswerType, ReadAnswer, WriteAnswer, WriteAnswer_WriteType,
};

use crate::hermes::{Answer, HermesMessage};
use crate::paxos::{Content, PaxosMessage};
use crate::state::{Key, Member, Timestamp, Value, WriteResult};

pub trait Proto<T: Message> {
    fn from_proto(msg: &T) -> Self;
    fn to_proto(&self, node_id: u32) -> T;
}

impl Proto<proto::hermes::Timestamp> for Timestamp {
    fn from_proto(msg: &proto::hermes::Timestamp) -> Self {
        Timestamp::new(msg.get_version(), msg.get_cid())
    }

    fn to_proto(&self, _node_id: u32) -> proto::hermes::Timestamp {
        let mut ts = proto::hermes::Timestamp::new();
        ts.set_cid(self.c_id);
        ts.set_version(self.version);
        ts
    }
}

impl Proto<olympus::proto::hermes::HermesMessage> for HermesMessage {
    fn from_proto(msg: &olympus::proto::hermes::HermesMessage) -> Self {
        match msg.get_field_type() {
            HermesMessage_HermesType::Inv => {
                let inv = msg.get_inv();
                HermesMessage::Inv {
                    epoch_id: inv.get_epoch(),
                    key: Key(inv.get_key().to_vec()),
                    value: Value(inv.get_value().to_vec()),
                    ts: Proto::from_proto(inv.get_ts()),
                }
            }
            HermesMessage_HermesType::Val => {
                let val = msg.get_ack_or_val();
                HermesMessage::Val {
                    epoch_id: val.get_epoch(),
                    key: Key(val.get_key().to_vec()),
                    ts: Proto::from_proto(val.get_ts()),
                }
            }
            HermesMessage_HermesType::Ack => {
                let ack = msg.get_ack_or_val();
                HermesMessage::Ack {
                    epoch_id: ack.get_epoch(),
                    key: Key(ack.get_key().to_vec()),
                    ts: Proto::from_proto(ack.get_ts()),
                }
            }
        }
    }

    fn to_proto(&self, node_id: u32) -> olympus::proto::hermes::HermesMessage {
        let mut msg = olympus::proto::hermes::HermesMessage::new();
        msg.set_sender_id(node_id);
        match self {
            HermesMessage::Inv {
                epoch_id,
                key,
                value,
                ts: timestamp,
            } => {
                msg.set_field_type(HermesMessage_HermesType::Inv);
                let mut inval = proto::hermes::Inv::new();
                inval.set_epoch(*epoch_id);
                inval.set_key(key.0.clone());
                inval.set_value(value.0.clone());
                inval.set_ts(timestamp.to_proto(node_id));
                msg.set_inv(inval);
            }
            HermesMessage::Ack {
                epoch_id,
                key,
                ts: timestamp,
            } => {
                msg.set_field_type(HermesMessage_HermesType::Ack);
                let mut acking = AckOrVal::new();
                acking.set_epoch(*epoch_id);
                acking.set_key(key.0.clone());
                acking.set_ts(timestamp.to_proto(node_id));
                msg.set_ack_or_val(acking);
            }
            HermesMessage::Val {
                epoch_id,
                key,
                ts: timestamp,
            } => {
                msg.set_field_type(HermesMessage_HermesType::Val);
                let mut valid = AckOrVal::new();
                valid.set_epoch(*epoch_id);
                valid.set_key(key.0.clone());
                valid.set_ts(timestamp.to_proto(node_id));
                msg.set_ack_or_val(valid);
            }
        }
        msg
    }
}

impl Proto<Answers> for Answer {
    fn from_proto(_msg: &Answers) -> Self {
        unimplemented!("Answers should only be read by the client")
    }

    fn to_proto(&self, _node_id: u32) -> Answers {
        let mut answer = Answers::new();
        match self {
            Answer::Read(r) => {
                answer.set_field_type(Answers_AnswerType::Read);
                let mut read = ReadAnswer::new();
                match r {
                    None => {
                        read.set_is_nil(true);
                    }
                    Some(v) => {
                        read.set_is_nil(false);
                        read.set_value(v.0.clone());
                    }
                }
                answer.set_read(read);
            }
            Answer::Write(w) => {
                answer.set_field_type(Answers_AnswerType::Write);
                let mut write = WriteAnswer::new();
                match w {
                    WriteResult::Rejected => {
                        write.set_code(WriteAnswer_WriteType::Refused);
                    }
                    WriteResult::Accepted => {
                        write.set_code(WriteAnswer_WriteType::Ok);
                    }
                }
                answer.set_write(write);
            }
        }
        answer
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

    fn to_proto(&self, node_id: u32) -> proto::hermes::PaxosMessage {
        let mut msg = proto::hermes::PaxosMessage::new();
        msg.set_epoch_id(self.epoch_id);
        msg.set_sender_id(node_id);
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
    fn as_peer(&self, node_id: u32) -> PeerMessage;
}

impl ToPeerMessage for HermesMessage {
    fn as_peer(&self, node_id: u32) -> PeerMessage {
        let mut msg = PeerMessage::new();
        msg.set_field_type(PeerMessage_Type::Hermes);
        msg.set_hermes(self.to_proto(node_id));
        msg
    }
}

impl ToPeerMessage for PaxosMessage {
    fn as_peer(&self, node_id: u32) -> PeerMessage {
        let mut msg = PeerMessage::new();
        msg.set_field_type(PeerMessage_Type::Paxos);
        msg.set_paxos(self.to_proto(node_id));
        msg
    }
}
