use crate::state::{Key, MachineValue, Member, ReadResult, Timestamp, Value, WriteResult};
use olympus::proto;
use olympus::proto::hermes::{AckOrVal, HermesMessage, HermesMessage_HermesType};
use olympus::proto::queries::{
    Answers, Answers_AnswerType, Commands, Commands_CommandType, Read, ReadAnswer, Write,
    WriteAnswer,
};
use std::collections::{HashMap, HashSet};

fn write_command(key: &Key, value: &Value) -> Commands {
    let mut write = Commands::new();
    write.set_field_type(Commands_CommandType::Write);
    let mut writing = Write::new();
    writing.set_key(key.0.clone());
    writing.set_value(value.0.clone());
    write.set_write(writing);
    write
}

fn read_command(key: &Key) -> Commands {
    let mut read = Commands::new();
    read.set_field_type(Commands_CommandType::Read);
    let mut reading = Read::new();
    reading.set_key(key.0.clone());
    read.set_read(reading);
    read
}

fn read_answer_of(value: &Value) -> Answers {
    let mut answer = Answers::new();
    answer.set_field_type(Answers_AnswerType::Read);
    let mut read = ReadAnswer::new();
    read.set_is_nil(false);
    read.set_value(value.0.clone());
    answer.set_read(read);
    answer
}

fn write_answer_ok() -> Answers {
    let mut answer = Answers::new();
    answer.set_field_type(Answers_AnswerType::Write);
    let write = WriteAnswer::new();
    answer.set_write(write);
    answer
}

fn ack_msg(key: &Key, timestamp: &Timestamp) -> HermesMessage {
    let mut ack = HermesMessage::new();
    ack.set_field_type(HermesMessage_HermesType::Ack);
    let mut acking = AckOrVal::new();
    acking.set_key(key.0.clone());
    let mut ts = proto::hermes::Timestamp::new();
    ts.set_cid(timestamp.c_id);
    ts.set_version(timestamp.version);
    acking.set_ts(ts);
    ack.set_ack_or_val(acking);
    ack
}

fn val_msg(key: &Key, timestamp: &Timestamp) -> HermesMessage {
    let mut val = HermesMessage::new();
    val.set_field_type(HermesMessage_HermesType::Val);
    let mut valid = AckOrVal::new();
    valid.set_key(key.0.clone());
    let mut ts = proto::hermes::Timestamp::new();
    ts.set_cid(timestamp.c_id);
    ts.set_version(timestamp.version);
    valid.set_ts(ts);
    val.set_ack_or_val(valid);
    val
}

fn inv_msg(key: &Key, timestamp: &Timestamp, value: &Value) -> HermesMessage {
    let mut inv = HermesMessage::new();
    inv.set_field_type(HermesMessage_HermesType::Inv);
    let mut inval = proto::hermes::Inv::new();
    inval.set_key(key.0.clone());
    inval.set_value(value.0.clone());
    let mut ts = proto::hermes::Timestamp::new();
    ts.set_cid(timestamp.c_id);
    ts.set_version(timestamp.version);
    inval.set_ts(ts);
    inv.set_inv(inval);
    inv
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientId(pub u32);

#[derive(Clone, Debug, PartialEq)]
pub enum HMessage {
    Client(ClientId, Commands),
    Answer(ClientId, Answers),
    Sync(Member, HermesMessage),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Hermes {
    keys: HashMap<Key, MachineValue>,
    ts: Timestamp,
    members: HashSet<Member>,
    inbox: Vec<HMessage>,
    pending_reads: Vec<HMessage>,
}

impl Hermes {
    pub fn new(c_id: u32) -> Self {
        Hermes {
            keys: HashMap::new(),
            ts: Timestamp::new(0, c_id),
            members: HashSet::new(),
            inbox: vec![],
            pending_reads: vec![],
        }
    }

    pub fn run(&mut self) -> Vec<HMessage> {
        let mut output = vec![];
        while let Some(ref msg) = self.inbox.pop() {
            match msg {
                HMessage::Client(client_id, command) => {
                    match command.get_field_type() {
                        Commands_CommandType::Read => {
                            if let Some(value) =
                                self.keys.get(&Key(command.get_read().get_key().to_vec()))
                            {
                                match value.read() {
                                    ReadResult::Pending => {
                                        self.pending_reads.push(msg.clone());
                                    }
                                    ReadResult::Value(value) => {
                                        output.push(HMessage::Answer(
                                            client_id.clone(),
                                            read_answer_of(&value),
                                        ));
                                    }
                                }
                            } else {
                                // TODO answer nil
                            }
                        }
                        Commands_CommandType::Write => {
                            self.ts.increment();
                            let key = Key(command.get_write().get_key().to_vec());
                            let value = Value(command.get_write().get_value().to_vec());
                            if let Some(machine) = self.keys.get_mut(&key) {
                                let state = machine.write(client_id.clone(), value.clone());
                                if state == WriteResult::Accepted {
                                    for member in &self.members {
                                        output.push(HMessage::Sync(
                                            member.clone(),
                                            inv_msg(&key, &self.ts, &value),
                                        ))
                                    }
                                }
                            } else {
                                self.keys.insert(
                                    key.clone(),
                                    MachineValue::write_default(
                                        client_id.clone(),
                                        value.clone(),
                                        self.ts.clone(),
                                    ),
                                );
                                for member in &self.members {
                                    output.push(HMessage::Sync(
                                        member.clone(),
                                        inv_msg(&key, &self.ts, &value),
                                    ))
                                }
                            }
                        }
                    }
                }
                HMessage::Sync(member, msg) => match msg.get_field_type() {
                    HermesMessage_HermesType::Inv => {}
                    HermesMessage_HermesType::Ack => {
                        let key = Key(msg.get_ack_or_val().get_key().to_vec());
                        if let Some(machine) = self.keys.get_mut(&key) {
                            machine.ack(member.clone());
                            if let Some(client_id) = machine.ack_write_against(&self.members) {
                                output.push(HMessage::Answer(client_id, write_answer_ok()));
                                for member in &self.members {
                                    output.push(HMessage::Sync(
                                        member.clone(),
                                        val_msg(&key, &machine.timestamp),
                                    ));
                                }
                            }
                        }
                    }
                    HermesMessage_HermesType::Val => {}
                },
                HMessage::Answer(c, d) => {
                    panic!("answer {:?} for {:?} in inbox!", d, c);
                }
            }
        }
        output
    }

    pub fn receive(&mut self, message: HMessage) {
        self.inbox.push(message);
    }

    pub fn update_members(&mut self, members: HashSet<Member>) {
        self.members = members;
    }
}

#[cfg(test)]
mod hermes_test {
    use std::cmp::Ordering;
    use std::collections::HashSet;
    use std::iter::FromIterator;

    use crate::hermes::{
        ack_msg, inv_msg, read_answer_of, read_command, val_msg, write_answer_ok, write_command,
        ClientId, HMessage, Hermes,
    };
    use crate::state::{Key, Member, Timestamp, Value};

    fn msg_by_member(left: &HMessage, right: &HMessage) -> Ordering {
        match left {
            HMessage::Client(left_id, _) => match right {
                HMessage::Client(right_id, _) => left_id.cmp(&right_id),
                HMessage::Sync(_, _) => Ordering::Greater,
                HMessage::Answer(_, _) => Ordering::Greater,
            },
            HMessage::Sync(left_id, _) => match right {
                HMessage::Client(_, _) => Ordering::Less,
                HMessage::Sync(right_id, _) => left_id.cmp(&right_id),
                HMessage::Answer(_, _) => Ordering::Greater,
            },
            HMessage::Answer(left_id, _) => match right {
                HMessage::Client(_, _) => Ordering::Less,
                HMessage::Sync(_, _) => Ordering::Less,
                HMessage::Answer(right_id, _) => left_id.cmp(&right_id),
            },
        }
    }

    #[test]
    fn first_scenario_write_goes_well_and_read_is_okay() {
        let mut hermes = Hermes::new(1);
        hermes.update_members(HashSet::from_iter(vec![Member(2), Member(3)]));

        let key = Key(vec![35, 36, 37]);
        let value = Value(vec![1, 2, 3]);

        hermes.receive(HMessage::Client(ClientId(1), write_command(&key, &value)));
        let expected_ts = Timestamp::new(1, 1);
        let mut vec2 = hermes.run();
        vec2.sort_by(msg_by_member);
        assert_eq!(
            vec2,
            vec![
                HMessage::Sync(Member(2), inv_msg(&key, &expected_ts, &value)),
                HMessage::Sync(Member(3), inv_msg(&key, &expected_ts, &value))
            ]
        );
        hermes.receive(HMessage::Sync(Member(2), ack_msg(&key, &expected_ts)));
        hermes.receive(HMessage::Sync(Member(3), ack_msg(&key, &expected_ts)));
        let mut vec1 = hermes.run();
        vec1.sort_by(msg_by_member);
        assert_eq!(
            vec1,
            vec![
                HMessage::Answer(ClientId(1), write_answer_ok()),
                HMessage::Sync(Member(2), val_msg(&key, &expected_ts)),
                HMessage::Sync(Member(3), val_msg(&key, &expected_ts))
            ]
        );
        hermes.receive(HMessage::Client(ClientId(4), read_command(&key)));
        assert_eq!(
            hermes.run(),
            vec![HMessage::Answer(ClientId(4), read_answer_of(&value))]
        );
    }
}
