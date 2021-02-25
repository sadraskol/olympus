use std::collections::{HashMap, HashSet};

use log::{debug, info};

use olympus::proto;
use olympus::proto::hermes::{AckOrVal, HermesMessage, HermesMessage_HermesType};
use olympus::proto::queries::{
    Answers, Answers_AnswerType, Commands, Commands_CommandType, ReadAnswer, WriteAnswer,
    WriteAnswer_WriteType,
};

use crate::state::{Key, MachineValue, Member, ReadResult, Timestamp, Value, WriteResult};

fn nil_read_answer() -> Answers {
    let mut answer = Answers::new();
    answer.set_field_type(Answers_AnswerType::Read);
    let mut read = ReadAnswer::new();
    read.set_is_nil(true);
    answer.set_read(read);
    answer
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
    let mut write = WriteAnswer::new();
    write.set_code(WriteAnswer_WriteType::Ok);
    answer.set_write(write);
    answer
}

fn write_answer_refused() -> Answers {
    let mut answer = Answers::new();
    answer.set_field_type(Answers_AnswerType::Write);
    let mut write = WriteAnswer::new();
    write.set_code(WriteAnswer_WriteType::Refused);
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
    pending_reads: HashMap<Key, Vec<HMessage>>,
}

impl Hermes {
    pub fn new(c_id: u32) -> Self {
        Hermes {
            keys: HashMap::new(),
            ts: Timestamp::new(0, c_id),
            members: HashSet::new(),
            inbox: vec![],
            pending_reads: HashMap::new(),
        }
    }

    pub fn run(&mut self, message: HMessage) -> Vec<HMessage> {
        debug!("{:?}", self);
        let mut output = vec![];
        match message {
            HMessage::Client(client_id, command) => match command.get_field_type() {
                Commands_CommandType::Read => {
                    let key = Key(command.get_read().get_key().to_vec());

                    info!("reading key: {:?}", key);

                    if let Some(value) = self.keys.get(&key) {
                        match value.read() {
                            ReadResult::Pending => {
                                if let Some(vec) = self.pending_reads.get_mut(&key) {
                                    vec.push(HMessage::Client(client_id, command));
                                } else {
                                    self.pending_reads
                                        .insert(key, vec![HMessage::Client(client_id, command)]);
                                }
                            }
                            ReadResult::Value(value) => {
                                output.push(HMessage::Answer(client_id, read_answer_of(&value)));
                            }
                        }
                    } else {
                        output.push(HMessage::Answer(client_id, nil_read_answer()));
                    }
                }
                Commands_CommandType::Write => {
                    self.ts.increment();
                    let key = Key(command.get_write().get_key().to_vec());
                    let value = Value(command.get_write().get_value().to_vec());

                    info!(
                        "writing key: {:?}, value: {:?}, ts: {:?}",
                        key, value, self.ts
                    );

                    if let Some(machine) = self.keys.get_mut(&key) {
                        let state =
                            machine.write(client_id.clone(), value.clone(), self.ts.clone());
                        if state == WriteResult::Accepted {
                            for member in &self.members {
                                output.push(HMessage::Sync(
                                    member.clone(),
                                    inv_msg(&key, &self.ts, &value),
                                ))
                            }
                        } else {
                            output.push(HMessage::Answer(client_id, write_answer_refused()))
                        }
                    } else {
                        self.keys.insert(
                            key.clone(),
                            MachineValue::write_value(client_id, value.clone(), self.ts.clone()),
                        );
                        for member in &self.members {
                            output.push(HMessage::Sync(
                                member.clone(),
                                inv_msg(&key, &self.ts, &value),
                            ))
                        }
                    }
                }
            },
            HMessage::Sync(member, msg) => match msg.get_field_type() {
                HermesMessage_HermesType::Inv => {
                    let key = Key(msg.get_inv().get_key().to_vec());
                    let value = Value(msg.get_inv().get_value().to_vec());
                    let ts = Timestamp::new(
                        msg.get_inv().get_ts().get_version(),
                        msg.get_inv().get_ts().get_cid(),
                    );

                    self.ts.increment_to(&ts);

                    info!(
                        "invalidate key: {:?}, value: {:?}, ts: {:?}",
                        key, value, ts
                    );

                    if let Some(machine) = self.keys.get_mut(&key) {
                        machine.invalid(ts.clone(), value);
                    } else {
                        self.keys
                            .insert(key.clone(), MachineValue::invalid_value(ts.clone(), value));
                    }
                    output.push(HMessage::Sync(member, ack_msg(&key, &ts)));
                }
                HermesMessage_HermesType::Ack => {
                    let key = Key(msg.get_ack_or_val().get_key().to_vec());

                    info!("ack key: {:?}", key);

                    if let Some(machine) = self.keys.get_mut(&key) {
                        machine.ack(member);
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
                HermesMessage_HermesType::Val => {
                    let key = Key(msg.get_ack_or_val().get_key().to_vec());
                    let ts = Timestamp::new(
                        msg.get_ack_or_val().get_ts().get_version(),
                        msg.get_ack_or_val().get_ts().get_cid(),
                    );

                    info!("validate key: {:?}, ts: {:?}", key, ts);

                    if let Some(machine) = self.keys.get_mut(&key) {
                        if let ReadResult::Value(value) = machine.validate(ts) {
                            for client_waiting in self.pending_reads.get(&key).unwrap_or(&vec![]) {
                                if let HMessage::Client(client_id, _) = client_waiting {
                                    output.push(HMessage::Answer(
                                        client_id.clone(),
                                        read_answer_of(&value),
                                    ));
                                }
                            }
                        }
                    }
                }
            },
            HMessage::Answer(c, d) => {
                panic!("answer {:?} for {:?} in inbox!", d, c);
            }
        }
        output
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

    use olympus::proto::queries::{Commands, Commands_CommandType, Read, Write};

    use crate::hermes::{
        ack_msg, inv_msg, read_answer_of, val_msg, write_answer_ok, write_answer_refused, ClientId,
        HMessage, Hermes,
    };
    use crate::state::{Key, Member, Timestamp, Value};

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

        let expected_ts = Timestamp::new(1, 1);
        let mut vec2 = hermes.run(HMessage::Client(ClientId(1), write_command(&key, &value)));
        vec2.sort_by(msg_by_member);
        assert_eq!(
            vec2,
            vec![
                HMessage::Sync(Member(2), inv_msg(&key, &expected_ts, &value)),
                HMessage::Sync(Member(3), inv_msg(&key, &expected_ts, &value))
            ]
        );
        hermes.run(HMessage::Sync(Member(2), ack_msg(&key, &expected_ts)));
        let mut vec1 = hermes.run(HMessage::Sync(Member(3), ack_msg(&key, &expected_ts)));
        vec1.sort_by(msg_by_member);
        assert_eq!(
            vec1,
            vec![
                HMessage::Answer(ClientId(1), write_answer_ok()),
                HMessage::Sync(Member(2), val_msg(&key, &expected_ts)),
                HMessage::Sync(Member(3), val_msg(&key, &expected_ts))
            ]
        );
        assert_eq!(
            hermes.run(HMessage::Client(ClientId(4), read_command(&key))),
            vec![HMessage::Answer(ClientId(4), read_answer_of(&value))]
        );
    }

    #[test]
    fn when_value_is_invalidated_writes_fail() {
        let mut hermes = Hermes::new(1);
        hermes.update_members(HashSet::from_iter(vec![Member(2), Member(3)]));

        let key = Key(vec![35, 36, 37]);
        let value = Value(vec![1, 2, 3]);
        let timestamp = Timestamp::new(2, 2);

        assert_eq!(
            hermes.run(HMessage::Sync(Member(2), inv_msg(&key, &timestamp, &value))),
            vec![HMessage::Sync(Member(2), ack_msg(&key, &timestamp))]
        );
        assert_eq!(
            hermes.run(HMessage::Client(ClientId(1), write_command(&key, &value))),
            vec![HMessage::Answer(ClientId(1), write_answer_refused())]
        );
    }

    #[test]
    fn when_value_is_validated_reads_are_triggered() {
        let mut hermes = Hermes::new(1);
        hermes.update_members(HashSet::from_iter(vec![Member(2), Member(3)]));

        let key = Key(vec![35, 36, 37]);
        let value = Value(vec![1, 2, 3]);
        let timestamp = Timestamp::new(2, 2);
        assert_eq!(
            hermes.run(HMessage::Sync(Member(3), inv_msg(&key, &timestamp, &value))),
            vec![HMessage::Sync(Member(3), ack_msg(&key, &timestamp))]
        );
        assert_eq!(
            hermes.run(HMessage::Client(ClientId(1), read_command(&key))),
            vec![]
        );
        assert_eq!(
            hermes.run(HMessage::Sync(Member(3), val_msg(&key, &timestamp))),
            vec![HMessage::Answer(ClientId(1), read_answer_of(&value))]
        );
    }

    #[test]
    fn when_messages_are_received_timestamp_is_updated() {
        let mut hermes = Hermes::new(1);
        hermes.update_members(HashSet::from_iter(vec![Member(2)]));

        let key = Key(vec![35, 36, 37]);
        let value = Value(vec![1, 2, 3]);
        let other_value = Value(vec![40, 41, 42]);

        let timestamp = Timestamp::new(1, 2);

        hermes.run(HMessage::Sync(
            Member(2),
            inv_msg(&key, &timestamp, &other_value),
        ));
        hermes.run(HMessage::Sync(Member(2), val_msg(&key, &timestamp)));

        let expected_ts = Timestamp::new(2, 1);
        assert_eq!(
            hermes.run(HMessage::Client(ClientId(1), write_command(&key, &value))),
            vec![HMessage::Sync(
                Member(2),
                inv_msg(&key, &expected_ts, &value)
            )]
        );
    }
}
