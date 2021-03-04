use std::collections::HashMap;

use log::{debug, info};

use olympus::config::Config;
use olympus::proto::queries::{Commands, Commands_CommandType};

use crate::paxos::{LeaseState, PaxosMessage, PaxosState};
use crate::state::{
    InvalidResult, Key, MachineValue, Member, ReadResult, Timestamp, Value, WriteResult,
};

#[derive(Clone, Debug, PartialEq)]
pub enum HermesMessage {
    Inv {
        key: Key,
        value: Value,
        ts: Timestamp,
    },
    Ack {
        key: Key,
        ts: Timestamp,
    },
    Val {
        key: Key,
        ts: Timestamp,
    },
}

impl HermesMessage {
    pub fn inv(key: &Key, timestamp: &Timestamp, value: &Value) -> Self {
        HermesMessage::Inv {
            key: key.clone(),
            value: value.clone(),
            ts: timestamp.clone(),
        }
    }

    pub fn ack(key: &Key, timestamp: &Timestamp) -> Self {
        HermesMessage::Ack {
            key: key.clone(),
            ts: timestamp.clone(),
        }
    }

    pub fn val(key: &Key, timestamp: &Timestamp) -> Self {
        HermesMessage::Val {
            key: key.clone(),
            ts: timestamp.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Answer {
    Read(Option<Value>),
    Write(WriteResult),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientId(pub u32);

#[derive(Clone, Debug, PartialEq)]
pub enum HMessage {
    Client(ClientId, Commands),
    Answer(ClientId, Answer),
    Sync(Member, HermesMessage),
    Paxos(Member, PaxosMessage),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Hermes {
    keys: HashMap<Key, MachineValue>,
    ts: Timestamp,
    membership: PaxosState,
    inbox: Vec<HMessage>,
    pending_reads: HashMap<Key, Vec<HMessage>>,
}

impl Hermes {
    pub fn new(config: &Config) -> Self {
        Hermes {
            keys: HashMap::new(),
            ts: Timestamp::new(0, config.id as u32),
            membership: PaxosState::new(config),
            inbox: vec![],
            pending_reads: HashMap::new(),
        }
    }

    pub fn start(&mut self) -> Vec<HMessage> {
        self.membership.next_epoch()
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
                                output.push(HMessage::Answer(client_id, Answer::Read(Some(value))));
                            }
                        }
                    } else {
                        output.push(HMessage::Answer(client_id, Answer::Read(None)));
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
                            for member in &self.membership.members() {
                                output.push(HMessage::Sync(
                                    *member,
                                    HermesMessage::inv(&key, &self.ts, &value),
                                ))
                            }
                        } else {
                            output.push(HMessage::Answer(
                                client_id,
                                Answer::Write(WriteResult::Rejected),
                            ))
                        }
                    } else {
                        self.keys.insert(
                            key.clone(),
                            MachineValue::write_value(client_id, value.clone(), self.ts.clone()),
                        );
                        for member in &self.membership.members() {
                            output.push(HMessage::Sync(
                                *member,
                                HermesMessage::inv(&key, &self.ts, &value),
                            ))
                        }
                    }
                }
            },
            HMessage::Sync(member, msg) => match msg {
                HermesMessage::Inv { key, value, ts } => {
                    info!(
                        "invalidate key: {:?}, value: {:?}, ts: {:?}",
                        key, value, ts
                    );
                    self.ts.increment_to(&ts);

                    if let Some(machine) = self.keys.get_mut(&key) {
                        if let InvalidResult::WriteCancelled(client) =
                            machine.invalid(ts.clone(), value)
                        {
                            output.push(HMessage::Answer(
                                client,
                                Answer::Write(WriteResult::Rejected),
                            ));
                        }
                    } else {
                        self.keys
                            .insert(key.clone(), MachineValue::invalid_value(ts.clone(), value));
                    }
                    output.push(HMessage::Sync(member, HermesMessage::ack(&key, &ts)));
                }
                HermesMessage::Ack { key, .. } => {
                    info!("ack key: {:?}", key);

                    if let Some(machine) = self.keys.get_mut(&key) {
                        machine.ack(member);
                        if let Some(client_id) =
                            machine.ack_write_against(&self.membership.members())
                        {
                            output.push(HMessage::Answer(
                                client_id,
                                Answer::Write(WriteResult::Accepted),
                            ));
                            for member in &self.membership.members() {
                                output.push(HMessage::Sync(
                                    *member,
                                    HermesMessage::val(&key, &machine.timestamp),
                                ));
                            }
                        }
                    }
                }
                HermesMessage::Val { key, ts } => {
                    info!("validate key: {:?}, ts: {:?}", key, ts);

                    if let Some(machine) = self.keys.get_mut(&key) {
                        if let ReadResult::Value(value) = machine.validate(ts) {
                            for client_waiting in self.pending_reads.get(&key).unwrap_or(&vec![]) {
                                if let HMessage::Client(client_id, _) = client_waiting {
                                    output.push(HMessage::Answer(
                                        client_id.clone(),
                                        Answer::Read(Some(value.clone())),
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
            HMessage::Paxos(_m, msg) => output = self.membership.run(msg),
        }
        output
    }

    pub fn lease_state(&self) -> LeaseState {
        self.membership.lease_state()
    }
}

#[cfg(test)]
mod hermes_test {
    use std::cmp::Ordering;
    use std::collections::HashSet;
    use std::iter::FromIterator;

    use olympus::config::Config;
    use olympus::proto::queries::{Commands, Commands_CommandType, Read, Write};

    use crate::hermes::{Answer, ClientId, HMessage, Hermes, HermesMessage};
    use crate::paxos::{Content, PaxosMessage};
    use crate::state::{Key, Member, Timestamp, Value, WriteResult};

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
                HMessage::Paxos(_, _) => Ordering::Greater,
            },
            HMessage::Sync(left_id, _) => match right {
                HMessage::Client(_, _) => Ordering::Less,
                HMessage::Sync(right_id, _) => left_id.cmp(&right_id),
                HMessage::Answer(_, _) => Ordering::Greater,
                HMessage::Paxos(_, _) => Ordering::Greater,
            },
            HMessage::Answer(left_id, _) => match right {
                HMessage::Client(_, _) => Ordering::Less,
                HMessage::Sync(_, _) => Ordering::Less,
                HMessage::Answer(right_id, _) => left_id.cmp(&right_id),
                HMessage::Paxos(_, _) => Ordering::Greater,
            },
            HMessage::Paxos(left_id, _) => match right {
                HMessage::Client(_, _) => Ordering::Less,
                HMessage::Sync(_, _) => Ordering::Less,
                HMessage::Answer(_, _) => Ordering::Less,
                HMessage::Paxos(right_id, _) => left_id.cmp(&right_id),
            },
        }
    }

    fn assert_msg_eq(mut first: Vec<HMessage>, mut second: Vec<HMessage>) {
        first.sort_by(msg_by_member);
        second.sort_by(msg_by_member);
        assert_eq!(first, second);
    }

    #[test]
    fn setup_a_working_paxos() {
        let mut hermes = Hermes::new(&Config::test_setup(2, vec![1, 3]));
        let expected_membership = HashSet::from_iter(vec![Member(1), Member(2), Member(3)]);
        assert_msg_eq(
            hermes.start(),
            vec![
                HMessage::Paxos(Member(1), PaxosMessage::new(1, Member(2), Content::P1a)),
                HMessage::Paxos(Member(3), PaxosMessage::new(1, Member(2), Content::P1a)),
            ],
        );

        assert_msg_eq(
            hermes.run(HMessage::Paxos(
                Member(2),
                PaxosMessage::new(1, Member(1), Content::P1b(expected_membership.clone())),
            )),
            vec![
                HMessage::Paxos(
                    Member(1),
                    PaxosMessage::new(1, Member(2), Content::P2a(expected_membership.clone())),
                ),
                HMessage::Paxos(
                    Member(3),
                    PaxosMessage::new(1, Member(2), Content::P2a(expected_membership.clone())),
                ),
            ],
        );

        assert_msg_eq(
            hermes.run(HMessage::Paxos(
                Member(2),
                PaxosMessage::new(1, Member(3), Content::P2b),
            )),
            vec![
                HMessage::Paxos(
                    Member(1),
                    PaxosMessage::new(1, Member(2), Content::Leasing(expected_membership.clone())),
                ),
                HMessage::Paxos(
                    Member(3),
                    PaxosMessage::new(1, Member(2), Content::Leasing(expected_membership.clone())),
                ),
            ],
        );
    }

    fn setup_default_running_hermes() -> Hermes {
        let mut hermes = Hermes::new(&Config::test_setup(2, vec![1, 3]));
        let expected_membership = HashSet::from_iter(vec![Member(1), Member(2), Member(3)]);
        hermes.start();
        hermes.run(HMessage::Paxos(
            Member(2),
            PaxosMessage::new(1, Member(1), Content::P1b(expected_membership.clone())),
        ));
        hermes.run(HMessage::Paxos(
            Member(2),
            PaxosMessage::new(1, Member(3), Content::P2b),
        ));
        hermes
    }

    #[test]
    fn first_scenario_write_goes_well_and_read_is_okay() {
        let mut hermes = setup_default_running_hermes();

        let key = Key(vec![35, 36, 37]);
        let value = Value(vec![1, 2, 3]);

        let expected_ts = Timestamp::new(1, 2);
        assert_msg_eq(
            hermes.run(HMessage::Client(ClientId(1), write_command(&key, &value))),
            vec![
                HMessage::Sync(Member(1), HermesMessage::inv(&key, &expected_ts, &value)),
                HMessage::Sync(Member(3), HermesMessage::inv(&key, &expected_ts, &value)),
            ],
        );
        hermes.run(HMessage::Sync(
            Member(1),
            HermesMessage::ack(&key, &expected_ts),
        ));
        assert_msg_eq(
            hermes.run(HMessage::Sync(
                Member(3),
                HermesMessage::ack(&key, &expected_ts),
            )),
            vec![
                HMessage::Answer(ClientId(1), Answer::Write(WriteResult::Accepted)),
                HMessage::Sync(Member(1), HermesMessage::val(&key, &expected_ts)),
                HMessage::Sync(Member(3), HermesMessage::val(&key, &expected_ts)),
            ],
        );
        assert_msg_eq(
            hermes.run(HMessage::Client(ClientId(4), read_command(&key))),
            vec![HMessage::Answer(ClientId(4), Answer::Read(Some(value)))],
        );
    }

    #[test]
    fn when_value_is_invalidated_writes_fail() {
        let mut hermes = setup_default_running_hermes();

        let key = Key(vec![35, 36, 37]);
        let value = Value(vec![1, 2, 3]);
        let timestamp = Timestamp::new(2, 2);

        assert_msg_eq(
            hermes.run(HMessage::Sync(
                Member(2),
                HermesMessage::inv(&key, &timestamp, &value),
            )),
            vec![HMessage::Sync(
                Member(2),
                HermesMessage::ack(&key, &timestamp),
            )],
        );
        assert_msg_eq(
            hermes.run(HMessage::Client(ClientId(1), write_command(&key, &value))),
            vec![HMessage::Answer(
                ClientId(1),
                Answer::Write(WriteResult::Rejected),
            )],
        );
    }

    #[test]
    fn when_value_is_validated_reads_are_triggered() {
        let mut hermes = setup_default_running_hermes();

        let key = Key(vec![35, 36, 37]);
        let value = Value(vec![1, 2, 3]);
        let timestamp = Timestamp::new(2, 2);
        assert_msg_eq(
            hermes.run(HMessage::Sync(
                Member(3),
                HermesMessage::inv(&key, &timestamp, &value),
            )),
            vec![HMessage::Sync(
                Member(3),
                HermesMessage::ack(&key, &timestamp),
            )],
        );
        assert_msg_eq(
            hermes.run(HMessage::Client(ClientId(1), read_command(&key))),
            vec![],
        );
        assert_msg_eq(
            hermes.run(HMessage::Sync(
                Member(3),
                HermesMessage::val(&key, &timestamp),
            )),
            vec![HMessage::Answer(ClientId(1), Answer::Read(Some(value)))],
        );
    }

    #[test]
    fn when_messages_are_received_timestamp_is_updated() {
        let mut hermes = setup_default_running_hermes();

        let key = Key(vec![35, 36, 37]);
        let value = Value(vec![1, 2, 3]);
        let other_value = Value(vec![40, 41, 42]);

        let timestamp = Timestamp::new(1, 3);

        hermes.run(HMessage::Sync(
            Member(3),
            HermesMessage::inv(&key, &timestamp, &other_value),
        ));
        hermes.run(HMessage::Sync(
            Member(3),
            HermesMessage::val(&key, &timestamp),
        ));

        let expected_ts = Timestamp::new(2, 2);
        assert_msg_eq(
            hermes.run(HMessage::Client(ClientId(1), write_command(&key, &value))),
            vec![
                HMessage::Sync(Member(1), HermesMessage::inv(&key, &expected_ts, &value)),
                HMessage::Sync(Member(3), HermesMessage::inv(&key, &expected_ts, &value)),
            ],
        );
    }

    #[test]
    fn when_write_are_invalidated_by_another_node_send_refused_write() {
        let mut hermes = setup_default_running_hermes();

        let key = Key(vec![35, 36, 37]);
        let value = Value(vec![1, 2, 3]);
        let other_value = Value(vec![40, 41, 42]);

        let timestamp = Timestamp::new(1, 3);

        hermes.run(HMessage::Client(ClientId(2), write_command(&key, &value)));
        assert_msg_eq(
            hermes.run(HMessage::Sync(
                Member(3),
                HermesMessage::inv(&key, &timestamp, &other_value),
            )),
            vec![
                HMessage::Answer(ClientId(2), Answer::Write(WriteResult::Rejected)),
                HMessage::Sync(Member(3), HermesMessage::ack(&key, &timestamp)),
            ],
        );
    }
}
