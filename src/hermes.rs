use std::collections::{HashMap, HashSet};

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
        epoch_id: u64,
        key: Key,
        value: Value,
        ts: Timestamp,
    },
    Ack {
        epoch_id: u64,
        key: Key,
        ts: Timestamp,
    },
    Val {
        epoch_id: u64,
        key: Key,
        ts: Timestamp,
    },
}

impl HermesMessage {
    pub fn inv(epoch_id: u64, key: &Key, timestamp: &Timestamp, value: &Value) -> Self {
        HermesMessage::Inv {
            epoch_id,
            key: key.clone(),
            value: value.clone(),
            ts: *timestamp,
        }
    }

    pub fn ack(epoch_id: u64, key: &Key, timestamp: &Timestamp) -> Self {
        HermesMessage::Ack {
            epoch_id,
            key: key.clone(),
            ts: *timestamp,
        }
    }

    pub fn val(epoch_id: u64, key: &Key, timestamp: &Timestamp) -> Self {
        HermesMessage::Val {
            epoch_id,
            key: key.clone(),
            ts: *timestamp,
        }
    }

    pub fn epoch_id(&self) -> u64 {
        match self {
            HermesMessage::Inv { epoch_id, .. } => *epoch_id,
            HermesMessage::Ack { epoch_id, .. } => *epoch_id,
            HermesMessage::Val { epoch_id, .. } => *epoch_id,
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
    // optimisation not to traverse every values in the k/v hashmap
    keys_expecting_quorum: HashSet<Key>,
    ts: Timestamp,
    membership: PaxosState,
    inbox: Vec<HMessage>,
    pending_reads: HashMap<Key, Vec<HMessage>>,
}

impl Hermes {
    pub fn new(config: &Config) -> Self {
        Hermes {
            keys: HashMap::new(),
            keys_expecting_quorum: HashSet::new(),
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
            HMessage::Client(client_id, command) => {
                output = self.client_command(client_id, command)
            }
            HMessage::Sync(member, msg) => {
                if msg.epoch_id() == self.membership.current_epoch() {
                    match msg {
                        HermesMessage::Inv {
                            epoch_id,
                            key,
                            value,
                            ts,
                        } => {
                            info!(
                                "invalidate epoch_id: {}, key: {:?}, value: {:?}, ts: {:?}",
                                epoch_id, key, value, ts
                            );
                            self.ts.increment_to(&ts);

                            if let Some(machine) = self.keys.get_mut(&key) {
                                if let InvalidResult::WriteCancelled(client) =
                                    machine.invalid(ts, value)
                                {
                                    output.push(HMessage::Answer(
                                        client,
                                        Answer::Write(WriteResult::Rejected),
                                    ));
                                }
                            } else {
                                self.keys
                                    .insert(key.clone(), MachineValue::invalid_value(ts, value));
                            }
                            output.push(HMessage::Sync(
                                member,
                                HermesMessage::ack(self.membership.current_epoch(), &key, &ts),
                            ));
                        }
                        HermesMessage::Ack { key, .. } => {
                            info!("ack key: {:?}", key);

                            if let Some(machine) = self.keys.get_mut(&key) {
                                machine.ack(member);
                                if let Some(client_id) =
                                    machine.ack_write_against(&self.membership.members())
                                {
                                    self.keys_expecting_quorum.remove(&key);
                                    output.push(HMessage::Answer(
                                        client_id,
                                        Answer::Write(WriteResult::Accepted),
                                    ));
                                    for member in &self.membership.members() {
                                        output.push(HMessage::Sync(
                                            *member,
                                            HermesMessage::val(
                                                self.membership.current_epoch(),
                                                &key,
                                                &machine.timestamp,
                                            ),
                                        ));
                                    }
                                }
                            }
                        }
                        HermesMessage::Val { epoch_id, key, ts } => {
                            info!(
                                "validate epoch_id: {}, key: {:?}, ts: {:?}",
                                epoch_id, key, ts
                            );

                            if let Some(machine) = self.keys.get_mut(&key) {
                                if let ReadResult::Value(value) = machine.validate(ts) {
                                    for client_waiting in
                                        self.pending_reads.get(&key).unwrap_or(&vec![])
                                    {
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
                    }
                }
            }
            HMessage::Answer(c, d) => {
                panic!("answer {:?} for {:?} in inbox!", d, c);
            }
            HMessage::Paxos(_m, msg) => {
                output = self.membership.run(msg);

                for key in &self.keys_expecting_quorum.clone() {
                    if let Some(machine) = self.keys.get_mut(key) {
                        if let Some(client_id) =
                            machine.ack_write_against(&self.membership.members())
                        {
                            self.keys_expecting_quorum.remove(&key);
                            output.push(HMessage::Answer(
                                client_id,
                                Answer::Write(WriteResult::Accepted),
                            ));
                            for member in &self.membership.members() {
                                output.push(HMessage::Sync(
                                    *member,
                                    HermesMessage::val(
                                        self.membership.current_epoch(),
                                        &key,
                                        &machine.timestamp,
                                    ),
                                ));
                            }
                        }
                    }
                }
            }
        }
        output
    }

    fn client_command(&mut self, client_id: ClientId, command: Commands) -> Vec<HMessage> {
        let mut output = vec![];
        match command.get_field_type() {
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
                    let state = machine.write(client_id.clone(), value.clone(), self.ts);
                    if state == WriteResult::Accepted {
                        self.keys_expecting_quorum.insert(key.clone());
                        for member in &self.membership.members() {
                            output.push(HMessage::Sync(
                                *member,
                                HermesMessage::inv(
                                    self.membership.current_epoch(),
                                    &key,
                                    &self.ts,
                                    &value,
                                ),
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
                        MachineValue::write_value(client_id, value.clone(), self.ts),
                    );
                    for member in &self.membership.members() {
                        self.keys_expecting_quorum.insert(key.clone());
                        output.push(HMessage::Sync(
                            *member,
                            HermesMessage::inv(
                                self.membership.current_epoch(),
                                &key,
                                &self.ts,
                                &value,
                            ),
                        ))
                    }
                }
            }
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

    /// inspired by the assert_eq! macro
    macro_rules! assert_msg_eq {
        ($left:expr, $right:expr $(,)?) => {{
            match (&mut $left, &mut $right) {
                (left_val, right_val) => {
                    left_val.sort_by(msg_by_member);
                    right_val.sort_by(msg_by_member);
                    assert_eq!(left_val, right_val);
                }
            }
        }};
    }

    #[test]
    fn setup_a_working_paxos() {
        let mut hermes = Hermes::new(&Config::test_setup(2, vec![1, 3]));
        let expected_membership = HashSet::from_iter(vec![Member(1), Member(2), Member(3)]);
        assert_msg_eq!(
            hermes.start(),
            vec![
                HMessage::Paxos(Member(1), PaxosMessage::new(1, Member(2), Content::P1a)),
                HMessage::Paxos(Member(3), PaxosMessage::new(1, Member(2), Content::P1a)),
            ],
        );

        assert_msg_eq!(
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

        assert_msg_eq!(
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

    // setup Hermes without the need of running the initial paxos membership agreement
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
        assert_msg_eq!(
            hermes.run(HMessage::Client(ClientId(1), write_command(&key, &value))),
            vec![
                HMessage::Sync(Member(1), HermesMessage::inv(1, &key, &expected_ts, &value)),
                HMessage::Sync(Member(3), HermesMessage::inv(1, &key, &expected_ts, &value)),
            ],
        );
        hermes.run(HMessage::Sync(
            Member(1),
            HermesMessage::ack(1, &key, &expected_ts),
        ));
        assert_msg_eq!(
            hermes.run(HMessage::Sync(
                Member(3),
                HermesMessage::ack(1, &key, &expected_ts),
            )),
            vec![
                HMessage::Answer(ClientId(1), Answer::Write(WriteResult::Accepted)),
                HMessage::Sync(Member(1), HermesMessage::val(1, &key, &expected_ts)),
                HMessage::Sync(Member(3), HermesMessage::val(1, &key, &expected_ts)),
            ],
        );
        assert_msg_eq!(
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

        assert_msg_eq!(
            hermes.run(HMessage::Sync(
                Member(2),
                HermesMessage::inv(1, &key, &timestamp, &value),
            )),
            vec![HMessage::Sync(
                Member(2),
                HermesMessage::ack(1, &key, &timestamp),
            )],
        );
        assert_msg_eq!(
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
        assert_msg_eq!(
            hermes.run(HMessage::Sync(
                Member(3),
                HermesMessage::inv(1, &key, &timestamp, &value),
            )),
            vec![HMessage::Sync(
                Member(3),
                HermesMessage::ack(1, &key, &timestamp),
            )],
        );
        assert_msg_eq!(
            hermes.run(HMessage::Client(ClientId(1), read_command(&key))),
            vec![],
        );
        assert_msg_eq!(
            hermes.run(HMessage::Sync(
                Member(3),
                HermesMessage::val(1, &key, &timestamp),
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
            HermesMessage::inv(1, &key, &timestamp, &other_value),
        ));
        hermes.run(HMessage::Sync(
            Member(3),
            HermesMessage::val(1, &key, &timestamp),
        ));

        let expected_ts = Timestamp::new(2, 2);
        assert_msg_eq!(
            hermes.run(HMessage::Client(ClientId(1), write_command(&key, &value))),
            vec![
                HMessage::Sync(Member(1), HermesMessage::inv(1, &key, &expected_ts, &value)),
                HMessage::Sync(Member(3), HermesMessage::inv(1, &key, &expected_ts, &value)),
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
        assert_msg_eq!(
            hermes.run(HMessage::Sync(
                Member(3),
                HermesMessage::inv(1, &key, &timestamp, &other_value),
            )),
            vec![
                HMessage::Answer(ClientId(2), Answer::Write(WriteResult::Rejected)),
                HMessage::Sync(Member(3), HermesMessage::ack(1, &key, &timestamp)),
            ],
        );
    }

    // This test is ignored until I have a clearer view on this point (transition between epochs)
    #[test]
    #[ignore]
    fn when_write_span_multiple_epoch_unacked_messages_are_resent() {
        let mut hermes = setup_default_running_hermes();
        let expected_membership = HashSet::from_iter(vec![Member(1), Member(2), Member(3)]);

        let key = Key(vec![35, 36, 37]);
        let value = Value(vec![1, 2, 3]);

        let timestamp = Timestamp::new(1, 3);

        hermes.run(HMessage::Client(ClientId(2), write_command(&key, &value)));
        hermes.run(HMessage::Sync(
            Member(3),
            HermesMessage::ack(1, &key, &timestamp),
        ));
        assert_msg_eq!(
            hermes.run(HMessage::Paxos(
                Member(2),
                PaxosMessage::new(123, Member(1), Content::Leasing(expected_membership))
            )),
            vec![HMessage::Sync(
                Member(1),
                HermesMessage::inv(123, &key, &timestamp, &value)
            ),],
        );
    }

    #[test]
    fn when_a_failing_node_is_excluded_send_vals_and_acks_accordingly() {
        let mut hermes = setup_default_running_hermes();
        let degraded_membership = HashSet::from_iter(vec![Member(1), Member(2)]);

        let key = Key(vec![35, 36, 37]);
        let value = Value(vec![1, 2, 3]);
        let expected_ts = Timestamp::new(1, 2);

        hermes.run(HMessage::Client(ClientId(2), write_command(&key, &value)));
        hermes.run(HMessage::Sync(
            Member(1),
            HermesMessage::ack(1, &key, &expected_ts),
        ));
        assert_msg_eq!(
            hermes.run(HMessage::Paxos(
                Member(2),
                PaxosMessage::new(123, Member(1), Content::Leasing(degraded_membership))
            )),
            vec![
                HMessage::Answer(ClientId(2), Answer::Write(WriteResult::Accepted)),
                HMessage::Sync(Member(1), HermesMessage::val(123, &key, &expected_ts))
            ],
        );
    }
}
