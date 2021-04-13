use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use log::{debug, info};

use crate::config::Config;
use client_interface::client::{Answer, Key, Query, Value, WriteResult};

use crate::paxos::{LeaseState, PaxosMessage, PaxosState};
use crate::state::{InvalidResult, MachineValue, Member, ReadResult, Timestamp, ValidateResult};

#[cfg(test)]
use std::ops::Add;

#[derive(Clone, Debug, PartialEq)]
pub enum HermesMessage {
    Inv {
        // TODO replace with Member
        sender_id: u32,
        epoch_id: u64,
        key: Key,
        value: Value,
        ts: Timestamp,
        rmw: bool,
    },
    Ack {
        // TODO replace with Member
        sender_id: u32,
        epoch_id: u64,
        key: Key,
        ts: Timestamp,
    },
    Val {
        // TODO replace with Member
        sender_id: u32,
        epoch_id: u64,
        key: Key,
        ts: Timestamp,
    },
}

impl HermesMessage {
    pub fn inv(
        sender_id: u32,
        epoch_id: u64,
        key: &Key,
        timestamp: &Timestamp,
        value: &Value,
        rmw: bool
    ) -> Self {
        HermesMessage::Inv {
            sender_id,
            epoch_id,
            key: key.clone(),
            value: value.clone(),
            ts: *timestamp,
            rmw
        }
    }

    pub fn ack(sender_id: u32, epoch_id: u64, key: &Key, timestamp: &Timestamp) -> Self {
        HermesMessage::Ack {
            sender_id,
            epoch_id,
            key: key.clone(),
            ts: *timestamp,
        }
    }

    pub fn val(sender_id: u32, epoch_id: u64, key: &Key, timestamp: &Timestamp) -> Self {
        HermesMessage::Val {
            sender_id,
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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestId(pub u32);

#[derive(Clone, Debug, PartialEq)]
pub enum HMessage {
    Client(RequestId, Query),
    Answer(RequestId, Answer),
    Sync(Member, HermesMessage),
    Paxos(Member, PaxosMessage),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Clock {
    #[cfg(test)]
    Controlled(Instant),
    System,
}

impl Clock {
    pub fn now(&self) -> Instant {
        match self {
            #[cfg(test)]
            Clock::Controlled(now) => *now,
            Clock::System => Instant::now(),
        }
    }

    pub fn elapsed(&self, since: Instant) -> Duration {
        match self {
            #[cfg(test)]
            Clock::Controlled(now) => *now - since,
            Clock::System => since.elapsed(),
        }
    }

    #[cfg(test)]
    pub fn add(&mut self, duration: Duration) {
        match self {
            #[cfg(test)]
            Clock::Controlled(ref mut now) => *now = now.add(duration),
            Clock::System => panic!("cannot modify system clock"),
        };
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Hermes {
    internal_clock: Clock,
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
            internal_clock: Clock::System,
            keys: HashMap::new(),
            keys_expecting_quorum: HashSet::new(),
            ts: Timestamp::new(0, config.id as u32),
            membership: PaxosState::new(config),
            inbox: vec![],
            pending_reads: HashMap::new(),
        }
    }

    #[cfg(test)]
    pub fn force_clock(&mut self, instant: Instant) {
        self.internal_clock = Clock::Controlled(instant);
    }

    pub fn start(&mut self) -> Vec<HMessage> {
        self.membership.next_epoch(&self.internal_clock)
    }

    pub fn run(&mut self, message: HMessage) -> Vec<HMessage> {
        debug!("{:?}", self);
        let mut output = vec![];
        match message {
            HMessage::Client(req_id, query) => output = self.client_query(req_id, query),
            HMessage::Sync(member, msg) => {
                if msg.epoch_id() == self.membership.current_epoch() {
                    match msg {
                        HermesMessage::Inv {
                            sender_id: _,
                            epoch_id,
                            key,
                            value,
                            ts,
                            rmw,
                        } => {
                            info!(
                                "invalidate epoch_id: {}, key: {:?}, value: {:?}, ts: {:?}, rmw: {}",
                                epoch_id, key, value, ts, rmw
                            );
                            // TODO if ts of message is older, do not increment!
                            self.ts.increment_to(&ts);

                            if let Some(machine) = self.keys.get_mut(&key) {
                                if let InvalidResult::WriteCancelled(client) =
                                    machine.invalid(self.internal_clock.now(), ts, value)
                                {
                                    output.push(HMessage::Answer(
                                        client,
                                        Answer::Write(WriteResult::Rejected),
                                    ));
                                }
                            } else {
                                self.keys.insert(
                                    key.clone(),
                                    MachineValue::invalid_value(
                                        self.internal_clock.now(),
                                        ts,
                                        value,
                                    ),
                                );
                            }
                            output.push(HMessage::Sync(
                                member,
                                HermesMessage::ack(
                                    self.membership.self_id.0,
                                    self.membership.current_epoch(),
                                    &key,
                                    &ts,
                                ),
                            ));
                        }
                        HermesMessage::Ack { key, .. } => {
                            info!("ack key: {:?}", key);

                            if let Some(machine) = self.keys.get_mut(&key) {
                                machine.ack(member);
                                if let Some(req_id) =
                                    machine.ack_write_against(&self.membership.members())
                                {
                                    self.keys_expecting_quorum.remove(&key);
                                    output.push(HMessage::Answer(
                                        req_id,
                                        Answer::Write(WriteResult::Accepted),
                                    ));
                                    for member in &self.membership.members() {
                                        output.push(HMessage::Sync(
                                            *member,
                                            HermesMessage::val(
                                                self.membership.self_id.0,
                                                self.membership.current_epoch(),
                                                &key,
                                                &machine.timestamp,
                                            ),
                                        ));
                                    }
                                }
                            }
                        }
                        HermesMessage::Val {
                            sender_id: _,
                            epoch_id,
                            key,
                            ts,
                        } => {
                            info!(
                                "validate epoch_id: {}, key: {:?}, ts: {:?}",
                                epoch_id, key, ts
                            );

                            if let Some(machine) = self.keys.get_mut(&key) {
                                match machine.validate(ts) {
                                    ValidateResult::Value(value) => {
                                        for client_waiting in
                                            self.pending_reads.get(&key).unwrap_or(&vec![])
                                        {
                                            if let HMessage::Client(req_id, _) = client_waiting {
                                                output.push(HMessage::Answer(
                                                    req_id.clone(),
                                                    Answer::Read(Some(value.clone())),
                                                ));
                                            }
                                        }
                                    }
                                    ValidateResult::UnlockWrite(req_id) => {
                                        output.push(HMessage::Answer(
                                            req_id,
                                            Answer::Write(WriteResult::Accepted),
                                        ));
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
                output = self.membership.run(&self.internal_clock, msg);

                for key in &self.keys_expecting_quorum.clone() {
                    if let Some(machine) = self.keys.get_mut(key) {
                        if let Some(req_id) = machine.ack_write_against(&self.membership.members())
                        {
                            self.keys_expecting_quorum.remove(&key);
                            output.push(HMessage::Answer(
                                req_id,
                                Answer::Write(WriteResult::Accepted),
                            ));
                            for member in &self.membership.members() {
                                output.push(HMessage::Sync(
                                    *member,
                                    HermesMessage::val(
                                        self.membership.self_id.0,
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

    fn client_query(&mut self, req_id: RequestId, query: Query) -> Vec<HMessage> {
        let mut output = vec![];
        match &query {
            Query::Read(key) => {
                info!("reading key: {:?}", key);

                if let Some(value) = self.keys.get_mut(key) {
                    match value.read(&self.internal_clock, req_id.clone()) {
                        ReadResult::Pending => {
                            if let Some(vec) = self.pending_reads.get_mut(key) {
                                vec.push(HMessage::Client(req_id, query));
                            } else {
                                self.pending_reads
                                    .insert(key.clone(), vec![HMessage::Client(req_id, query)]);
                            }
                        }
                        ReadResult::ReplayWrite(ts, value, rmw) => {
                            self.keys_expecting_quorum.insert(key.clone());
                            for member in &self.membership.members() {
                                output.push(HMessage::Sync(
                                    *member,
                                    HermesMessage::inv(
                                        self.membership.self_id.0,
                                        self.membership.current_epoch(),
                                        &key,
                                        &ts,
                                        &value,
                                        rmw
                                    ),
                                ))
                            }
                        }
                        ReadResult::Value(value) => {
                            output.push(HMessage::Answer(req_id, Answer::Read(Some(value))));
                        }
                    }
                } else {
                    output.push(HMessage::Answer(req_id, Answer::Read(None)));
                }
            }
            Query::Write(key, value) => {
                self.write(&mut output, req_id, key, value, false);
            },
            Query::RMW(key, value) => {
                self.write(&mut output, req_id, key, value, false);
            }
        }
        output
    }

    fn write(&mut self, output: &mut Vec<HMessage>, req_id: RequestId, key: &Key, value: &Value, rmw: bool) {
        if rmw {
            self.ts.increment();
        } else {
            self.ts.increment();
            self.ts.increment();
        }
        info!(
            "writing key: {:?}, value: {:?}, ts: {:?}, rmw: {}",
            key, value, self.ts, rmw
        );

        if let Some(machine) = self.keys.get_mut(&key) {
            let state = machine.write(req_id.clone(), value.clone(), self.ts);
            if state == WriteResult::Accepted {
                self.keys_expecting_quorum.insert(key.clone());
                for member in &self.membership.members() {
                    output.push(HMessage::Sync(
                        *member,
                        HermesMessage::inv(
                            self.membership.self_id.0,
                            self.membership.current_epoch(),
                            &key,
                            &self.ts,
                            &value,
                            rmw
                        ),
                    ))
                }
            } else {
                output.push(HMessage::Answer(
                    req_id,
                    Answer::Write(WriteResult::Rejected),
                ))
            }
        } else {
            self.keys.insert(
                key.clone(),
                MachineValue::write_value(req_id, value.clone(), self.ts),
            );
            for member in &self.membership.members() {
                self.keys_expecting_quorum.insert(key.clone());
                output.push(HMessage::Sync(
                    *member,
                    HermesMessage::inv(
                        self.membership.self_id.0,
                        self.membership.current_epoch(),
                        &key,
                        &self.ts,
                        &value,
                        rmw
                    ),
                ))
            }
        }
    }

    pub fn lease_state(&self) -> LeaseState {
        self.membership.lease_state()
    }

    pub fn failing_member(&mut self, member: Member) {
        self.membership.failing_member(member);
    }
}

#[cfg(test)]
mod hermes_test {
    use std::cmp::Ordering;
    use std::collections::HashSet;
    use std::iter::FromIterator;

    use crate::config::Config;
    use client_interface::client::{Query, WriteResult};

    use crate::hermes::{Answer, HMessage, Hermes, HermesMessage, RequestId};
    use crate::paxos::{Content, PaxosMessage};
    use crate::state::{Member, Timestamp};
    use client_interface::client::{Key, Value};
    use std::time::{Duration, Instant};

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
        hermes.force_clock(Instant::now());
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

        let expected_ts = Timestamp::new(2, 2);
        assert_msg_eq!(
            hermes.run(HMessage::Client(
                RequestId(1),
                Query::write(key.clone(), value.clone())
            )),
            vec![
                HMessage::Sync(
                    Member(1),
                    HermesMessage::inv(2, 1, &key, &expected_ts, &value, false)
                ),
                HMessage::Sync(
                    Member(3),
                    HermesMessage::inv(2, 1, &key, &expected_ts, &value, false)
                ),
            ],
        );
        hermes.run(HMessage::Sync(
            Member(1),
            HermesMessage::ack(1, 1, &key, &expected_ts),
        ));
        assert_msg_eq!(
            hermes.run(HMessage::Sync(
                Member(3),
                HermesMessage::ack(2, 1, &key, &expected_ts),
            )),
            vec![
                HMessage::Answer(RequestId(1), Answer::Write(WriteResult::Accepted)),
                HMessage::Sync(Member(1), HermesMessage::val(2, 1, &key, &expected_ts)),
                HMessage::Sync(Member(3), HermesMessage::val(2, 1, &key, &expected_ts)),
            ],
        );
        assert_msg_eq!(
            hermes.run(HMessage::Client(RequestId(4), Query::read(key.clone()))),
            vec![HMessage::Answer(RequestId(4), Answer::Read(Some(value)))],
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
                HermesMessage::inv(2, 1, &key, &timestamp, &value, false),
            )),
            vec![HMessage::Sync(
                Member(2),
                HermesMessage::ack(2, 1, &key, &timestamp),
            )],
        );
        assert_msg_eq!(
            hermes.run(HMessage::Client(
                RequestId(1),
                Query::write(key.clone(), value.clone())
            )),
            vec![HMessage::Answer(
                RequestId(1),
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
                HermesMessage::inv(2, 1, &key, &timestamp, &value, false),
            )),
            vec![HMessage::Sync(
                Member(3),
                HermesMessage::ack(2, 1, &key, &timestamp),
            )],
        );
        assert_msg_eq!(
            hermes.run(HMessage::Client(RequestId(1), Query::read(key.clone()))),
            vec![],
        );
        assert_msg_eq!(
            hermes.run(HMessage::Sync(
                Member(3),
                HermesMessage::val(2, 1, &key, &timestamp),
            )),
            vec![HMessage::Answer(RequestId(1), Answer::Read(Some(value)))],
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
            HermesMessage::inv(3, 1, &key, &timestamp, &other_value, false),
        ));
        hermes.run(HMessage::Sync(
            Member(3),
            HermesMessage::val(3, 1, &key, &timestamp),
        ));

        let expected_ts = Timestamp::new(3, 2);
        assert_msg_eq!(
            hermes.run(HMessage::Client(
                RequestId(1),
                Query::write(key.clone(), value.clone())
            )),
            vec![
                HMessage::Sync(
                    Member(1),
                    HermesMessage::inv(2, 1, &key, &expected_ts, &value, false)
                ),
                HMessage::Sync(
                    Member(3),
                    HermesMessage::inv(2, 1, &key, &expected_ts, &value, false)
                ),
            ],
        );
    }

    #[test]
    fn when_write_are_invalidated_by_another_node_send_refused_write() {
        let mut hermes = setup_default_running_hermes();

        let key = Key(vec![35, 36, 37]);
        let value = Value(vec![1, 2, 3]);
        let other_value = Value(vec![40, 41, 42]);

        let timestamp = Timestamp::new(2, 3);

        hermes.run(HMessage::Client(
            RequestId(2),
            Query::write(key.clone(), value.clone()),
        ));
        assert_msg_eq!(
            hermes.run(HMessage::Sync(
                Member(3),
                HermesMessage::inv(2, 1, &key, &timestamp, &other_value, false),
            )),
            vec![
                HMessage::Answer(RequestId(2), Answer::Write(WriteResult::Rejected)),
                HMessage::Sync(Member(3), HermesMessage::ack(2, 1, &key, &timestamp)),
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

        hermes.run(HMessage::Client(
            RequestId(2),
            Query::write(key.clone(), value.clone()),
        ));
        hermes.run(HMessage::Sync(
            Member(3),
            HermesMessage::ack(3, 1, &key, &timestamp),
        ));
        assert_msg_eq!(
            hermes.run(HMessage::Paxos(
                Member(2),
                PaxosMessage::new(123, Member(1), Content::Leasing(expected_membership))
            )),
            vec![HMessage::Sync(
                Member(1),
                HermesMessage::inv(2, 123, &key, &timestamp, &value, false)
            ),],
        );
    }

    #[test]
    fn when_a_failing_node_is_excluded_send_vals_and_acks_accordingly() {
        let mut hermes = setup_default_running_hermes();
        let degraded_membership = HashSet::from_iter(vec![Member(1), Member(2)]);

        let key = Key(vec![35, 36, 37]);
        let value = Value(vec![1, 2, 3]);
        let expected_ts = Timestamp::new(2, 2);

        hermes.run(HMessage::Client(
            RequestId(2),
            Query::write(key.clone(), value.clone()),
        ));
        hermes.run(HMessage::Sync(
            Member(1),
            HermesMessage::ack(1, 1, &key, &expected_ts),
        ));
        assert_msg_eq!(
            hermes.run(HMessage::Paxos(
                Member(2),
                PaxosMessage::new(123, Member(1), Content::Leasing(degraded_membership))
            )),
            vec![
                HMessage::Answer(RequestId(2), Answer::Write(WriteResult::Accepted)),
                HMessage::Sync(Member(1), HermesMessage::val(2, 123, &key, &expected_ts))
            ],
        );
    }

    #[test]
    fn replay_write_behaves_correctly_when_a_node_is_removed() {
        let mut hermes = setup_default_running_hermes();

        let key = Key(vec![35, 36, 37]);
        let value = Value(vec![1, 2, 3]);
        let timestamp = Timestamp::new(1, 3);

        hermes.run(HMessage::Sync(
            Member(3),
            HermesMessage::inv(3, 1, &key, &timestamp, &value, false),
        ));
        hermes.internal_clock.add(Duration::from_secs(11));

        assert_msg_eq!(
            hermes.run(HMessage::Client(RequestId(3), Query::read(key.clone()))),
            vec![
                HMessage::Sync(
                    Member(1),
                    HermesMessage::inv(2, 1, &key, &timestamp, &value, false)
                ),
                HMessage::Sync(
                    Member(3),
                    HermesMessage::inv(2, 1, &key, &timestamp, &value, false)
                ),
            ],
        );
        hermes.run(HMessage::Sync(
            Member(1),
            HermesMessage::ack(1, 1, &key, &timestamp),
        ));
        assert_msg_eq!(
            hermes.run(HMessage::Sync(
                Member(3),
                HermesMessage::ack(2, 1, &key, &timestamp)
            )),
            vec![
                HMessage::Answer(RequestId(3), Answer::Write(WriteResult::Accepted)),
                HMessage::Sync(Member(1), HermesMessage::val(2, 1, &key, &timestamp)),
                HMessage::Sync(Member(3), HermesMessage::val(2, 1, &key, &timestamp)),
            ],
        )
    }

    #[test]
    fn when_peer_writes_but_receives_the_write_replay_of_another_member_it_accepts() {
        let mut hermes = setup_default_running_hermes();

        let key = Key(vec![35, 36, 37]);
        let value = Value(vec![1, 2, 3]);
        let timestamp = Timestamp::new(1, 2);

        hermes.run(HMessage::Client(
            RequestId(3),
            Query::write(key.clone(), value.clone()),
        ));

        assert_msg_eq!(
            hermes.run(HMessage::Sync(
                Member(3),
                HermesMessage::inv(2, 1, &key, &timestamp, &value, false)
            )),
            vec![HMessage::Sync(
                Member(3),
                HermesMessage::ack(2, 1, &key, &timestamp)
            )],
        );
        assert_msg_eq!(
            hermes.run(HMessage::Sync(
                Member(3),
                HermesMessage::val(2, 1, &key, &timestamp)
            )),
            vec![HMessage::Answer(
                RequestId(3),
                Answer::Write(WriteResult::Accepted)
            )],
        )
    }
}
