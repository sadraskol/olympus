use std::collections::HashSet;
use std::fmt::{Debug, Error, Formatter};

use crate::hermes::ClientId;

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct Key(pub Vec<u8>);

impl Debug for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        f.debug_tuple("Key")
            .field(&std::str::from_utf8(&self.0).unwrap())
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct Value(pub Vec<u8>);

impl Debug for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        f.debug_tuple("Value")
            .field(&std::str::from_utf8(&self.0).unwrap())
            .finish()
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub struct Member(pub i32);

#[derive(Clone, Debug, Eq, PartialEq)]
enum State {
    Valid,
    Inv,
    Write(
        // client asking for write
        ClientId,
        // received acks
        HashSet<Member>,
    ),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadResult {
    Pending,
    Value(Value),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InvalidResult {
    Discarded,
    Accepted,
    WriteCancelled(ClientId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteResult {
    Rejected,
    Accepted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MachineValue {
    value: Value,
    state: State,
    pub timestamp: Timestamp,
}

impl MachineValue {
    pub fn invalid_value(ts: Timestamp, value: Value) -> Self {
        MachineValue {
            value,
            state: State::Inv,
            timestamp: ts,
        }
    }

    // TODO replay the write if invalidated since longer then the mlt (timeout)
    pub fn read(&self) -> ReadResult {
        if self.state == State::Valid {
            ReadResult::Value(self.value.clone())
        } else {
            ReadResult::Pending
        }
    }

    pub fn write(&mut self, client: ClientId, value: Value, ts: Timestamp) -> WriteResult {
        if self.state == State::Valid {
            self.state = State::Write(client, HashSet::new());
            self.value = value;
            self.timestamp = ts;
            WriteResult::Accepted
        } else {
            WriteResult::Rejected
        }
    }

    pub fn write_value(client: ClientId, value: Value, ts: Timestamp) -> Self {
        MachineValue {
            value,
            state: State::Write(client, HashSet::new()),
            timestamp: ts,
        }
    }

    pub fn ack(&mut self, member: Member) {
        if let State::Write(_, ref mut acks) = self.state {
            acks.insert(member);
        }
    }

    /// check if acks is compatible with members in the cluster.
    /// returns true when a valid message should be sent
    pub fn ack_write_against(&mut self, membership: &HashSet<Member>) -> Option<ClientId> {
        if let State::Write(ref client_id, ref mut acks) = self.state {
            if membership.is_subset(acks) {
                let cid = client_id.clone();
                self.state = State::Valid;
                Some(cid)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn validate(&mut self, ts: Timestamp) -> ReadResult {
        if self.state == State::Inv && self.timestamp == ts {
            self.state = State::Valid;
            self.timestamp = ts;
        }

        self.read()
    }

    pub fn invalid(&mut self, ts: Timestamp, value: Value) -> InvalidResult {
        if ts > self.timestamp {
            let previous_state = self.state.clone();
            self.state = State::Inv;
            self.timestamp = ts;
            self.value = value;
            match previous_state {
                State::Write(client, _) => InvalidResult::WriteCancelled(client),
                _ => InvalidResult::Accepted,
            }
        } else {
            InvalidResult::Discarded
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Timestamp {
    pub version: u32,
    pub c_id: u32,
}

impl Timestamp {
    pub fn new(version: u32, c_id: u32) -> Self {
        Timestamp { version, c_id }
    }

    pub fn increment(&mut self) {
        self.version += 1;
    }

    pub fn increment_to(&mut self, rhs: &Timestamp) {
        self.version = rhs.version;
    }
}

#[cfg(test)]
mod test_reads {
    use std::collections::HashSet;

    use crate::state::{MachineValue, ReadResult, State, Timestamp, Value};
    use crate::ClientId;

    #[test]
    fn reading_a_valid_value_works() {
        let state = MachineValue {
            value: Value(vec![1, 2, 3]),
            state: State::Valid,
            timestamp: Timestamp::new(0, 0),
        };

        assert_eq!(state.read(), ReadResult::Value(Value(vec![1, 2, 3])));
    }

    #[test]
    fn hermes_reading_a_invalid_value_is_postponed() {
        let state = MachineValue {
            value: Value(vec![1, 2, 3]),
            state: State::Inv,
            timestamp: Timestamp::new(0, 0),
        };

        assert_eq!(state.read(), ReadResult::Pending);
    }

    #[test]
    fn hermes_reading_a_write_value_is_postponed() {
        let state = MachineValue {
            value: Value(vec![1, 2, 3]),
            state: State::Write(ClientId(1), HashSet::new()),
            timestamp: Timestamp::new(0, 0),
        };

        assert_eq!(state.read(), ReadResult::Pending);
    }
}

#[cfg(test)]
mod test_coordinator_writes {
    use std::collections::HashSet;
    use std::iter::FromIterator;

    use crate::state::{MachineValue, Member, State, Timestamp, Value, WriteResult};
    use crate::ClientId;

    #[test]
    fn writing_to_a_valid_value_enters_in_write_state() {
        let mut state = MachineValue {
            value: Value(vec![1, 2, 3]),
            state: State::Valid,
            timestamp: Timestamp::new(0, 0),
        };

        let res = state.write(ClientId(1), Value(vec![3, 2, 1]), Timestamp::new(100, 100));
        assert_eq!(res, WriteResult::Accepted);
        assert_eq!(
            state,
            MachineValue {
                value: Value(vec![3, 2, 1]),
                state: State::Write(ClientId(1), HashSet::new()),
                timestamp: Timestamp::new(100, 100),
            }
        );
    }

    #[test]
    fn acks_are_accumulated_into_the_state() {
        let mut state = MachineValue {
            value: Value(vec![1, 2, 3]),
            state: State::Write(ClientId(1), HashSet::new()),
            timestamp: Timestamp::new(0, 0),
        };

        state.ack(Member(3));
        state.ack(Member(1));
        assert_eq!(
            state,
            MachineValue {
                value: Value(vec![1, 2, 3]),
                state: State::Write(ClientId(1), HashSet::from_iter(vec![Member(3), Member(1)])),
                timestamp: Timestamp::new(0, 0),
            }
        );
    }

    #[test]
    fn when_write_is_universally_acked_write_is_committed() {
        let mut state = MachineValue {
            value: Value(vec![1, 2, 3]),
            state: State::Write(ClientId(1), HashSet::from_iter(vec![Member(3), Member(1)])),
            timestamp: Timestamp::new(0, 0),
        };

        let validate_write =
            state.ack_write_against(&HashSet::from_iter(vec![Member(3), Member(1)]));
        assert_eq!(validate_write, Some(ClientId(1)));
        assert_eq!(
            state,
            MachineValue {
                value: Value(vec![1, 2, 3]),
                state: State::Valid,
                timestamp: Timestamp::new(0, 0),
            }
        );
    }

    #[test]
    fn write_on_invalid_key_is_canceled() {
        let mut state = MachineValue {
            value: Value(vec![1, 2, 3]),
            state: State::Inv,
            timestamp: Timestamp::new(0, 0),
        };
        let expected = state.clone();

        let res = state.write(ClientId(1), Value(vec![3, 2, 1]), Timestamp::new(100, 100));
        assert_eq!(res, WriteResult::Rejected);
        assert_eq!(state, expected);
    }

    #[test]
    fn write_on_write_key_is_canceled() {
        let mut state = MachineValue {
            value: Value(vec![1, 2, 3]),
            state: State::Write(ClientId(1), HashSet::new()),
            timestamp: Timestamp::new(0, 0),
        };
        let expected = state.clone();

        let res = state.write(ClientId(1), Value(vec![3, 2, 1]), Timestamp::new(100, 100));
        assert_eq!(res, WriteResult::Rejected);
        assert_eq!(state, expected);
    }
}

#[cfg(test)]
mod invalid_state {
    // Invalid state = when a coordinator in a write receives an invalid message
    // => it must considers his write as invalid and..? what happens is unclear
    // 1) it goes to trans, receives every acks then transitions to INV state

    use std::collections::HashSet;
    use std::iter::FromIterator;

    use crate::state::{InvalidResult, MachineValue, Member, State, Timestamp, Value};
    use crate::ClientId;

    #[test]
    fn invalidation_from_the_past_during_write_is_ignored() {
        let mut state = MachineValue {
            value: Value(vec![1, 2, 3]),
            state: State::Write(ClientId(1), HashSet::new()),
            timestamp: Timestamp::new(100, 100),
        };
        let expected = state.clone();

        let res = state.invalid(Timestamp::new(1, 1), Value(vec![3, 2, 1]));
        assert_eq!(res, InvalidResult::Discarded);
        assert_eq!(state, expected);
    }

    #[test]
    fn invalidation_from_the_future_during_write_is_invalidating() {
        let mut state = MachineValue {
            value: Value(vec![1, 2, 3]),
            state: State::Write(ClientId(1), HashSet::from_iter(vec![Member(1), Member(3)])),
            timestamp: Timestamp::new(1, 1),
        };
        let res = state.invalid(Timestamp::new(100, 100), Value(vec![3, 2, 1]));
        assert_eq!(res, InvalidResult::WriteCancelled(ClientId(1)));
        assert_eq!(
            state,
            MachineValue {
                value: Value(vec![3, 2, 1]),
                state: State::Inv,
                timestamp: Timestamp::new(100, 100),
            }
        );
    }
}

#[cfg(test)]
mod test_follower_write {
    use crate::state::{InvalidResult, MachineValue, State, Timestamp, Value};

    #[test]
    fn invalidating_a_valid_key_override_the_value() {
        let mut state = MachineValue {
            value: Value(vec![1, 2, 3]),
            state: State::Valid,
            timestamp: Timestamp::new(0, 0),
        };

        let res = state.invalid(Timestamp::new(100, 100), Value(vec![3, 2, 1]));
        assert_eq!(res, InvalidResult::Accepted);
        assert_eq!(
            state,
            MachineValue {
                value: Value(vec![3, 2, 1]),
                state: State::Inv,
                timestamp: Timestamp::new(100, 100),
            }
        );
    }

    #[test]
    fn invalidating_a_valid_key_with_past_timestamp_is_ignored() {
        let mut state = MachineValue {
            value: Value(vec![1, 2, 3]),
            state: State::Valid,
            timestamp: Timestamp::new(100, 100),
        };
        let expected = state.clone();

        let res = state.invalid(Timestamp::new(1, 1), Value(vec![3, 2, 1]));
        assert_eq!(res, InvalidResult::Discarded);
        assert_eq!(state, expected);
    }

    #[test]
    fn invalidating_an_invalidated_value_fails_if_in_the_past() {
        let mut state = MachineValue {
            value: Value(vec![1, 2, 3]),
            state: State::Inv,
            timestamp: Timestamp::new(100, 100),
        };
        let expected = state.clone();

        let res = state.invalid(Timestamp::new(1, 1), Value(vec![3, 2, 1]));
        assert_eq!(res, InvalidResult::Discarded);
        assert_eq!(state, expected);
    }

    #[test]
    fn invalidating_an_invalidated_value_with_above_timestamp_overrides_the_value() {
        let mut state = MachineValue {
            value: Value(vec![1, 2, 3]),
            state: State::Inv,
            timestamp: Timestamp::new(1, 1),
        };

        let res = state.invalid(Timestamp::new(100, 100), Value(vec![3, 2, 1]));
        assert_eq!(res, InvalidResult::Accepted);
        assert_eq!(
            state,
            MachineValue {
                value: Value(vec![3, 2, 1]),
                state: State::Inv,
                timestamp: Timestamp::new(100, 100),
            }
        );
    }

    #[test]
    fn validating_an_invalidated_value_confirms_the_write() {
        let mut state = MachineValue {
            value: Value(vec![1, 2, 3]),
            state: State::Inv,
            timestamp: Timestamp::new(100, 100),
        };

        state.validate(Timestamp::new(100, 100));
        assert_eq!(
            state,
            MachineValue {
                value: Value(vec![1, 2, 3]),
                state: State::Valid,
                timestamp: Timestamp::new(100, 100),
            }
        );
    }

    #[test]
    fn validating_an_invalidated_value_without_same_timestamp_is_ignored() {
        let mut state = MachineValue {
            value: Value(vec![1, 2, 3]),
            state: State::Inv,
            timestamp: Timestamp::new(0, 0),
        };
        let expected = state.clone();

        state.validate(Timestamp::new(100, 100));
        assert_eq!(state, expected);
    }
}
