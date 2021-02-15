use std::collections::{HashSet};

mod proto;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // let size = stream.read_u64().await?;
    // let mut buf = Vec::<u8>::with_capacity(size as usize);
    // buf.resize(size as usize, 0);
    // stream.read_exact(&mut buf).await?;
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct Key(Vec<u8>);

#[derive(Clone, Debug, Eq, PartialEq)]
struct Value(Vec<u8>);

/*
#[derive(Clone, Debug, Eq, PartialEq)]
enum Query {
    Write(Key, Value),
    Read(Key),
}

impl Query {
    fn from_bytes(bytes: &[u8]) -> std::io::Result<Query> {
        let c = Commands::parse_from_bytes(bytes)?;
        match c.get_field_type() {
            Commands_CommandType::Read => Ok(Query::Read(Key(c.get_read().get_key().to_vec()))),
            Commands_CommandType::Write => Ok(Query::Write(
                Key(c.get_write().get_key().to_vec()),
                Value(c.get_write().get_value().to_vec()),
            )),
        }
    }
}
*/

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct Member(i32);

#[derive(Clone, Debug, Eq, PartialEq)]
enum State {
    Valid,
    Inv,
    Write(
        // received acks
        HashSet<Member>,
    ),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MachineValue {
    value: Value,
    state: State,
    timestamp: Timestamp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReadResult {
    Pending,
    Value(Value),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum WriteResult {
    Rejected,
    Accepted,
}

impl MachineValue {
    fn read(self) -> ReadResult {
        if self.state == State::Valid {
            ReadResult::Value(self.value)
        } else {
            ReadResult::Pending
        }
    }

    fn write(&mut self, value: Value) -> WriteResult {
        if self.state == State::Valid {
            self.state = State::Write(HashSet::new());
            self.value = value;
            WriteResult::Accepted
        } else {
            WriteResult::Rejected
        }
    }

    fn ack(&mut self, member: Member) {
        if let State::Write(ref mut acks) = self.state {
            acks.insert(member);
        }
    }

    /// check if acks is compatible with members in the cluster.
    /// returns true when a valid message should be sent
    fn ack_write_against(&mut self, membership: &HashSet<Member>) -> bool {
        if let State::Write(ref mut acks) = self.state {
                if membership.is_subset(acks) {
                    self.state = State::Valid;
                    true
                } else {
                    false
                }
        } else {
            false
        }
    }

    fn validate(&mut self, ts: Timestamp) {
        if self.state == State::Inv && self.timestamp == ts {
            self.state = State::Valid;
            self.timestamp = ts;
        }
    }

    fn invalid(&mut self, ts: Timestamp, value: Value) {
        if self.state == State::Valid && ts > self.timestamp {
            self.state = State::Inv;
            self.timestamp = ts;
            self.value = value;
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct Timestamp {
    version: u32,
    c_id: u32,
}

impl Timestamp {
    fn new(version: u32, c_id: u32) -> Self {
        Timestamp { version, c_id }
    }
}

#[cfg(test)]
mod test_reads {
    use std::collections::HashSet;

    use crate::{MachineValue, ReadResult, State, Timestamp, Value};

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
            state: State::Write(HashSet::new()),
            timestamp: Timestamp::new(0, 0),
        };

        assert_eq!(state.read(), ReadResult::Pending);
    }
}

#[cfg(test)]
mod test_coordinator_writes {
    use std::collections::HashSet;
    use std::iter::FromIterator;

    use crate::{MachineValue, Member, State, Timestamp, Value, WriteResult};

    #[test]
    fn writing_to_a_valid_value_enters_in_write_state() {
        let mut state = MachineValue {
            value: Value(vec![1, 2, 3]),
            state: State::Valid,
            timestamp: Timestamp::new(0, 0),
        };

        let res = state.write(Value(vec![3, 2, 1]));
        assert_eq!(res, WriteResult::Accepted);
        assert_eq!(
            state,
            MachineValue {
                value: Value(vec![3, 2, 1]),
                state: State::Write(HashSet::new()),
                timestamp: Timestamp::new(0, 0)
            }
        );
    }

    #[test]
    fn acks_are_accumulated_into_the_state() {
        let mut state = MachineValue {
            value: Value(vec![1, 2, 3]),
            state: State::Write(HashSet::new()),
            timestamp: Timestamp::new(0, 0),
        };

        state.ack(Member(3));
        state.ack(Member(1));
        assert_eq!(
            state,
            MachineValue {
                value: Value(vec![1, 2, 3]),
                state: State::Write(HashSet::from_iter(vec![Member(3), Member(1)])),
                timestamp: Timestamp::new(0, 0)
            }
        );
    }

    #[test]
    fn when_write_is_universally_acked_write_is_committed() {
        let mut state = MachineValue {
            value: Value(vec![1, 2, 3]),
            state: State::Write(HashSet::from_iter(vec![Member(3), Member(1)])),
            timestamp: Timestamp::new(0, 0),
        };

        let validate_write =
            state.ack_write_against(&HashSet::from_iter(vec![Member(3), Member(1)]));
        assert!(validate_write);
        assert_eq!(
            state,
            MachineValue {
                value: Value(vec![1, 2, 3]),
                state: State::Valid,
                timestamp: Timestamp::new(0, 0)
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

        let res = state.write(Value(vec![3, 2, 1]));
        assert_eq!(res, WriteResult::Rejected);
        assert_eq!(state, expected);
    }

    #[test]
    fn write_on_write_key_is_canceled() {
        let mut state = MachineValue {
            value: Value(vec![1, 2, 3]),
            state: State::Write(HashSet::new()),
            timestamp: Timestamp::new(0, 0),
        };
        let expected = state.clone();

        let res = state.write(Value(vec![3, 2, 1]));
        assert_eq!(res, WriteResult::Rejected);
        assert_eq!(state, expected);
    }
}

#[cfg(test)]
mod test_follower_write {
    use crate::{MachineValue, State, Timestamp, Value};

    #[test]
    fn invalidating_a_valid_key_override_the_value() {
        let mut state = MachineValue {
            value: Value(vec![1, 2, 3]),
            state: State::Valid,
            timestamp: Timestamp::new(0, 0),
        };

        state.invalid(Timestamp::new(100, 100), Value(vec![3, 2, 1]));
        assert_eq!(
            state,
            MachineValue {
                value: Value(vec![3, 2, 1]),
                state: State::Inv,
                timestamp: Timestamp::new(100, 100)
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

        state.invalid(Timestamp::new(1, 1), Value(vec![3, 2, 1]));
        assert_eq!(state, expected);
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
                timestamp: Timestamp::new(100, 100)
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
