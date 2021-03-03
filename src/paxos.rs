use std::collections::{HashMap, HashSet};
use std::time::Instant;

use tokio::time::Duration;

use crate::paxos::State::{Leasing, LeaderVote};
use crate::state::Member;

const LEASE_TIMEOUT: Duration = Duration::from_millis(10);

#[derive(Clone, Debug, Eq, PartialEq)]
enum State {
    // P1 phase of paxos
    LeaderPreVote(HashMap<Member, HashSet<Member>>),
    // P2 phase of paxos
    LeaderVote(HashSet<Member>),
    Follower,
    // Vote done, lease in progress
    Leasing(Instant),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PaxosState {
    self_id: Member,
    epoch_id: u64,
    // configured membership
    original_membership: HashSet<Member>,
    // voted membership
    membership: HashSet<Member>,
    // membership local view
    local_membership: HashSet<Member>,
    state: State,
}

struct PMessage {
    epoch_id: u64,
    sender: Member,
    dest: Member,
    content: Content,
}

enum Content {
    P1a,
    P1b(HashSet<Member>),
    P2a(HashSet<Member>),
    P2b,
    Leasing(HashSet<Member>),
}

impl PaxosState {
    fn is_leader(&self) -> bool {
        self.epoch_id % (self.original_membership.len() as u64 + 1) == (self.self_id.0 as u64 - 1)
    }

    fn go_to_epoch(&mut self, epoch: u64) {
        self.epoch_id = epoch;
        let mut map = HashMap::new();
        map.insert(self.self_id, self.local_membership.clone());
        self.state = State::LeaderPreVote(map);
    }

    fn next_epoch(&mut self) -> Vec<PMessage> {
        if let State::Leasing(_) = self.state {
            return vec![];
        }

        self.go_to_epoch(self.epoch_id + 1);
        if self.is_leader() {
            let mut out = vec![];
            for dest in &self.original_membership {
                out.push(PMessage {
                    epoch_id: self.epoch_id,
                    sender: self.self_id,
                    dest: dest.clone(),
                    content: Content::P1a,
                })
            }
            out
        } else {
            vec![]
        }
    }

    fn run(&mut self, msg: PMessage) -> Vec<PMessage> {
        if msg.epoch_id > self.epoch_id {
            self.go_to_epoch(msg.epoch_id);
        } else if msg.epoch_id < self.epoch_id {
            return vec![];
        }
        let mut out = vec![];
        if self.is_leader() {
            let nodes = self.nodes();
            match self.state {
                State::LeaderPreVote(ref mut rcvs) => {
                    if let Content::P1b(vote) = msg.content {
                        rcvs.insert(msg.sender, vote);
                    }

                    if Self::quorum(&nodes, &rcvs.keys()) {
                        let mut intersect = HashSet::new();
                        let mut iter = rcvs.values();
                        if let Some(v) = iter.next() {
                            intersect = v.clone();
                            while let Some(v) = iter.next() {
                                intersect = intersect.intersection(v).map(|x| x.clone()).collect();
                            }
                        }
                        let mut set = HashSet::new();
                        set.insert(self.self_id);
                        self.state = State::LeaderVote(set);
                        self.membership = intersect.clone();
                        for dest in &self.original_membership {
                            out.push(PMessage {
                                epoch_id: self.epoch_id,
                                sender: self.self_id,
                                dest: dest.clone(),
                                content: Content::P2a(intersect.clone()),
                            })
                        }
                    }
                }
                State::LeaderVote(ref mut voted) => {
                    if let Content::P2b = msg.content {
                        voted.insert(msg.sender);
                    }

                    if Self::quorum(&nodes, &voted.iter()) {
                        self.local_membership = self.membership.clone();
                        self.state = State::Leasing(Instant::now());
                        for dest in &self.original_membership {
                            out.push(PMessage {
                                epoch_id: self.epoch_id,
                                sender: self.self_id,
                                dest: dest.clone(),
                                content: Content::Leasing(self.membership.clone()),
                            })
                        }
                    }
                }
                _ => {}
            }
        } else {
            match msg.content {
                Content::P1a => {
                    self.state = State::Follower;
                    out.push(PMessage {
                        epoch_id: msg.epoch_id,
                        sender: self.self_id,
                        dest: msg.sender,
                        content: Content::P1b(self.local_membership.clone()),
                    });
                }
                Content::P2a(_) => {
                    out.push(PMessage {
                        epoch_id: msg.epoch_id,
                        sender: self.self_id,
                        dest: msg.sender,
                        content: Content::P2b,
                    });
                }
                Content::Leasing(voted) => {
                    self.membership = voted.clone();
                    self.local_membership = voted;
                    self.state = Leasing(Instant::now())
                }
                _ => {}
            }
        }
        out
    }

    fn nodes(&self) -> HashSet<Member> {
        let mut nodes = self.original_membership.clone();
        nodes.insert(self.self_id);
        nodes
    }

    fn quorum<T>(membership: &HashSet<Member>, votes: &dyn ExactSizeIterator<Item = T>) -> bool {
        votes.len() > (membership.len() / 2)
    }

    fn has_lease(&self) -> bool {
        if let State::Leasing(since) = self.state {
            since.elapsed() < LEASE_TIMEOUT
        } else {
            false
        }
    }

    fn lease_membership(&self) -> Option<HashSet<Member>> {
        if self.has_lease() {
            Some(self.membership.clone())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};
    use std::iter::FromIterator;
    use std::time::Instant;

    use crate::paxos::{PaxosState, State};
    use crate::state::Member;

    #[test]
    fn leader_of_current_epoch_id_is_deterministic_with_a_modulo_on_membership_size() {
        let mut state = PaxosState {
            self_id: Member(1),
            epoch_id: 0,
            original_membership: HashSet::from_iter(vec![Member(2), Member(3)]),
            membership: HashSet::new(),
            local_membership: HashSet::new(),
            state: State::Leasing(Instant::now()),
        };
        assert!(state.is_leader());

        state.epoch_id = 1;
        assert!(!state.is_leader());

        state.epoch_id = 2;
        assert!(!state.is_leader());

        state.epoch_id = 3;
        assert!(state.is_leader());
    }

    #[test]
    fn quorum_is_correctly_calculated() {
        let original_membership = HashSet::from_iter(vec![Member(1), Member(2), Member(3)]);

        assert!(!PaxosState::quorum(&original_membership, &vec![(Member(1), 123)].iter()));
        assert!(PaxosState::quorum(&original_membership, &vec![
            (Member(1), 123),
            (Member(2), 123)
        ].iter()));
    }
}
