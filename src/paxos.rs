use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::ops::Sub;
use std::time::Instant;

use log::info;
use tokio::time::Duration;

use olympus::config::Config;

use crate::hermes::HMessage;
use crate::state::Member;

const LEASE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Eq, PartialEq)]
enum State {
    // P1 phase of paxos
    LeaderPreVote(HashMap<Member, HashSet<Member>>),
    // P2 phase of paxos (voters, candidate value)
    LeaderVote(HashSet<Member>, HashSet<Member>),
    Follower,
    // Vote done, lease in progress
    Leasing,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaxosState {
    self_id: Member,
    current_epoch_id: u64,
    next_epoch_id: u64,
    // configured membership
    original_membership: HashSet<Member>,
    // voted membership
    membership: HashSet<Member>,
    // membership local view
    local_membership: HashSet<Member>,
    state: State,
    since: Instant,
}

impl PaxosState {
    pub fn new(config: &Config) -> Self {
        PaxosState {
            self_id: Member(config.id as u32),
            current_epoch_id: 0,
            next_epoch_id: 1,
            original_membership: config.peers.iter().map(|p| Member(p.id as u32)).collect(),
            membership: HashSet::new(),
            local_membership: config.peers.iter().map(|p| Member(p.id as u32)).collect(),
            state: State::Follower,
            since: Instant::now().sub(Duration::from_secs(1_000_000)),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PaxosMessage {
    pub epoch_id: u64,
    pub sender: Member,
    pub content: Content,
}

impl PaxosMessage {
    pub fn new(epoch_id: u64, sender: Member, content: Content) -> Self {
        PaxosMessage {
            epoch_id,
            sender,
            content,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Content {
    P1a,
    P1b(HashSet<Member>),
    P2a(HashSet<Member>),
    P2b,
    Leasing(HashSet<Member>),
}

impl PaxosState {
    fn is_leader(&self, epoch_id: u64) -> bool {
        epoch_id % (self.original_membership.len() as u64 + 1) == (self.self_id.0 as u64 - 1)
    }

    fn go_to_epoch(&mut self, epoch: u64) {
        self.next_epoch_id = epoch;
        if self.is_leader(self.next_epoch_id) {
            let mut map = HashMap::new();
            map.insert(self.self_id, self.wanted_nodes());
            self.state = State::LeaderPreVote(map);
        } else {
            self.state = State::Follower;
        }
        self.since = Instant::now();
    }

    pub fn next_epoch(&mut self) -> Vec<HMessage> {
        self.go_to_epoch(self.current_epoch_id + 1);
        if self.is_leader(self.next_epoch_id) {
            let mut out = vec![];
            for dest in &self.original_membership {
                out.push(HMessage::Paxos(
                    *dest,
                    PaxosMessage {
                        epoch_id: self.next_epoch_id,
                        sender: self.self_id,
                        content: Content::P1a,
                    },
                ))
            }
            out
        } else {
            vec![]
        }
    }

    pub fn run(&mut self, msg: PaxosMessage) -> Vec<HMessage> {
        info!("paxos message: {:?}", msg);

        match msg.epoch_id.cmp(&self.next_epoch_id) {
            Ordering::Less => return vec![],
            Ordering::Equal => {}
            Ordering::Greater => self.go_to_epoch(msg.epoch_id),
        }
        let mut out = vec![];
        if self.is_leader(self.next_epoch_id) {
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
                            for v in iter {
                                intersect = intersect.intersection(v).copied().collect();
                            }
                        }
                        let mut set = HashSet::new();
                        set.insert(self.self_id);
                        self.state = State::LeaderVote(set, intersect.clone());
                        info!("transition from P1 to P2");
                        for dest in &self.original_membership {
                            out.push(HMessage::Paxos(
                                *dest,
                                PaxosMessage {
                                    epoch_id: self.next_epoch_id,
                                    sender: self.self_id,
                                    content: Content::P2a(intersect.clone()),
                                },
                            ))
                        }
                    }
                }
                State::LeaderVote(ref mut voted, ref mut chosen) => {
                    if let Content::P2b = msg.content {
                        voted.insert(msg.sender);
                    }

                    if Self::quorum(&nodes, &voted.iter()) {
                        info!("P2 to leasing phase with membership {:?}", chosen);
                        self.membership = chosen.clone();
                        self.local_membership = chosen.clone();
                        self.since = Instant::now();
                        self.current_epoch_id = self.next_epoch_id;
                        self.state = State::Leasing;
                        for dest in &self.original_membership {
                            out.push(HMessage::Paxos(
                                *dest,
                                PaxosMessage {
                                    epoch_id: self.current_epoch_id,
                                    sender: self.self_id,
                                    content: Content::Leasing(self.membership.clone()),
                                },
                            ))
                        }
                    }
                }
                State::Follower => panic!("leader of round is in follower state!"),
                State::Leasing => {}
            }
        } else {
            match msg.content {
                Content::P1a => {
                    self.state = State::Follower;
                    out.push(HMessage::Paxos(
                        msg.sender,
                        PaxosMessage {
                            epoch_id: msg.epoch_id,
                            sender: self.self_id,
                            content: Content::P1b(self.wanted_nodes()),
                        },
                    ));
                }
                Content::P2a(_) => {
                    out.push(HMessage::Paxos(
                        msg.sender,
                        PaxosMessage {
                            epoch_id: msg.epoch_id,
                            sender: self.self_id,
                            content: Content::P2b,
                        },
                    ));
                }
                Content::Leasing(voted) => {
                    info!("follower transitions to leasing {:?}", voted);
                    self.membership = voted.clone();
                    self.local_membership = voted;
                    self.current_epoch_id = msg.epoch_id;
                    self.next_epoch_id = msg.epoch_id;
                    self.state = State::Leasing;
                    self.since = Instant::now();
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

    fn wanted_nodes(&self) -> HashSet<Member> {
        let mut nodes = self.local_membership.clone();
        nodes.insert(self.self_id);
        nodes
    }

    fn quorum<T>(membership: &HashSet<Member>, votes: &dyn ExactSizeIterator<Item = T>) -> bool {
        votes.len() > (membership.len() / 2)
    }

    pub fn members(&self) -> HashSet<Member> {
        let mut set = self.membership.clone();
        set.remove(&self.self_id);
        set
    }

    pub fn lease_state(&self) -> LeaseState {
        if State::Leasing == self.state {
            if self.since.elapsed() < LEASE_TIMEOUT {
                LeaseState::PendingUntil(LEASE_TIMEOUT - self.since.elapsed())
            } else {
                LeaseState::Expired
            }
        } else if self.since.elapsed() < LEASE_TIMEOUT {
            LeaseState::Renewing(LEASE_TIMEOUT - self.since.elapsed())
        } else {
            LeaseState::Expired
        }
    }

    pub fn current_epoch(&self) -> u64 {
        self.current_epoch_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LeaseState {
    Expired,
    Renewing(Duration),
    PendingUntil(Duration),
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::iter::FromIterator;
    use std::time::Instant;

    use crate::paxos::{PaxosState, State};
    use crate::state::Member;

    #[test]
    fn leader_of_current_epoch_id_is_deterministic_with_a_modulo_on_membership_size() {
        let state = PaxosState {
            self_id: Member(1),
            current_epoch_id: 0,
            next_epoch_id: 1,
            original_membership: HashSet::from_iter(vec![Member(2), Member(3)]),
            membership: HashSet::new(),
            local_membership: HashSet::new(),
            state: State::Leasing,
            since: Instant::now(),
        };
        assert!(state.is_leader(0));
        assert!(!state.is_leader(1));
        assert!(!state.is_leader(2));
        assert!(state.is_leader(3));
    }

    #[test]
    fn quorum_is_correctly_calculated() {
        let original_membership = HashSet::from_iter(vec![Member(1), Member(2), Member(3)]);

        assert!(!PaxosState::quorum(
            &original_membership,
            &vec![(Member(1), 123)].iter(),
        ));
        assert!(PaxosState::quorum(
            &original_membership,
            &vec![(Member(1), 123), (Member(2), 123)].iter(),
        ));
    }
}
