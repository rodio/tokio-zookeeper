use tracing::Instrument;
use tracing::{debug, info, trace, trace_span};

use crate::Acl;
use crate::CreateMode;
use crate::ZooKeeper;

/// Participate in leader election through this struct
#[derive(Debug, Clone)]
pub struct LeaderElection {
    /// ZNode under which volunteers are registered
    election_node: String,
    zk: ZooKeeper,
    my_path: Option<String>,
}

impl LeaderElection {
    /// Create a new leader election struct
    pub fn new(zk: ZooKeeper, election_node: &str) -> Self {
        Self {
            election_node: election_node.to_string(),
            zk,
            my_path: None,
        }
    }
    /// Participate in leader election
    pub async fn volunteer(mut self) -> Result<futures::channel::oneshot::Receiver<()>, ()> {
        info!("volunteering for leader election");
        let path = self
            .zk
            .create(
                &format!("{}/guid-n_", self.election_node), // todo guid
                &b""[..],
                Acl::open_unsafe(),
                CreateMode::EphemeralSequential,
            )
            .await
            .unwrap()
            .unwrap();
        self.my_path = Some(path.clone());

        let (leader_sender, leader_receiver) = futures::channel::oneshot::channel();
        tokio::spawn(
            self.observe(leader_sender)
                .instrument(trace_span!("election_observer", my_path = %path)),
        );

        Ok(leader_receiver)
    }

    async fn observe(self, leader_sender: futures::channel::oneshot::Sender<()>) {
        assert!(self.my_path.is_some());
        loop {
            let mut children: Vec<ElectionChild> = self
                .zk
                .get_children(&self.election_node)
                .await
                .unwrap()
                .unwrap()
                .into_iter()
                .map(|s| ElectionChild::try_from(s).unwrap())
                .collect();

            children.sort_unstable();

            trace!(participants = ?children, "got leader election participants");

            match children
                    .iter()
                    .position(|s| format!("{}/{}", self.election_node, s.0) == *self.my_path.as_ref().unwrap()) // todo get rid of formats
                {
                    Some(0) => {
                        info!("i am the leader"); 
                        _ = leader_sender.send(()); // todo check error
                        return; // todo acknowledge that users may want to create a node to acknowledge
                    }
                    Some(index) => {
                        info!("i am a follower");
                        let preceeding_node =
                            ElectionChild::try_from(format!("{}/{}", self.election_node, children.get(index-1).unwrap().0)).unwrap();
                        debug!(?preceeding_node, "setting the watch for the preceeding node");
                        let (rx, _stat) = self.zk.with_watcher().exists(&preceeding_node.0).await.unwrap(); // todo check it existed TOCTOU
                        let event = rx.await.unwrap(); // todo check that it is a delete event 
                        debug!(?event, "preceeding node was removed");
                    }
                    None => unimplemented!("can't find myself"), // TOCTOU
                };
        }
    }
}

#[derive(PartialEq, Eq, Debug, Ord)]
struct ElectionChild(String);

impl TryFrom<String> for ElectionChild {
    type Error = (); // todo
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(Self(value)) // todo
    }
}

impl PartialOrd for ElectionChild {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0) // todo change when guids
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ZooKeeperBuilder;
    use std::time::Duration;
    use tokio::time;

    fn init_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
    }

    #[tokio::test]
    async fn election_works() {
        let builder = ZooKeeperBuilder::default();
        let connect_addr = "127.0.0.1:2181".parse().unwrap();

        init_tracing_subscriber();

        let (zk1, _w) = builder.connect(&connect_addr).await.unwrap();
        let leader_election1 = LeaderElection::new(zk1.clone(), "/election");
        let f1 = leader_election1.volunteer();
        let mut rx1 = f1.await.unwrap();
        assert!(
            wait_for_leadership(&mut rx1).await,
            "testing that the first participant becomes the leader"
        );

        let (zk2, _w) = builder.connect(&connect_addr).await.unwrap();
        let leader_election2 = LeaderElection::new(zk2, "/election");
        let f2 = leader_election2.volunteer();
        let mut rx2 = f2.await.unwrap();

        assert_eq!(
            wait_for_leadership(&mut rx2).await,
            false,
            "testing that the second participant is not the leader"
        );

        drop(zk1);
        assert_eq!(
            wait_for_leadership(&mut rx2).await,
            true,
            "testing that the second participant now becomes the leader"
        );
    }

    async fn wait_for_leadership(rx: &mut futures::channel::oneshot::Receiver<()>) -> bool {
        let mut retries = 0;
        loop {
            match rx.try_recv() {
                Ok(Some(_)) => return true,
                Ok(None) => {
                    retries += 1;
                    _ = time::sleep(Duration::from_millis(100)).await;
                    if retries > 50 {
                        return false;
                    }
                }
                _ => {
                    panic!("closed channel");
                }
            }
        }
    }
}
