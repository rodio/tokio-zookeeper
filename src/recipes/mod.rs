use futures::channel::oneshot::Canceled;
use snafu::{OptionExt, ResultExt, Whatever};
use tokio::task::JoinHandle;
use tracing::{Instrument, error, warn};
use tracing::{debug, info, trace, trace_span};

use crate::Acl;
use crate::CreateMode;
use crate::WatchedEventType::NodeDeleted;
use crate::ZooKeeper;

use tokio::sync::watch;

use backon::ExponentialBuilder;
use backon::Retryable;

/// Participate in leader election through this struct
#[derive(Debug)]
pub struct LeaderElection {
    /// ZNode under which volunteers are registered
    election_node: String,
    zk: ZooKeeper,
    backon_builder: ExponentialBuilder,
}

/// Represents the current state this node is in
#[derive(Debug, Clone, Copy)]
pub enum LeadershipState {
    /// This node is a follower
    Follower,
    /// This node is the leader
    Leader,
    /// Leadership participation procedure has not yet started
    Uninitialized,
    /// An error has occured in the leader election procedure
    Error(&'static str),
}

#[derive(Debug)]
enum ObserveError {
    Canceled,
    ZkError(String),
    SendError(String),
    LeaderNodeMissing,
    LeaderNodeChanged,
    FollowerNodeUnexpectedChange(String),
    NoParticipants,
}

impl From<tokio::sync::watch::error::SendError<LeadershipState>> for ObserveError {
    fn from(value: tokio::sync::watch::error::SendError<LeadershipState>) -> Self {
        ObserveError::SendError(value.to_string())
    }
}

impl From<crate::Error> for ObserveError {
    fn from(value: crate::Error) -> Self {
        ObserveError::ZkError(value.to_string())
    }
}

impl From<Canceled> for ObserveError {
    fn from(_value: Canceled) -> Self {
        ObserveError::Canceled
    }
}

#[derive(Debug)]
struct Candidate {
    zk: ZooKeeper,
    path: String,
    election_node: String,
}

impl Candidate {
    fn new(path: String, election_node: String, zk: ZooKeeper) -> Self {
        Self {
            zk,
            path,
            election_node,
        }
    }

    async fn observe(self, leader_sender: tokio::sync::watch::Sender<LeadershipState>) {
        loop {
            match self.observe_once(&leader_sender).await {
                Ok(_) => continue,
                Err(e) => match e {
                    ObserveError::Canceled => {
                        _ = leader_sender.send(LeadershipState::Error(
                            "Watch receiver canceled, server disconnected?",
                        ));
                        return;
                    }
                    ObserveError::ZkError(e) => {
                        error!("ZooKeeper error: {e}");
                        _ = leader_sender.send(LeadershipState::Error("ZooKeeper error"));
                        return;
                    }
                    ObserveError::SendError(e) => {
                        error!("Unable to send leadership state update: {e}");
                        return;
                    }
                    ObserveError::LeaderNodeMissing => {
                        _ = leader_sender
                            .send(LeadershipState::Error("Leader's znode was missing"));
                        return;
                    }
                    ObserveError::LeaderNodeChanged => {
                        _ = leader_sender
                            .send(LeadershipState::Error("Leader's znode has changed"));
                        return;
                    }
                    ObserveError::NoParticipants => {
                        _ = leader_sender
                            .send(LeadershipState::Error("No leader election participants"));
                        return;
                    }
                    ObserveError::FollowerNodeUnexpectedChange(e) => {
                        error!("Unexpected change to follower ephemeral node: {e}");
                        _ = leader_sender.send(LeadershipState::Error(
                            "Unexpected change to follower ephemeral node",
                        ));
                        return;
                    }
                },
            }
        }
    }

    async fn observe_once(
        &self,
        leader_sender: &tokio::sync::watch::Sender<LeadershipState>,
    ) -> Result<(), ObserveError> {
        let mut children: Vec<ElectionChild> = get_children(&self.zk, &self.election_node).await?;
        children.sort_unstable();

        trace!(participants = ?children, "got leader election participants");

        match children
            .iter()
            .position(|node| self.get_full_path(node) == *self.path)
        {
            Some(0) => {
                self.observe_leader(&leader_sender).await?;
            }
            Some(index) => {
                self.observe_follower(&children[index - 1], &leader_sender)
                    .await?;
            }
            None => unimplemented!("can't find myself"), // TODO TOCTOU, try again? forever?
        };

        Ok(())
    }

    async fn observe_leader(
        &self,
        leader_sender: &tokio::sync::watch::Sender<LeadershipState>,
    ) -> Result<(), ObserveError> {
        // start watching my own ephemeral node
        let (rx, stat) = self.zk.with_watcher().exists(&self.path).await?;
        if stat.is_none() {
            return Err(ObserveError::LeaderNodeMissing);
        }

        info!("i am the leader");
        leader_sender.send(LeadershipState::Leader)?;

        let event = rx.await?;
        error!(?event, "leader's ephemeral node changed");
        Err(ObserveError::LeaderNodeChanged)
    }

    async fn observe_follower(
        &self,
        preceding_node: &ElectionChild,
        leader_sender: &tokio::sync::watch::Sender<LeadershipState>,
    ) -> Result<(), ObserveError> {
        let path = self.get_full_path(preceding_node);
        debug!(?path, "setting the watch for the preceding node");

        let (rx, stat) = self.zk.with_watcher().exists(&path).await?;
        if stat.is_none() {
            debug!("preceding node already gone, re-evaluating");
            return Ok(());
        }

        leader_sender.send(LeadershipState::Follower)?;

        let event = rx.await?;
        match event.event_type {
            NodeDeleted => {
                debug!(?event, "the preceding node was removed");
                Ok(())
            }
            _ => Err(ObserveError::FollowerNodeUnexpectedChange(format!(
                "{:?}",
                event.event_type
            ))),
        }
    }

    fn get_full_path(&self, election_child: &ElectionChild) -> String {
        format!("{}/{}", self.election_node, election_child.0)
    }
}

impl LeaderElection {
    /// Create a new leader election struct
    pub fn new(zk: ZooKeeper, election_node: &str) -> Self {
        let backon_builder = ExponentialBuilder::default()
            .with_jitter()
            .with_min_delay(core::time::Duration::from_millis(100))
            .with_max_delay(core::time::Duration::from_millis(5000))
            .with_max_times(5);

        Self {
            election_node: election_node.to_string(),
            zk,
            backon_builder,
        }
    }
    /// Participate in [leader election](https://zookeeper.apache.org/doc/current/recipes.html#sc_leaderElection)
    ///
    /// # Returns
    ///
    /// - A [tokio::sync::watch::Receiver] that resolves once this node becomes a
    /// leader. To stop participating, drop the underlying ZooKeeper connection,
    /// so that the underlying ephemeral znodes are removed.
    /// - A [tokio::runtime::task::join::JoinHandle]. Call .abort() to stop
    /// participating in leader election. If a connection to ZooKeeper is  kept
    /// alive after this call, the ephemeral nodes are not removed making it it
    /// seem like you're still participating
    ///
    /// Upon receiving from this receiver applications may consider creating a
    /// separate znode to acknowledge that the leader has executed the leader
    /// procedure.
    pub async fn volunteer(
        self,
    ) -> Result<
        (
            tokio::sync::watch::Receiver<LeadershipState>,
            JoinHandle<()>,
        ),
        Whatever,
    > {
        info!("volunteering for leader election");

        // TODO error handling with guids:
        // https://zookeeper.apache.org/doc/current/recipes.html#sc_recipes_GuidNote
        // "If a recoverable error occurs calling create() the client should
        // call getChildren() and check for a node containing the guid used in the path
        // name. This handles the case (noted above) of the create() succeeding on the
        // server but the server crashing before returning the name of the new node."
        let path = match self
            .zk
            .create(
                &format!("{}/guid-n_", self.election_node),
                &b""[..],
                Acl::open_unsafe(), // todo
                CreateMode::EphemeralSequential,
            )
            .await
        {
            Ok(create_res) => {
                // if this is an err, it is "unrecoverable": no parent node, etc.
                create_res.whatever_context("can't create ephemeral node, unrecoverable error")?
            }
            Err(e) => {
                warn!("can't get children: {e}, recoverable error, will now retry...");
                (|| async {
                    get_children(&self.zk, &self.election_node)
                        .await
                        .map_err(|e| format!("{e:?}"))
                        .whatever_context("retry failed")
                })
                .retry(self.backon_builder)
                .await?
                .into_iter()
                .find(|child| child.0.starts_with("guid-n_"))
                .map(|child| child.0)
                .whatever_context("can't find my guid")?
            }
        };

        let (leader_sender, leader_receiver) = watch::channel(LeadershipState::Uninitialized);
        let candidate = Candidate::new(path.clone(), self.election_node, self.zk);

        let jh = tokio::spawn(
            candidate
                .observe(leader_sender)
                .instrument(trace_span!("election_observer", my_path = %path)),
        );

        Ok((leader_receiver, jh))
    }
}

#[derive(PartialEq, Eq, Debug, Ord)]
struct ElectionChild(String);

impl TryFrom<String> for ElectionChild {
    type Error = (); // TODO
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(Self(value)) // TODO error when there's no guid, etc.
    }
}

impl PartialOrd for ElectionChild {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0) // TODO change when guids
    }
}

async fn get_children(
    zk: &ZooKeeper,
    election_node: &str,
) -> Result<Vec<ElectionChild>, ObserveError> {
    Ok(zk
        .get_children(election_node)
        .await?
        .ok_or(ObserveError::NoParticipants)?
        .into_iter()
        .map(|s| ElectionChild::try_from(s).unwrap())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ZooKeeperBuilder;

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
        let leader_election1 = LeaderElection::new(zk1, "/election");
        let (mut rx1, jh1) = leader_election1.volunteer().await.unwrap();
        assert!(
            wait_for_leadership(&mut rx1).await,
            "testing that the first participant becomes the leader"
        );

        let (zk2, _w) = builder.connect(&connect_addr).await.unwrap();
        let leader_election2 = LeaderElection::new(zk2, "/election");
        let (mut rx2, _jh2) = leader_election2.volunteer().await.unwrap();

        assert_eq!(
            wait_for_follower(&mut rx2).await,
            true,
            "testing that the second participant is not the leader"
        );

        jh1.abort();

        assert_eq!(
            wait_for_leadership(&mut rx2).await,
            true,
            "testing that the second participant now becomes the leader"
        );
    }

    async fn wait_for_leadership(rx: &mut tokio::sync::watch::Receiver<LeadershipState>) -> bool {
        println!("waiting for leadership");
        loop {
            let state = *rx.borrow_and_update();
            match state {
                LeadershipState::Leader => {
                    return true;
                }
                LeadershipState::Uninitialized | LeadershipState::Follower => {
                    rx.changed().await.unwrap()
                }
                _ => panic!("wait for leadership error {state:?}"),
            }
        }
    }

    async fn wait_for_follower(rx: &mut tokio::sync::watch::Receiver<LeadershipState>) -> bool {
        println!("waiting for follower");
        loop {
            let state = *rx.borrow_and_update();
            match state {
                LeadershipState::Leader | LeadershipState::Uninitialized => {
                    rx.changed().await.unwrap()
                }
                LeadershipState::Follower => {
                    return true;
                }
                _ => panic!("wait for follower error {state:?}"),
            }
        }
    }
}
