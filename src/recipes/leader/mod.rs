use snafu::{OptionExt, ResultExt, Whatever, whatever};
use tokio::task::AbortHandle;
use tracing::{Instrument, error, warn};
use tracing::{debug, info, trace_span};
use uuid::Uuid;

use crate::Acl;
use crate::CreateMode;
use crate::WatchedEventType::NodeDeleted;
use crate::ZooKeeper;

use tokio::sync::watch;

use backon::ExponentialBuilder;
use backon::Retryable;

static CANT_SEND_ERROR_MSG: &str = "Can't send leadership state update";
static RX_CANCELLED_ERROR_MSG: &str = "Watch receiver canceled, server disconnected?";
static ZNODE_NOT_FOUND_ERROR_MSG: &str = "The ephemeral znode of the participant was not found";

/// Participate in leader election through this struct
#[derive(Debug)]
pub struct LeaderElection {
    /// ZNode under which volunteers are registered
    election_prefix: String,
    zk: ZooKeeper,
    backon_builder: ExponentialBuilder,
    acl: &'static [Acl],
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
    Error,
}

#[derive(Debug)]
struct Candidate {
    zk: ZooKeeper,
    node: ElectionChild,
    backon_builder: ExponentialBuilder,
    // election_base_path: String,
}

impl Candidate {
    fn new(node: ElectionChild, zk: ZooKeeper, backon_builder: ExponentialBuilder) -> Self {
        Self {
            zk,
            node,
            backon_builder,
            // election_base_path: election_base_path.to_string(),
        }
    }

    async fn observe(self, leader_sender: watch::Sender<LeadershipState>) {
        loop {
            match (|| async { self.observe_once(&leader_sender).await })
                .retry(self.backon_builder)
                .notify(|err, dur| {
                    warn!("retrying {:?} after {:?}", err, dur);
                })
                .when(|e| e.to_string() == ZNODE_NOT_FOUND_ERROR_MSG)
                .await
            {
                // match self.observe_once(&leader_sender).await {
                Ok(_) => continue,
                Err(e) => {
                    error!("{e}");
                    _ = leader_sender.send(LeadershipState::Error);
                    return;
                }
            }
        }
    }

    async fn observe_once(
        &self,
        leader_sender: &watch::Sender<LeadershipState>,
    ) -> Result<(), crate::Error> {
        let mut children: Vec<ElectionChild> =
            get_children(&self.zk, &self.node.election_prefix).await?;
        children.sort_unstable();

        debug!(participants = ?children, "got leader election participants");

        match children.iter().position(|node| node == &self.node) {
            Some(0) => {
                self.observe_leader(leader_sender).await?;
            }
            Some(index) => {
                self.observe_follower(&children[index - 1], leader_sender)
                    .await?;
            }
            None => whatever!("{}", ZNODE_NOT_FOUND_ERROR_MSG),
        };

        Ok(())
    }

    async fn observe_leader(
        &self,
        leader_sender: &watch::Sender<LeadershipState>,
    ) -> Result<(), crate::Error> {
        // start watching my own ephemeral node
        let (rx, stat) = self
            .zk
            .with_watcher()
            .exists(&self.node.full_path())
            .await?;
        if stat.is_none() {
            whatever!("Leader's znode was missing");
        }

        info!("i am the leader");
        leader_sender
            .send(LeadershipState::Leader)
            .whatever_context(CANT_SEND_ERROR_MSG)?;

        let event = rx.await.whatever_context(RX_CANCELLED_ERROR_MSG)?;

        error!(?event, "leader's ephemeral node changed");
        whatever!(
            "Unexpected change to the leader's node: {:?}",
            event.event_type
        );
    }

    async fn observe_follower(
        &self,
        preceding_node: &ElectionChild,
        leader_sender: &watch::Sender<LeadershipState>,
    ) -> Result<(), crate::Error> {
        let path = preceding_node.full_path();
        debug!(?path, "setting the watch for the preceding node");

        let (rx, stat) = self.zk.with_watcher().exists(&path).await?;
        if stat.is_none() {
            debug!("preceding node already gone, re-evaluating");
            return Ok(());
        }

        leader_sender
            .send(LeadershipState::Follower)
            .whatever_context(CANT_SEND_ERROR_MSG)?;

        let event = rx.await.whatever_context(RX_CANCELLED_ERROR_MSG)?;
        match event.event_type {
            NodeDeleted => {
                debug!(?event, "the preceding node was removed");
                Ok(())
            }
            _ => whatever!(
                "Unexpected change to the follower's node: {:?}",
                event.event_type
            ),
        }
    }
}

impl LeaderElection {
    /// Create a new leader election struct
    pub fn new(zk: ZooKeeper, election_node: &str, acl: &'static [Acl]) -> Self {
        let backon_builder = ExponentialBuilder::default()
            .with_jitter()
            .with_min_delay(core::time::Duration::from_millis(100))
            .with_max_delay(core::time::Duration::from_millis(5000))
            .with_max_times(5);

        Self {
            election_prefix: election_node.to_string(),
            zk,
            backon_builder,
            acl,
        }
    }
    /// Participate in [leader election](https://zookeeper.apache.org/doc/current/recipes.html#sc_leaderElection)
    ///
    /// # Returns
    ///
    /// - A [tokio::sync::watch::Receiver] that resolves once this node becomes a
    ///   leader. To stop participating, drop the underlying ZooKeeper connection,
    ///   so that the underlying ephemeral znodes are removed.
    /// - A [tokio::runtime::task::join::JoinHandle]. Call .abort() to stop
    ///   participating in leader election. If a connection to ZooKeeper is  kept
    ///   alive after this call, the ephemeral nodes are not removed making it it
    ///   seem like you're still participating
    ///
    /// Upon receiving from this receiver applications may consider creating a
    /// separate znode to acknowledge that the leader has executed the leader
    /// procedure.
    pub async fn volunteer(
        self,
    ) -> Result<(watch::Receiver<LeadershipState>, AbortHandle), Whatever> {
        info!("volunteering for leader election");

        // Error handling with guids:
        // https://zookeeper.apache.org/doc/current/recipes.html#sc_recipes_GuidNote
        // "If a recoverable error occurs calling create() the client should
        // call getChildren() and check for a node containing the guid used in the path
        // name. This handles the case [...] of the create() succeeding on the
        // server but the server crashing before returning the name of the new node."
        let guid = Uuid::new_v4();
        let path = match self
            .zk
            .create(
                &format!("{}/{}-n_", self.election_prefix, guid),
                &b""[..],
                self.acl,
                CreateMode::EphemeralSequential,
            )
            .await
        {
            Ok(create_res) => {
                // if this is an err, it is "unrecoverable": no parent node, node already exists, etc.
                create_res.whatever_context("can't create ephemeral node, unrecoverable error")?
            }
            Err(e) => {
                warn!(
                    "can't get children: {e}, recoverable error, will now try to find my guid again..."
                );
                (|| async {
                    get_children(&self.zk, &self.election_prefix)
                        .await
                        .whatever_context("can't get leader election nodes, retry failed")
                })
                .retry(self.backon_builder)
                .notify(|err, dur| {
                    warn!("retrying {:?} after {:?}", err, dur);
                })
                .await?
                .into_iter()
                .find(|child| child.guid == guid)
                .map(|child| child.full_path())
                .whatever_context("can't find a znode with my guid")?
            }
        };

        let node = ElectionChild::try_from_full_path(&path, &self.election_prefix)
            .whatever_context("wrong format of the election child node")?;

        let (leader_sender, leader_receiver) = watch::channel(LeadershipState::Uninitialized);
        let candidate = Candidate::new(node, self.zk, self.backon_builder);

        let jh = tokio::spawn(
            candidate
                .observe(leader_sender)
                .instrument(trace_span!("election_observer", my_path = %path)),
        );

        Ok((leader_receiver, jh.abort_handle()))
    }
}

#[derive(PartialEq, Eq, Debug)]
struct ElectionChild {
    election_prefix: String,
    path: String,
    guid: Uuid,
    seq: u32,
}

impl ElectionChild {
    fn try_from_full_path(full_path: &str, election_prefix: &str) -> Result<Self, crate::Error> {
        if !full_path.starts_with(election_prefix) {
            whatever!(
                "wrong format of a child node; must start with election prefix {election_prefix}"
            );
        }

        let Some(path) = full_path.strip_prefix(&format!("{}/", election_prefix)) else {
            whatever!("wrong format of a child node; must be `/prefix/path`");
        };

        let path_parts = path.split("-n_").collect::<Vec<&str>>();
        if path_parts.len() != 2 {
            whatever!("wrong format of a child node's path; must `<guid>-n_<number>`");
        }
        let guid = Uuid::parse_str(path_parts[0]).whatever_context("Can't parse node's guid")?;
        let seq = path_parts[1]
            .parse::<u32>()
            .whatever_context("cant parse node's sequential number as u32 from {full_path}: {e}")?;

        Ok(Self {
            election_prefix: election_prefix.to_string(),
            path: path.to_string(),
            guid,
            seq,
        })
    }

    fn try_from_parts(prefix: &str, path: &str) -> Result<Self, crate::Error> {
        let path_parts = path.split("-n_").collect::<Vec<&str>>();
        if path_parts.len() != 2 {
            whatever!("wrong format of a child node's path; must `<guid>-n_<number>`");
        }
        let guid = Uuid::parse_str(path_parts[0]).whatever_context("Can't parse node's guid")?;
        let seq = path_parts[1].parse::<u32>().whatever_context(format!(
            "cant parse node's sequential number as u32 from prefix `{prefix}` and path `{path}`"
        ))?;

        Ok(Self {
            election_prefix: prefix.to_string(),
            path: path.to_string(),
            guid,
            seq,
        })
    }

    fn full_path(&self) -> String {
        format!("{}/{}", self.election_prefix, self.path)
    }
}

impl PartialOrd for ElectionChild {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ElectionChild {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.seq.cmp(&other.seq)
    }
}

async fn get_children(
    zk: &ZooKeeper,
    election_node: &str,
) -> Result<Vec<ElectionChild>, crate::Error> {
    Ok(zk
        .get_children(election_node)
        .await?
        .unwrap_or_default()
        .into_iter()
        .filter_map(|s| {
            ElectionChild::try_from_parts(election_node, &s)
                .inspect_err(|e| warn!("skipping malformed election child {s:?}: {e}"))
                .ok()
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ZooKeeperBuilder, error::Create};

    fn init_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
    }

    #[test]
    fn parse_path() {
        init_tracing_subscriber();
        let guid = Uuid::new_v4();
        let ok_path = format!("/election/{}-n_000123", guid);
        let c = ElectionChild::try_from_full_path(&ok_path, "/election").unwrap();
        assert_eq!(c.election_prefix, "/election");
        assert_eq!(c.path, format!("{}-n_000123", guid));
        assert_eq!(c.guid, guid);
        assert_eq!(c.full_path(), format!("/election/{}-n_000123", guid));
    }

    #[tokio::test]
    async fn election_works() {
        let builder = ZooKeeperBuilder::default();
        let connect_addr = "127.0.0.1:2181".parse().unwrap();

        init_tracing_subscriber();

        let (zk1, _w) = builder.connect(&connect_addr).await.unwrap();
        create_election_node(&zk1).await;
        let leader_election1 = LeaderElection::new(zk1, "/election", Acl::open_unsafe());
        let (mut rx1, jh1) = leader_election1.volunteer().await.unwrap();
        assert!(
            wait_for_leadership(&mut rx1).await,
            "testing that the first participant becomes the leader"
        );

        let (zk2, _w) = builder.connect(&connect_addr).await.unwrap();
        let leader_election2 = LeaderElection::new(zk2, "/election", Acl::open_unsafe());
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
        debug!("waiting for leadership");
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

    async fn create_election_node(zk: &ZooKeeper) {
        let res = zk
            .create(
                "/election",
                &b""[..],
                Acl::open_unsafe(),
                CreateMode::Persistent,
            )
            .await
            .unwrap();

        match res {
            Ok(_) => {}
            Err(e) if e == Create::NodeExists => {}
            Err(e) => panic!("{e}"),
        };
    }

    async fn wait_for_follower(rx: &mut tokio::sync::watch::Receiver<LeadershipState>) -> bool {
        debug!("waiting for follower");
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
