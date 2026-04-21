use crate::Acl;
use crate::CreateMode;
use crate::ZooKeeper;

/// Participate in leader election through this struct
#[derive(Debug, Copy, Clone)]
pub struct LeaderElection {
    /// ZNode under which volunteers are registered, /election by default
    election_node: &'static str,
}

impl Default for LeaderElection {
    fn default() -> Self {
        Self {
            election_node: "/election",
        }
    }
}

impl LeaderElection {
    /// Participate in leader election
    pub async fn volunteer(
        &self,
        zk: &ZooKeeper,
    ) -> Result<futures::channel::oneshot::Receiver<()>, ()> {
        let path = zk
            .create(
                &format!("{}/guid-n_", self.election_node), // todo guid
                &b""[..],
                Acl::open_unsafe(),
                CreateMode::EphemeralSequential,
            )
            .await
            .unwrap()
            .unwrap();

        let children: Vec<ElectionChild> = zk
            .get_children(self.election_node)
            .await
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|s| ElectionChild::try_from(s).unwrap())
            .collect();

        dbg!(&children);

        match children
            .iter()
            .position(|s| format!("{}/{}", self.election_node, s.0) == path)
        {
            Some(0) => {
                println!("i am the leader");
            }
            Some(index) => println!(
                "i am a follower need to set the watch for the previous node {:?}",
                children[index - 1]
            ),
            None => unimplemented!("can't find myself"),
        };

        let (_tx, rx) = futures::channel::oneshot::channel();
        Ok(rx)
    }
}

#[derive(PartialEq, Eq, Debug)]
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

    use futures::FutureExt;

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

        init_tracing_subscriber();

        let connect_addr = "127.0.0.1:2181".parse().unwrap();
        let (zk1, _w) = builder.connect(&connect_addr).await.unwrap();

        let (zk2, _w) = builder.connect(&connect_addr).await.unwrap();

        let leader_election = LeaderElection::default();

        let mut f1 = Box::pin(leader_election.volunteer(&zk1).fuse());
        let mut f2 = Box::pin(leader_election.volunteer(&zk2).fuse());
        // futures::select! {
        //     _ = f1 => println!("zk1 exited"),
        //     _ = f2 => println!("zk2 exited"),
        // }

        _ = f1.await;
        _ = f2.await;
    }
}
