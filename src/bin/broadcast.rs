use gossip_glomers::{
    error::Error,
    node::{Context, Node},
    protocol::{Broadcast, BroadcastOk, Read, ReadOk, Topology, TopologyOk},
    server::Server,
};

async fn broadcast(_: Context, Broadcast { message }: Broadcast) -> Result<BroadcastOk, Error> {
    Ok(BroadcastOk {})
}

async fn read(_: Context, _: Read) -> Result<ReadOk, Error> {
    Ok(ReadOk { messages: vec![] })
}

async fn topology(_: Context, _: Topology) -> Result<TopologyOk, Error> {
    Ok(TopologyOk {})
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let node = Node::default()
        .add_route("broadcast", broadcast)
        .add_route("read", read)
        .add_route("topology", topology);

    Server::default().serve(node).await
}
