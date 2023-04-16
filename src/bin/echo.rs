use gossip_glomers::{
    error::Error,
    node::{Node, NodeContext},
    protocol::{Echo, EchoOk},
    server::Server,
};

async fn echo(_: NodeContext, _: (), Echo { echo }: Echo) -> Result<EchoOk, Error> {
    Ok(EchoOk { echo })
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let node = Node::default().add_handler("echo", echo);

    Server::default().serve(node).await
}
