use gossip_glomers::{
    error::Error,
    node::{Context, Node},
    protocol::{Echo, EchoOk},
    server::Server,
};

async fn echo(_: Context, Echo { echo }: Echo) -> Result<EchoOk, Error> {
    Ok(EchoOk { echo })
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let node = Node::default().add_route("echo", echo);

    Server::default().serve(node).await
}
