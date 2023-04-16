use {
    gossip_glomers::{
        error::Error,
        node::{Context, Node},
        protocol::{Generate, GenerateOk},
        server::Server,
    },
    uuid::Uuid,
};

async fn generate(_: Context, _: Generate) -> Result<GenerateOk, Error> {
    Ok(GenerateOk { id: Uuid::new_v4() })
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let node = Node::default().add_route("generate", generate);

    Server::default().serve(node).await
}
