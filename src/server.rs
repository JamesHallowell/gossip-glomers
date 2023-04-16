use {
    crate::{
        error::Error,
        io::messages_from_std_in,
        node::{Context, Node},
        protocol::{Init, InitOk},
    },
    futures::{stream::FuturesUnordered, stream::StreamExt},
};

#[derive(Default)]
pub struct Server;

impl Server {
    pub async fn serve(self, node: Node) -> Result<(), Error> {
        let mut messages = messages_from_std_in();

        let message = messages.next().await.expect("no messages received");

        let init: Init =
            serde_json::from_value(message.body.clone()).expect("didn't receive an init message");

        let context = Context::new(init.node_id, init.node_ids);
        context.reply(message, InitOk {}).await?;

        let mut futures = FuturesUnordered::new();
        loop {
            tokio::select! {
                message = messages.next() => {
                    match message {
                        Some(message) => {
                            if let Ok(future) = node.handle(&context, message) {
                                futures.push(future);
                            }
                        }
                        None => break,
                    };

                },
                result = futures.next(), if ! futures.is_empty() => {
                    match result {
                        Some(Ok(())) => {},
                        Some(Err(error)) => {
                            eprintln!("error: {}", error);
                        }
                        None =>  {
                            eprintln!("finished all futures");
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
