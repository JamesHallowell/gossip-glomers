use {
    crate::{
        error::Error,
        io::messages_from_std_in,
        node::{Node, NodeContext},
        protocol::{Init, InitOk},
    },
    futures::{stream::FuturesUnordered, stream::StreamExt},
};

#[derive(Default)]
pub struct Server;

impl Server {
    pub async fn serve<State>(self, node: Node<State>) -> Result<(), Error>
    where
        State: Clone + 'static,
    {
        let (state, router) = node.into_parts();

        let mut incoming_messages = messages_from_std_in();

        let message = incoming_messages
            .next()
            .await
            .expect("no messages received");

        let init: Init =
            serde_json::from_value(message.body.clone()).expect("didn't receive an init message");

        let context = NodeContext::new(init.node_id);
        context.reply(message, InitOk {}).await?;

        let mut futures = FuturesUnordered::new();
        loop {
            tokio::select! {
                message = incoming_messages.next() => {
                    match message {
                        Some(message) => {
                            if let Ok(future) = router.handle(&context, &state, message) {
                                futures.push(future);
                            }
                        }
                        None => break,
                    };

                },
                result = futures.next(), if ! futures.is_empty() => {
                    match result {
                        Some(Ok(_)) => {},
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
