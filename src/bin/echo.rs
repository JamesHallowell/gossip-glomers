use {
    futures::{Stream, StreamExt},
    gossip_glomers::{
        error::Error,
        node::Node,
        protocol::{Message, MessageBody},
    },
    std::pin::pin,
    tokio::{
        io::{self, AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader},
        sync::{mpsc, Mutex},
    },
    tokio_stream::wrappers::ReceiverStream,
};

fn messages_from_std_in(std_in: io::Stdin) -> impl Stream<Item = Message> {
    let (sender, receiver) = mpsc::channel(16);
    tokio::spawn(async move {
        let std_in = BufReader::new(std_in);
        let mut lines = std_in.lines();

        while let Ok(Some(line)) = lines.next_line().await {
            match serde_json::from_str(&line) {
                Ok(message) => {
                    if sender.send(message).await.is_err() {
                        break;
                    }
                }
                Err(err) => eprintln!("error parsing message: {err}"),
            }
        }
    });
    ReceiverStream::new(receiver)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let mut messages = messages_from_std_in(io::stdin());

    let node = if let Some(Message {
        src,
        dest,
        body:
            MessageBody::Init {
                msg_id,
                node_id,
                node_ids,
            },
    }) = messages.next().await
    {
        Message {
            src: dest,
            dest: src,
            body: MessageBody::InitOk {
                in_reply_to: msg_id,
            },
        }
        .send()
        .await?;

        Node::new(node_id, node_ids)
    } else {
        panic!("didn't receive a node init message");
    };

    node.run(messages).await
}
