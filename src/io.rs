use {
    crate::protocol::Message,
    futures::stream::Stream,
    tokio::{
        io::{stdin, AsyncBufReadExt, BufReader},
        sync::mpsc,
    },
    tokio_stream::wrappers::ReceiverStream,
};

pub fn messages_from_std_in() -> impl Stream<Item = Message> {
    let (sender, receiver) = mpsc::channel(8);

    tokio::spawn(async move {
        let std_in = BufReader::new(stdin());
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
