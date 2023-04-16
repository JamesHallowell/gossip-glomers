use crate::protocol::{Message, MessageId};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    JsonError(#[from] serde_json::Error),

    #[error(transparent)]
    ChannelError(#[from] tokio::sync::mpsc::error::SendError<Message>),

    #[error("no response for message {0:?}")]
    NoResponse(MessageId),
}
