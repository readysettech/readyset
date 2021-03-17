use crate::codec::{Codec, DecodeError, EncodeError};
use crate::message::{BackendMessage, FrontendMessage};
use crate::r#type::Type;
use crate::value::Value;
use futures::prelude::*;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

pub struct Channel<C, R>(Framed<C, Codec<R>>);

impl<C, R> Channel<C, R>
where
    C: AsyncRead + AsyncWrite + Unpin,
    R: IntoIterator<Item: Into<Value>>,
{
    pub fn new(inner: C) -> Channel<C, R> {
        let codec = Codec::new(); // TODO Updated version of `Framed` provides `fn with_capacity`.
        Channel(Framed::new(inner, codec))
    }

    pub fn set_start_up_complete(&mut self) {
        self.0.codec_mut().set_start_up_complete();
    }

    pub fn set_statement_param_types(&mut self, statement_name: &str, types: Vec<Type>) {
        self.0
            .codec_mut()
            .set_statement_param_types(statement_name, types);
    }

    pub fn clear_statement_param_types(&mut self, statement_name: &str) {
        self.0
            .codec_mut()
            .clear_statement_param_types(statement_name);
    }

    pub async fn next(&mut self) -> Option<Result<FrontendMessage, DecodeError>> {
        self.0.next().await
    }

    pub async fn feed(&mut self, item: BackendMessage<R>) -> Result<(), EncodeError> {
        self.0.feed(item).await
    }

    pub async fn flush(&mut self) -> Result<(), EncodeError> {
        self.0.flush().await
    }
}
