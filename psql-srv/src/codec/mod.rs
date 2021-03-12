mod decoder;
mod encoder;
mod error;

pub use error::{DecodeError, EncodeError};

use crate::r#type::Type;
use crate::value::Value;
use std::collections::HashMap;
use std::marker::PhantomData;

/// A [`Decoder`] implementation that deserializes `FrontendMessage` and [`Encoder`] implementation
/// that serializes `BackendMessage`.
///
/// [`Decoder`]: https://docs.rs/tokio-util/0.2.0/tokio_util/codec/trait.Decoder.html
/// [`Encoder`]: https://docs.rs/tokio-util/0.2.0/tokio_util/codec/trait.Encoder.html
pub struct Codec<R> {
    is_starting_up: bool,
    statement_param_types: HashMap<String, Vec<Type>>,
    _unused: PhantomData<R>,
}

impl<R: IntoIterator<Item: Into<Value>>> Codec<R> {
    pub fn new() -> Codec<R> {
        Codec {
            is_starting_up: true,
            statement_param_types: HashMap::new(),
            _unused: PhantomData,
        }
    }

    /// Set when the connection start up phase is complete. Indicates that regular mode messages
    /// will be parsed instead of startup messages.
    pub fn set_start_up_complete(&mut self) {
        self.is_starting_up = false;
    }

    /// Set the data types of a prepared statement's parameters. These data types must be set
    /// before the data values within a `FrontendMessage::Bind` message referencing the named
    /// pepared statement can be parsed.
    pub fn set_statement_param_types(&mut self, statement_name: String, types: Vec<Type>) {
        self.statement_param_types.insert(statement_name, types);
    }
}
