use std::convert::TryInto;
use std::sync::Arc;

use futures::prelude::*;
use smallvec::SmallVec;

use crate::codec::EncodeError;
use crate::error::Error;
use crate::message::{BackendMessage, CommandCompleteTag, TransferFormat};
use crate::value::PsqlValue;

/// An encapsulation of a complete response produced by a Postgresql backend in response to a
/// request. The response will be sent to the frontend as a sequence of zero or more
/// `BackendMessage`s.
#[derive(Debug, Eq, PartialEq)]
#[warn(variant_size_differences)]
pub enum Response<R, S> {
    Empty,
    Message(BackendMessage<R>),
    /// Send multiple messages at once
    Messages(SmallVec<[BackendMessage<R>; 2]>),

    /// `Select` is the most complex variant, containing data rows to be sent to the frontend in
    /// response to a select query.
    Select {
        header: Option<BackendMessage<R>>,
        resultset: S,
        result_transfer_formats: Option<Arc<Vec<TransferFormat>>>,
        trailer: Option<BackendMessage<R>>,
    },
}

impl<R, S> Response<R, S>
where
    R: IntoIterator<Item: TryInto<PsqlValue, Error = Error>>,
    S: Stream<Item = Result<R, Error>> + Unpin,
{
    pub async fn write<K>(self, sink: &mut K) -> Result<(), EncodeError>
    where
        K: Sink<BackendMessage<R>, Error = EncodeError> + Unpin,
    {
        use Response::*;
        match self {
            Empty => Ok(()),

            Message(m) => sink.send(m).await,

            Messages(ms) => {
                let num_messages = ms.len();
                for (i, m) in ms.into_iter().enumerate() {
                    if i == num_messages - 1 {
                        sink.send(m).await?;
                    } else {
                        sink.feed(m).await?
                    }
                }
                Ok(())
            }

            Select {
                header,
                mut resultset,
                result_transfer_formats,
                trailer,
            } => {
                if let Some(header) = header {
                    sink.feed(header).await?;
                }

                let mut n_rows = 0;
                while let Some(r) = resultset.next().await {
                    match r {
                        Ok(row) => {
                            sink.feed(BackendMessage::DataRow {
                                values: row,
                                explicit_transfer_formats: result_transfer_formats.clone(),
                            })
                            .await?;
                            n_rows += 1;
                        }
                        Err(e) => {
                            sink.feed(e.into()).await?;
                        }
                    }
                }

                sink.feed(BackendMessage::CommandComplete {
                    tag: CommandCompleteTag::Select(n_rows),
                })
                .await?;

                if let Some(trailer) = trailer {
                    sink.feed(trailer).await?;
                }

                sink.flush().await
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::convert::TryFrom;
    use std::vec;

    use smallvec::smallvec;
    use tokio_test::block_on;

    use super::*;
    use crate::value::PsqlValue;

    #[derive(Clone, Debug, PartialEq)]
    struct Value(PsqlValue);

    impl TryFrom<Value> for PsqlValue {
        type Error = Error;

        fn try_from(v: Value) -> Result<Self, Self::Error> {
            Ok(v.0)
        }
    }

    type TestResponse =
        Response<Vec<Value>, stream::Iter<vec::IntoIter<Result<Vec<Value>, Error>>>>;

    #[test]
    fn write_empty() {
        let response = TestResponse::Empty;
        let validating_sink = sink::unfold(0, |_i, _m: BackendMessage<Vec<Value>>| {
            async move {
                // No messages are expected.
                panic!();
            }
        });
        futures::pin_mut!(validating_sink);
        block_on(response.write(&mut validating_sink)).unwrap();
    }

    #[test]
    fn write_message() {
        let response = TestResponse::Empty;
        let validating_sink = sink::unfold(0, |i, m: BackendMessage<Vec<Value>>| {
            async move {
                match i {
                    0 => assert_eq!(m, BackendMessage::BindComplete),
                    // No further messages are expected.
                    _ => panic!(),
                }
                Ok::<_, EncodeError>(i + 1)
            }
        });
        futures::pin_mut!(validating_sink);
        block_on(response.write(&mut validating_sink)).unwrap();
    }

    #[test]
    fn write_message2() {
        let response = TestResponse::Messages(smallvec![
            BackendMessage::BindComplete,
            BackendMessage::CloseComplete,
        ]);
        let validating_sink = sink::unfold(0, |i, m: BackendMessage<Vec<Value>>| {
            async move {
                match i {
                    0 => assert_eq!(m, BackendMessage::BindComplete),
                    1 => assert_eq!(m, BackendMessage::CloseComplete),
                    // No further messages are expected.
                    _ => panic!(),
                }
                Ok::<_, EncodeError>(i + 1)
            }
        });
        futures::pin_mut!(validating_sink);
        block_on(response.write(&mut validating_sink)).unwrap();
    }

    #[test]
    fn write_select_simple_empty() {
        let response = TestResponse::Select {
            header: None,
            resultset: stream::iter(vec![]),
            result_transfer_formats: None,
            trailer: None,
        };
        let validating_sink = sink::unfold(0, |i, m: BackendMessage<Vec<Value>>| {
            async move {
                match i {
                    0 => assert_eq!(
                        m,
                        BackendMessage::CommandComplete {
                            tag: CommandCompleteTag::Select(0)
                        }
                    ),
                    // No further messages are expected.
                    _ => panic!(),
                }
                Ok::<_, EncodeError>(i + 1)
            }
        });
        futures::pin_mut!(validating_sink);
        block_on(response.write(&mut validating_sink)).unwrap();
    }

    #[test]
    fn write_select() {
        let response = Response::Select {
            header: Some(BackendMessage::RowDescription {
                field_descriptions: vec![],
            }),
            resultset: stream::iter(vec![
                Ok(vec![
                    Value(PsqlValue::Int(5)),
                    Value(PsqlValue::Double(0.123)),
                ]),
                Ok(vec![
                    Value(PsqlValue::Int(99)),
                    Value(PsqlValue::Double(0.456)),
                ]),
            ]),
            result_transfer_formats: Some(Arc::new(vec![
                TransferFormat::Text,
                TransferFormat::Binary,
            ])),
            trailer: Some(BackendMessage::ready_for_query_idle()),
        };
        let validating_sink = sink::unfold(0, |i, m: BackendMessage<Vec<Value>>| {
            async move {
                match i {
                    0 => assert_eq!(
                        m,
                        BackendMessage::RowDescription {
                            field_descriptions: vec![]
                        }
                    ),
                    1 => assert_eq!(
                        m,
                        BackendMessage::DataRow {
                            values: vec![Value(PsqlValue::Int(5)), Value(PsqlValue::Double(0.123))],
                            explicit_transfer_formats: Some(Arc::new(vec![
                                TransferFormat::Text,
                                TransferFormat::Binary
                            ]))
                        }
                    ),
                    2 => assert_eq!(
                        m,
                        BackendMessage::DataRow {
                            values: vec![
                                Value(PsqlValue::Int(99)),
                                Value(PsqlValue::Double(0.456))
                            ],
                            explicit_transfer_formats: Some(Arc::new(vec![
                                TransferFormat::Text,
                                TransferFormat::Binary
                            ]))
                        }
                    ),
                    3 => assert_eq!(
                        m,
                        BackendMessage::CommandComplete {
                            tag: CommandCompleteTag::Select(2)
                        }
                    ),
                    4 => assert_eq!(m, BackendMessage::ready_for_query_idle()),
                    // No further messages are expected.
                    _ => panic!(),
                }
                Ok::<_, EncodeError>(i + 1)
            }
        });
        futures::pin_mut!(validating_sink);
        block_on(response.write(&mut validating_sink)).unwrap();
    }
}
