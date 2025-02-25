use std::sync::Arc;

use futures::prelude::*;
use smallvec::SmallVec;
use tokio_postgres::SimpleQueryMessage;
use tracing::trace;

use crate::codec::EncodeError;
use crate::error::Error;
use crate::message::{BackendMessage, CommandCompleteTag, PsqlSrvRow, TransferFormat};

/// An encapsulation of a complete response produced by a Postgresql backend in response to a
/// request. The response will be sent to the frontend as a sequence of zero or more
/// `BackendMessage`s.
#[derive(Debug)]
#[warn(variant_size_differences)]
pub enum Response<S> {
    Empty,
    Message(BackendMessage),
    /// Send multiple messages at once
    Messages(SmallVec<[BackendMessage; 2]>),

    /// `Stream` is the most complex variant, containing data rows to be sent to the frontend in
    /// response to a one or more queries.
    Stream {
        header: Option<BackendMessage>,
        resultset: S,
        result_transfer_formats: Option<Arc<Vec<TransferFormat>>>,
        trailer: Option<BackendMessage>,
    },
}

impl<S> Response<S>
where
    S: Stream<Item = Result<PsqlSrvRow, Error>> + Unpin,
{
    pub async fn write<K>(self, sink: &mut K) -> Result<(), EncodeError>
    where
        K: Sink<BackendMessage, Error = EncodeError> + Unpin,
    {
        use Response::*;
        match self {
            Empty => Ok(()),

            Message(m) => {
                trace!("Sending message: {:?}", m);
                sink.feed(m).await
            }

            Messages(ms) => {
                trace!("Sending messages");
                for m in ms {
                    trace!("Sending message: {:?}", m);
                    sink.feed(m).await?
                }
                Ok(())
            }

            Stream {
                header,
                mut resultset,
                result_transfer_formats,
                trailer,
            } => {
                if let Some(header) = header {
                    trace!("Sending header: {:?}", header);
                    sink.feed(header).await?;
                }

                // For multi-selects, the responses will have the CommandComplete messages with the
                // number of associated rows in the stream itself. Otherwise, we keep track of
                // them and send our own command complete message for our single result set.
                let mut n_rows = 0;
                let mut sent_response = false;
                // We send a row description for each batch of rows, then the rows themselves, then
                // a command complete
                let mut sent_row_description = false;
                while let Some(r) = resultset.next().await {
                    match r {
                        Ok(PsqlSrvRow::ValueVec(row)) => {
                            trace!("Sending row: {:?}", row);
                            sink.feed(BackendMessage::DataRow {
                                values: row,
                                explicit_transfer_formats: result_transfer_formats.clone(),
                            })
                            .await?;
                            n_rows += 1;
                        }
                        Ok(PsqlSrvRow::RawRow(row)) => {
                            trace!("Sending raw row: {:?}", row);
                            sink.feed(BackendMessage::PassThroughDataRow(row)).await?;
                            n_rows += 1;
                        }
                        Err(e) => {
                            trace!("Sending error: {:?}", e);
                            sink.feed(e.into()).await?;
                        }
                        Ok(PsqlSrvRow::SimpleQueryMessage(m)) => {
                            trace!("Sending simple query message: {:?}", m);
                            debug_assert_eq!(n_rows, 0, "should not see a mix of simple query messages and rows that we count manually");

                            match m {
                                SimpleQueryMessage::Row(r) => {
                                    if !sent_row_description {
                                        sent_row_description = true;
                                        trace!("Sending row description: {:?}", r.fields());
                                        sink.feed(BackendMessage::PassThroughRowDescription(
                                            r.fields().to_vec(),
                                        ))
                                        .await?;
                                    }
                                    trace!("Sending pass-through row: {:?}", r);
                                    sink.feed(BackendMessage::PassThroughSimpleRow(r)).await?;
                                }
                                SimpleQueryMessage::CommandComplete(c) => {
                                    if c.rows == 0 && c.fields.is_none() && c.tag.is_empty() {
                                        // XXX JCD tokio_postgres does not provide the expected
                                        // EmptyQueryResponse message when processing empty
                                        // queries and instead gives a CommandComplete message with
                                        // no rows, no fields, and an empty tag.  I believe
                                        // PostgreSQL always supplies a tag for a valid
                                        // CommandComplete message.  Now that we've detected an
                                        // empty query response, send the appropriate message to
                                        // our client.
                                        sink.feed(BackendMessage::EmptyQueryResponse).await?;
                                        sent_response = true;
                                        continue;
                                    }
                                    if let Some(fields) = &c.fields {
                                        sink.feed(BackendMessage::PassThroughRowDescription(
                                            fields.to_vec(),
                                        ))
                                        .await?;
                                    }

                                    trace!("Sending pass-through command complete: {:?}", c);
                                    sink.feed(BackendMessage::PassThroughCommandComplete(c.tag))
                                        .await?;

                                    // We may have sent a row description, but it was for this
                                    // batch, so reset it for the next batch
                                    sent_row_description = false;
                                    sent_response = true;
                                }
                                _ => {
                                    unimplemented!("Unhandled variant of SimpleQueryMessage added")
                                }
                            }
                        }
                    }
                }

                if !sent_response {
                    trace!("Sending command complete: {:?}", n_rows);
                    sink.feed(BackendMessage::CommandComplete {
                        tag: CommandCompleteTag::Select(n_rows),
                    })
                    .await?;
                }

                if let Some(trailer) = trailer {
                    trace!("Sending trailer: {:?}", trailer);
                    sink.feed(trailer).await?;
                }

                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use smallvec::smallvec;
    use tokio_postgres::CommandCompleteContents;
    use tokio_test::block_on;

    use super::*;
    use crate::message::TransactionState;
    use crate::value::PsqlValue;

    type TestResponse = Response<stream::Iter<vec::IntoIter<Result<PsqlSrvRow, Error>>>>;

    #[test]
    fn write_empty() {
        let response = TestResponse::Empty;
        let validating_sink = sink::unfold(0, |_i, _m: BackendMessage| {
            async move {
                // No messages are expected.
                panic!();
            }
        });
        futures::pin_mut!(validating_sink);
        block_on(response.write(&mut validating_sink)).unwrap();
        block_on(validating_sink.flush()).unwrap();
    }

    #[test]
    fn write_message() {
        let response = TestResponse::Empty;
        let validating_sink = sink::unfold(0, |i, m: BackendMessage| {
            async move {
                match i {
                    0 => assert!(matches!(m, BackendMessage::BindComplete)),
                    // No further messages are expected.
                    _ => panic!(),
                }
                Ok::<_, EncodeError>(i + 1)
            }
        });
        futures::pin_mut!(validating_sink);
        block_on(response.write(&mut validating_sink)).unwrap();
        block_on(validating_sink.flush()).unwrap();
    }

    #[test]
    fn write_message2() {
        let response = TestResponse::Messages(smallvec![
            BackendMessage::BindComplete,
            BackendMessage::CloseComplete,
        ]);
        let validating_sink = sink::unfold(0, |i, m: BackendMessage| {
            async move {
                match i {
                    0 => assert!(matches!(m, BackendMessage::BindComplete)),
                    1 => assert!(matches!(m, BackendMessage::CloseComplete)),
                    // No further messages are expected.
                    _ => panic!(),
                }
                Ok::<_, EncodeError>(i + 1)
            }
        });
        futures::pin_mut!(validating_sink);
        block_on(response.write(&mut validating_sink)).unwrap();
        block_on(validating_sink.flush()).unwrap();
    }

    #[test]
    fn write_empty_query() {
        let response = TestResponse::Stream {
            header: None,
            resultset: stream::iter(vec![Ok(PsqlSrvRow::SimpleQueryMessage(
                SimpleQueryMessage::CommandComplete(CommandCompleteContents {
                    fields: None,
                    rows: 0,
                    tag: "".into(),
                }),
            ))]),
            result_transfer_formats: None,
            trailer: None,
        };
        let validating_sink = sink::unfold(0, |i, m: BackendMessage| {
            async move {
                match i {
                    0 => assert!(matches!(m, BackendMessage::EmptyQueryResponse)),
                    // No further messages are expected.
                    _ => panic!(),
                }
                Ok::<_, EncodeError>(i + 1)
            }
        });
        futures::pin_mut!(validating_sink);
        block_on(response.write(&mut validating_sink)).unwrap();
        block_on(validating_sink.flush()).unwrap();
    }

    #[test]
    fn write_select() {
        let response = Response::Stream {
            header: Some(BackendMessage::RowDescription {
                field_descriptions: vec![],
            }),
            resultset: stream::iter(vec![
                Ok(vec![PsqlValue::Int(5), PsqlValue::Double(0.123)].into()),
                Ok(vec![PsqlValue::Int(99), PsqlValue::Double(0.456)].into()),
            ]),
            result_transfer_formats: Some(Arc::new(vec![
                TransferFormat::Text,
                TransferFormat::Binary,
            ])),
            trailer: Some(BackendMessage::ready_for_query(
                TransactionState::InTransactionOk,
            )),
        };
        let validating_sink = sink::unfold(0, |i, m: BackendMessage| {
            async move {
                match i {
                    0 => assert!(matches!(
                        m,
                        BackendMessage::RowDescription {
                            field_descriptions
                        } if field_descriptions == vec![]
                    )),
                    1 => match m {
                        BackendMessage::DataRow {
                            values,
                            explicit_transfer_formats,
                        } => {
                            assert_eq!(values, vec![PsqlValue::Int(5), PsqlValue::Double(0.123)]);
                            assert_eq!(
                                explicit_transfer_formats,
                                Some(Arc::new(vec![TransferFormat::Text, TransferFormat::Binary]))
                            );
                        }
                        _ => panic!("Unexpected message {:?}", m),
                    },
                    2 => match m {
                        BackendMessage::DataRow {
                            values,
                            explicit_transfer_formats,
                        } => {
                            assert_eq!(values, vec![PsqlValue::Int(99), PsqlValue::Double(0.456)]);
                            assert_eq!(
                                explicit_transfer_formats,
                                Some(Arc::new(vec![TransferFormat::Text, TransferFormat::Binary]))
                            );
                        }
                        _ => panic!("Unexpected message {:?}", m),
                    },
                    3 => assert!(matches!(
                        m,
                        BackendMessage::CommandComplete {
                            tag
                        } if tag == CommandCompleteTag::Select(2)
                    )),
                    4 => match (
                        m,
                        BackendMessage::ready_for_query(TransactionState::InTransactionOk),
                    ) {
                        (
                            BackendMessage::ReadyForQuery { status },
                            BackendMessage::ReadyForQuery {
                                status: expected_status,
                            },
                        ) => assert_eq!(status, expected_status),
                        _ => panic!(),
                    },
                    // No further messages are expected.
                    _ => panic!(),
                }
                Ok::<_, EncodeError>(i + 1)
            }
        });
        futures::pin_mut!(validating_sink);
        block_on(response.write(&mut validating_sink)).unwrap();
        block_on(validating_sink.flush()).unwrap();
    }
}
