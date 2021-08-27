use std::collections::BTreeMap;
use std::fmt::Display;
use std::str::FromStr;

use anyhow::{bail, Error};
use chrono::{DateTime, FixedOffset, Utc};
use enum_display_derive::Display;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, Lines};

#[derive(Clone, Debug, Display, Eq, PartialEq)]
pub enum Command {
    Connect,
    Query,
    Prepare,
    Execute,
    CloseStmt,
    Quit,
}

impl FromStr for Command {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        match s {
            "Connect" => Ok(Self::Connect),
            "Query" => Ok(Self::Query),
            "Prepare" => Ok(Self::Prepare),
            "Execute" => Ok(Self::Execute),
            "Close stmt" => Ok(Self::CloseStmt),
            "Quit" => Ok(Self::Quit),
            _ => bail!("Unknown command '{}'", s),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub timestamp: DateTime<Utc>,
    pub id: u32,
    pub command: Command,
    pub arguments: String,
}

impl FromStr for Entry {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        let parts = s.splitn(3, '\t').collect::<Vec<&str>>();
        if parts.len() != 3 {
            bail!("Wrong number of tab-delimited parts: {}", parts.len());
        }
        let middle_parts = parts[1].trim().split(' ').collect::<Vec<&str>>();
        if middle_parts.len() != 2 {
            bail!("Wrong number of middle parts: {}", middle_parts.len());
        }
        Ok(Self {
            timestamp: DateTime::<FixedOffset>::parse_from_rfc3339(parts[0])?.into(),
            id: middle_parts[0].parse()?,
            command: Command::from_str(middle_parts[1])?,
            arguments: parts[2].to_string(),
        })
    }
}

#[derive(Debug)]
pub struct Session {
    pub entries: Vec<Entry>,
}

impl Session {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    fn query_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|e| match e.command {
                Command::Query => true,
                // TODO:  true
                Command::Execute => false,
                _ => false,
            })
            .count()
    }
}

pub struct Stream<R: AsyncBufRead + Unpin> {
    // Include this to emulate the functionality of .enumerate()
    current_idx: usize,
    inner: Lines<R>,
    partial_sessions: BTreeMap<u32, Session>,
    current_entry: Option<Entry>,
    split_sessions: bool,
}

// Query logs have a format that looks like this:
//    Time                 Id Command    Argument
//    2021-08-17T16:28:35.019845Z        43 Quit
//    2021-08-17T16:28:35.374604Z        44 Connect   root@172.17.0.1 on test using TCP/IP
//    2021-08-17T16:28:35.374879Z        44 Query     SET autocommit=0
//    2021-08-17T16:28:35.382943Z        44 Query     SELECT DISTINCT TABLE_NAME, CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_NAME IS NOT NULL
//    2021-08-17T16:28:35.388404Z        44 Query     SHOW TABLES
//    2021-08-17T16:28:35.693179Z        45 Connect   root@172.17.0.1 on test using TCP/IP
//    2021-08-17T16:28:40.559222Z        44 Quit
// Where the "Id" field is a unique identifier for a given client connection,
// and various connections are interleaved.
// The code to read them is, in essence, a state machine.  We want to yield one
// Session for each of those connections.  To do this, we read the input
// line-by-line, buffering entries for each connection we find until we get a
// "Quit" command for that connection.  Once we do, we remove it from the
// "buffer" and return it.  Upon getting EOF on the input, we yield all the
// "incomplete" sessions that are still in the buffer.
impl<R: AsyncBufRead + Unpin> Stream<R> {
    pub fn new(input: R, split_sessions: bool) -> Self {
        Self {
            current_idx: 0,
            inner: input.lines(),
            partial_sessions: BTreeMap::new(),
            current_entry: None,
            split_sessions,
        }
    }

    pub async fn next(&mut self) -> Option<(usize, Session)> {
        let result = self.next_inner().await.map(|s| (self.current_idx, s));
        self.current_idx += 1;
        result
    }

    async fn next_inner(&mut self) -> Option<Session> {
        if self.split_sessions {
            self.next_split().await
        } else {
            self.next_unsplit().await
        }
    }

    async fn next_split(&mut self) -> Option<Session> {
        while let Ok(Some(line)) = self.inner.next_line().await {
            let completed_session = match Entry::from_str(&line) {
                Ok(v) => {
                    let old_entry = self.current_entry.replace(v);
                    if let Some(entry) = old_entry {
                        self.partial_sessions
                            .entry(entry.id)
                            .or_insert_with(Session::new)
                            .entries
                            .push(entry.clone());
                        if entry.command == Command::Quit {
                            self.partial_sessions.remove(&entry.id)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                Err(_) => {
                    if let Some(entry) = self.current_entry.as_mut() {
                        entry.arguments.push_str(&line);
                    };
                    None
                }
            };
            if let Some(session) = completed_session {
                if session.query_count() > 0 {
                    return Some(session);
                }
            }
        }
        while let Some(k) = self.partial_sessions.keys().cloned().next() {
            if let Some(session) = self.partial_sessions.remove(&k) {
                if session.query_count() > 0 {
                    return Some(session);
                }
            }
        }
        None
    }

    async fn next_unsplit(&mut self) -> Option<Session> {
        let mut session = Session::new();
        // TODO:  Queries can contain non-UTF8 bytes; this will skip those lines, which will cause
        // incorrect conversion (best case) or a completely broken logictest (worst case).  We
        // should handle these as just byte arrays instead of strings, but mysql_async accepts
        // queries as AsRef<str>, so at this time we can only support UTF8 anyway.
        while let Ok(Some(line)) = self.inner.next_line().await {
            if let Ok(v) = Entry::from_str(&line) {
                if let Some(entry) = self.current_entry.replace(v) {
                    session.entries.push(entry);
                }
            } else if let Some(entry) = self.current_entry.as_mut() {
                entry.arguments.push_str(&line);
            }
        }
        if session.entries.is_empty() {
            None
        } else {
            Some(session)
        }
    }
}
