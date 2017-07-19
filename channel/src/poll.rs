use tcp::*;

use std::time::Duration;

use mio::{Poll, Token, Events, Ready, PollOpt};
use mio::net::TcpListener;

use serde::{Serialize, Deserialize};

const LISTENER: Token = Token(0);
const CHANNEL_OFFSET: usize = 1;

pub enum PollEvent<'a, T> {
    ResumePolling(&'a mut Option<Duration>),
    Process(T),
    Timeout,
}

pub enum ProcessResult {
    KeepPolling,
    StopPolling,
}
pub use self::ProcessResult::*;

pub struct PollingLoop<T> {
    poll: Poll,
    events: Events,
    listener: TcpListener,
    channels: Vec<Option<TcpReceiver<T>>>,
}

impl<T> PollingLoop<T>
where
    T: Serialize,
    for<'de> T: Deserialize<'de>,
{
    pub fn new(listener: TcpListener) -> Self {
        let poll = Poll::new().unwrap();
        poll.register(&listener, LISTENER, Ready::readable(), PollOpt::level())
            .unwrap();

        Self {
            poll,
            events: Events::with_capacity(32),
            listener,
            channels: Vec::new(),
        }
    }


    pub fn run_polling_loop<F>(&mut self, mut process_event: F)
    where
        F: FnMut(PollEvent<T>) -> ProcessResult,
    {
        loop {
            let mut timeout = None;
            if let StopPolling = process_event(PollEvent::ResumePolling(&mut timeout)) {
                return;
            }

            if self.poll.poll(&mut self.events, timeout).unwrap_or(0) == 0 {
                if let StopPolling = process_event(PollEvent::Timeout) {
                    return;
                }
            };

            for event in self.events.iter() {
                if event.token() == LISTENER {
                    if let Ok((stream, _addr)) = self.listener.accept() {
                        self.poll
                            .register(
                                &stream,
                                Token(self.channels.len() + CHANNEL_OFFSET),
                                Ready::readable(),
                                PollOpt::level(),
                            )
                            .unwrap();
                        self.channels.push(Some(TcpReceiver::new(stream)));
                    }
                    continue;
                }

                loop {
                    let recv = self.channels[event.token().0 - CHANNEL_OFFSET]
                        .as_mut()
                        .unwrap()
                        .try_recv();

                    match recv {
                        Ok(message) => {
                            if let StopPolling = process_event(PollEvent::Process(message)) {
                                return;
                            }
                        }
                        Err(TryRecvError::Disconnected) => {
                            let channel = self.channels[event.token().0 - CHANNEL_OFFSET].take();
                            let _ = self.poll.deregister(&channel.unwrap());
                            break;
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::DeserializationError) => unreachable!(),
                    }
                }
            }
        }

    }
}
