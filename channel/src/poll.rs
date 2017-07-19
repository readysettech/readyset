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
    listener: Option<TcpListener>,
    channels: Vec<Option<TcpReceiver<T>>>,
}

impl<T> PollingLoop<T>
where
    T: Serialize,
    for<'de> T: Deserialize<'de>,
{
    pub fn from_listener(listener: TcpListener) -> Self {
        let poll = Poll::new().unwrap();
        poll.register(&listener, LISTENER, Ready::readable(), PollOpt::level())
            .unwrap();

        Self {
            poll,
            events: Events::with_capacity(32),
            listener: Some(listener),
            channels: Vec::new(),
        }
    }

    pub fn from_receivers(receivers: Vec<TcpReceiver<T>>) -> Self {
        let poll = Poll::new().unwrap();
        for (i, r) in receivers.iter().enumerate() {
            poll.register(r, Token(i + 1), Ready::readable(), PollOpt::level())
                .unwrap();
        }

        Self {
            poll,
            events: Events::with_capacity(32),
            listener: None,
            channels: receivers.into_iter().map(|r| Some(r)).collect(),
        }
    }

    /// Execute steps of the polling loop until process_event() returns `StopPolling`.
    ///
    /// Note that to prevent packet loss, if multiple packets are retrieved when polling a channel
    /// then process_event will be called with each in turn. This will happen regardless of the
    /// value returned by that function. However, once the queue has been drained, the polling loop
    /// will stop if the return value from any of the packets was `StopPolling`
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
                    if let Ok((stream, _addr)) = self.listener.as_mut().unwrap().accept() {
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

                let mut stop_polling = false;
                loop {
                    let recv = self.channels[event.token().0 - CHANNEL_OFFSET]
                        .as_mut()
                        .unwrap()
                        .try_recv();

                    match recv {
                        Ok(message) => {
                            if let StopPolling = process_event(PollEvent::Process(message)) {
                                stop_polling = true;
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

                if stop_polling {
                    return;
                }
            }
        }

    }
}
