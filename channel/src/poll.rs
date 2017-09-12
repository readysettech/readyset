use tcp::*;
use rpc::*;

use std;
use std::net::SocketAddr;
use std::time::Duration;

use mio::{Events, Poll, PollOpt, Ready, Token};
use mio::net::{TcpListener, TcpStream};

use serde::Serialize;
use serde::de::DeserializeOwned;

const LISTENER: usize = 0;

const CHANNEL_OFFSET: usize = LISTENER + 1;
const MAX_CHANNEL: usize = CHANNEL_OFFSET + 100000;

const RPC_OFFSET: usize = MAX_CHANNEL + 1;
const MAX_RPC: usize = RPC_OFFSET + 100000;

pub enum AcceptDecision {
    Reject,
    Channel,
    Rpc,
}

pub enum GeneralizedPollEvent<'a, T, Q, R: 'a> {
    ResumePolling(&'a mut Option<Duration>),
    AcceptConnection(&'a mut AcceptDecision),
    ProcessChannel(T),
    ProcessRpc(Q, &'a mut Option<R>),
    Timeout,
}

#[derive(Debug)]
pub enum PollEvent<'a, T> {
    ResumePolling(&'a mut Option<Duration>),
    Process(T),
    Timeout,
}

#[derive(Debug)]
pub enum RpcPollEvent<'a, Q, R: 'a> {
    ResumePolling(&'a mut Option<Duration>),
    Process(Q, &'a mut Option<R>),
    Timeout,
}

#[derive(Debug)]
pub enum ProcessResult {
    KeepPolling,
    StopPolling,
}
pub use self::ProcessResult::*;

struct GeneralizedPollingLoopInner<T, Q, R> {
    poll: Poll,
    listener: Option<TcpListener>,
    channels: Vec<Option<TcpReceiver<T>>>,
    rpc_endpoints: Vec<Option<RpcServiceEndpoint<Q, R>>>,
}

pub struct GeneralizedPollingLoop<T, Q, R> {
    events: Events,
    inner: GeneralizedPollingLoopInner<T, Q, R>,
}

impl<T, Q, R> GeneralizedPollingLoopInner<T, Q, R>
where
    T: Serialize + DeserializeOwned,
    Q: Serialize + DeserializeOwned,
    R: Serialize + DeserializeOwned,
{
    pub fn get_listener_addr(&self) -> Option<SocketAddr> {
        self.listener.as_ref().and_then(|l| l.local_addr().ok())
    }

    pub fn add_channel(&mut self, stream: TcpStream) {
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
    fn remove_channel(&mut self, channel: usize) {
        let channel = self.channels[channel].take();
        let _ = self.poll.deregister(&channel.unwrap());
    }

    pub fn add_rpc_endpoint(&mut self, stream: TcpStream) {
        self.poll
            .register(
                &stream,
                Token(self.rpc_endpoints.len() + RPC_OFFSET),
                Ready::readable(),
                PollOpt::level(),
            )
            .unwrap();
        self.rpc_endpoints
            .push(Some(RpcServiceEndpoint::new(stream)));
    }

    fn remove_rpc_endpoint(&mut self, endpoint: usize) {
        let endpoint = self.rpc_endpoints[endpoint].take();
        let _ = self.poll.deregister(&endpoint.unwrap());
    }
}

impl<T, Q, R> GeneralizedPollingLoop<T, Q, R>
where
    T: Serialize + DeserializeOwned,
    Q: Serialize + DeserializeOwned,
    R: Serialize + DeserializeOwned,
{
    pub fn new() -> Self {
        let listener = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
        let addr = listener.local_addr().unwrap();
        Self::from_listener(TcpListener::from_listener(listener, &addr).unwrap())
    }

    pub fn from_listener(listener: TcpListener) -> Self {
        let poll = Poll::new().unwrap();
        poll.register(
            &listener,
            Token(LISTENER),
            Ready::readable(),
            PollOpt::level(),
        ).unwrap();

        Self {
            events: Events::with_capacity(32),
            inner: GeneralizedPollingLoopInner {
                poll,
                listener: Some(listener),
                channels: Vec::new(),
                rpc_endpoints: Vec::new(),
            },
        }
    }

    pub fn from_receivers(receivers: Vec<TcpReceiver<T>>) -> Self {
        let poll = Poll::new().unwrap();
        for (i, r) in receivers.iter().enumerate() {
            poll.register(r, Token(i + 1), Ready::readable(), PollOpt::level())
                .unwrap();
        }

        Self {
            events: Events::with_capacity(32),
            inner: GeneralizedPollingLoopInner {
                poll,
                listener: None,
                channels: receivers.into_iter().map(|r| Some(r)).collect(),
                rpc_endpoints: Vec::new(),
            },
        }
    }

    pub fn get_listener_addr(&self) -> Option<SocketAddr> {
        self.inner.get_listener_addr()
    }

    /// Execute steps of the polling loop until process_event() returns `StopPolling`.
    pub fn run_polling_loop<F>(&mut self, mut process_event: F)
    where
        F: FnMut(GeneralizedPollEvent<T, Q, R>) -> ProcessResult,
    {
        loop {
            let mut timeout = None;
            if let StopPolling = process_event(GeneralizedPollEvent::ResumePolling(&mut timeout)) {
                return;
            }

            if self.inner.poll.poll(&mut self.events, timeout).unwrap_or(0) == 0 &&
                timeout.is_some()
            {
                if let StopPolling = process_event(GeneralizedPollEvent::Timeout) {
                    return;
                }
            };

            for event in self.events.iter() {
                let token = event.token().0;
                let mut stop = KeepPolling;

                if token == 0 {
                    if let Ok((stream, _addr)) = self.inner.listener.as_mut().unwrap().accept() {
                        let mut decision = AcceptDecision::Reject;
                        stop = process_event(GeneralizedPollEvent::AcceptConnection(&mut decision));
                        match decision {
                            AcceptDecision::Reject => {}
                            AcceptDecision::Channel => self.inner.add_channel(stream),
                            AcceptDecision::Rpc => self.inner.add_rpc_endpoint(stream),
                        }
                    }
                } else if token < MAX_CHANNEL {
                    let channel = token - CHANNEL_OFFSET;
                    match self.inner.channels[channel].as_mut().unwrap().try_recv() {
                        Ok(message) => {
                            stop = process_event(GeneralizedPollEvent::ProcessChannel(message))
                        }
                        Err(TryRecvError::Disconnected) => {
                            self.inner.remove_channel(channel);
                        }
                        Err(TryRecvError::Empty) => {}
                        Err(TryRecvError::DeserializationError) => unreachable!(),
                    }
                } else if token < MAX_RPC {
                    let endpoint = token - RPC_OFFSET;
                    let mut reply = None;
                    match self.inner.rpc_endpoints[endpoint]
                        .as_mut()
                        .unwrap()
                        .try_recv()
                    {
                        Ok(message) => {
                            stop = process_event(
                                GeneralizedPollEvent::ProcessRpc(message, &mut reply),
                            );
                            if let Some(reply) = reply {
                                match self.inner.rpc_endpoints[endpoint]
                                    .as_mut()
                                    .unwrap()
                                    .send(&reply)
                                {
                                    Ok(_) => {}
                                    Err(RpcSendError::Disconnected) => {
                                        self.inner.remove_rpc_endpoint(endpoint)
                                    }
                                    Err(RpcSendError::SerializationError) => unreachable!(),
                                    Err(RpcSendError::StillNeedsFlush) => {
                                        // TODO: register to receive wake-up when the connection is
                                        // writable again.
                                        unimplemented!();
                                    }
                                }
                            }
                        }
                        Err(TryRecvError::Disconnected) => {
                            self.inner.remove_rpc_endpoint(endpoint);
                        }
                        Err(TryRecvError::Empty) => {}
                        Err(TryRecvError::DeserializationError) => unreachable!(),
                    }
                } else {
                    unreachable!();
                }

                if let StopPolling = stop {
                    return;
                }
            }
        }
    }
}

pub struct PollingLoop<T> {
    polling_loop: GeneralizedPollingLoop<T, (), ()>,
}
impl<T: Serialize + DeserializeOwned> PollingLoop<T> {
    pub fn new() -> Self {
        Self {
            polling_loop: GeneralizedPollingLoop::new(),
        }
    }
    pub fn from_listener(listener: TcpListener) -> Self {
        Self {
            polling_loop: GeneralizedPollingLoop::from_listener(listener),
        }
    }
    pub fn from_receivers(receivers: Vec<TcpReceiver<T>>) -> Self {
        Self {
            polling_loop: GeneralizedPollingLoop::from_receivers(receivers),
        }
    }
    pub fn get_listener_addr(&self) -> Option<SocketAddr> {
        self.polling_loop.get_listener_addr()
    }

    /// Execute steps of the polling loop until process_event() returns `StopPolling`.
    pub fn run_polling_loop<F>(&mut self, mut process_event: F)
    where
        F: FnMut(PollEvent<T>) -> ProcessResult,
    {
        self.polling_loop.run_polling_loop(|event| match event {
            GeneralizedPollEvent::ResumePolling(timeout) => {
                process_event(PollEvent::ResumePolling(timeout))
            }
            GeneralizedPollEvent::AcceptConnection(decision) => {
                *decision = AcceptDecision::Channel;
                KeepPolling
            }
            GeneralizedPollEvent::ProcessChannel(msg) => process_event(PollEvent::Process(msg)),
            GeneralizedPollEvent::ProcessRpc(_, _) => unreachable!(),
            GeneralizedPollEvent::Timeout => process_event(PollEvent::Timeout),
        })
    }
}

pub struct RpcPollingLoop<Q, R> {
    polling_loop: GeneralizedPollingLoop<(), Q, R>,
}
impl<Q: Serialize + DeserializeOwned, R: Serialize + DeserializeOwned> RpcPollingLoop<Q, R> {
    pub fn new() -> Self {
        Self {
            polling_loop: GeneralizedPollingLoop::new(),
        }
    }
    pub fn from_listener(listener: TcpListener) -> Self {
        Self {
            polling_loop: GeneralizedPollingLoop::from_listener(listener),
        }
    }
    pub fn get_listener_addr(&self) -> Option<SocketAddr> {
        self.polling_loop.get_listener_addr()
    }

    /// Execute steps of the polling loop until process_event() returns `StopPolling`.
    pub fn run_polling_loop<F>(&mut self, mut process_event: F)
    where
        F: FnMut(RpcPollEvent<Q, R>) -> ProcessResult,
    {
        self.polling_loop.run_polling_loop(|event| match event {
            GeneralizedPollEvent::ResumePolling(timeout) => {
                process_event(RpcPollEvent::ResumePolling(timeout))
            }
            GeneralizedPollEvent::AcceptConnection(decision) => {
                *decision = AcceptDecision::Rpc;
                KeepPolling
            }
            GeneralizedPollEvent::ProcessChannel(_) => unreachable!(),
            GeneralizedPollEvent::ProcessRpc(q, r) => process_event(RpcPollEvent::Process(q, r)),
            GeneralizedPollEvent::Timeout => process_event(RpcPollEvent::Timeout),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn simple_rpcs() {
        let mut service = RpcPollingLoop::<i64, i64>::new();
        let addr = service.get_listener_addr().unwrap();

        let t = thread::spawn(move || {
            service.run_polling_loop(|event| match event {
                RpcPollEvent::ResumePolling(duration) => {
                    *duration = Some(Duration::from_millis(100));
                    KeepPolling
                }
                RpcPollEvent::Timeout => StopPolling,
                RpcPollEvent::Process(message, reply) => {
                    *reply = Some(message + 42);
                    KeepPolling
                }
            });
        });

        let mut client = RpcClient::<i64, i64>::connect(&addr).unwrap();
        assert_eq!(client.send(&12).unwrap(), 54);
        assert_eq!(client.send(&83).unwrap(), 125);
        assert_eq!(client.send(&0).unwrap(), 42);
        t.join().unwrap();
    }

    #[test]
    fn multiclient_rpcs() {
        let mut service = RpcPollingLoop::<i64, i64>::new();
        let addr = service.get_listener_addr().unwrap();

        let t = thread::spawn(move || {
            service.run_polling_loop(|event| match event {
                RpcPollEvent::ResumePolling(duration) => {
                    *duration = Some(Duration::from_millis(100));
                    KeepPolling
                }
                RpcPollEvent::Timeout => StopPolling,
                RpcPollEvent::Process(message, reply) => {
                    *reply = Some(message + 42);
                    KeepPolling
                }
            });
        });

        let mut client = RpcClient::<i64, i64>::connect(&addr).unwrap();
        let mut client2 = RpcClient::<i64, i64>::connect(&addr).unwrap();
        let mut client3 = RpcClient::<i64, i64>::connect(&addr).unwrap();
        assert_eq!(client.send(&12).unwrap(), 54);
        assert_eq!(client2.send(&83).unwrap(), 125);
        assert_eq!(client.send(&0).unwrap(), 42);
        assert_eq!(client3.send(&83).unwrap(), 125);
        assert_eq!(client2.send(&84).unwrap(), 126);
        t.join().unwrap();
    }

    #[test]
    fn multiservice_rpcs() {
        let mut service = RpcPollingLoop::<i64, i64>::new();
        let mut service2 = RpcPollingLoop::<i64, i64>::new();
        let addr = service.get_listener_addr().unwrap();
        let addr2 = service2.get_listener_addr().unwrap();

        let t = thread::spawn(move || {
            service.run_polling_loop(|event| match event {
                RpcPollEvent::ResumePolling(duration) => {
                    *duration = Some(Duration::from_millis(100));
                    KeepPolling
                }
                RpcPollEvent::Timeout => StopPolling,
                RpcPollEvent::Process(message, reply) => {
                    *reply = Some(message + 1);
                    KeepPolling
                }
            });
        });

        let t2 = thread::spawn(move || {
            service2.run_polling_loop(|event| match event {
                RpcPollEvent::ResumePolling(duration) => {
                    *duration = Some(Duration::from_millis(100));
                    KeepPolling
                }
                RpcPollEvent::Timeout => StopPolling,
                RpcPollEvent::Process(message, reply) => {
                    *reply = Some(message + 2);
                    KeepPolling
                }
            });
        });

        let mut client = RpcClient::<i64, i64>::connect(&addr).unwrap();
        let mut client2 = RpcClient::<i64, i64>::connect(&addr2).unwrap();

        assert_eq!(client.send(&12).unwrap(), 13);
        assert_eq!(client.send(&83).unwrap(), 84);
        assert_eq!(client2.send(&12).unwrap(), 14);
        assert_eq!(client2.send(&83).unwrap(), 85);
        assert_eq!(client2.send(&0).unwrap(), 2);
        assert_eq!(client.send(&0).unwrap(), 1);

        t.join().unwrap();
        t2.join().unwrap();
    }
}
