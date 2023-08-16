use std::io;
use std::net::SocketAddr;

use anyhow::bail;
use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::select;
use tokio::task::JoinHandle;

const BUFFER_SIZE: usize = 16 * 1024 * 1024;

async fn proxy_pipe(mut upstream: TcpStream, mut downstream: TcpStream) -> io::Result<()> {
    let mut upstream_buf = Box::new([0; BUFFER_SIZE]);
    let mut downstream_buf = Box::new([0; BUFFER_SIZE]);
    loop {
        select! {
            res = upstream.read(upstream_buf.as_mut()) => {
                let n = res?;
                if n > 0 {
                    downstream.write_all(&upstream_buf[..n]).await?;
                } else {
                    break Ok(())
                }
            }
            res = downstream.read(downstream_buf.as_mut()) => {
                let n = res?;
                if n > 0 {
                    upstream.write_all(&downstream_buf[..n]).await?;
                } else {
                    break Ok(())
                }
            }
        }
    }
}

async fn direct_proxy<A>(listen: A, upstreams: &[SocketAddr]) -> io::Result<JoinHandle<()>>
where
    A: ToSocketAddrs,
{
    let listener = TcpListener::bind(listen).await?;
    let mut upstreams = upstreams.iter().enumerate().cycle();

    'accept: loop {
        let (sock, _addr) = listener.accept().await.unwrap();
        sock.set_nodelay(true).unwrap();

        let mut start_idx = None;
        let upstream = loop {
            let (idx, upstream_addr) = upstreams.next().unwrap();
            match start_idx {
                Some(start) => {
                    if start == idx {
                        eprintln!("Could not connect to any upstream");
                        continue 'accept;
                    }
                }
                None => start_idx = Some(idx),
            }

            if let Ok(upstream) = TcpStream::connect(upstream_addr).await {
                break upstream;
            }
        };
        upstream.set_nodelay(true).unwrap();

        tokio::spawn(proxy_pipe(sock, upstream));
    }
}

/// The tiniest cutest little TCP load balancer you ever did see
///
/// Examples:
///
/// ```notrust
/// $ cargo run --release --bin tinylb -- -a 0.0.0.0:5430 -u 127.0.0.1:5434 -u 127.0.0.1:5435
/// ```
#[derive(Parser)]
struct Options {
    /// Address to listen at for new connections
    #[clap(short = 'a')]
    listen_address: SocketAddr,

    /// Upstream address to proxy to. Pass multiple times to configure multiple upstreams
    #[clap(short = 'u', num_args = 1.., required = true)]
    upstream: Vec<SocketAddr>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let options = Options::parse();
    if options.upstream.is_empty() {
        bail!("Must specify at least one upstream with -u")
    }
    direct_proxy(options.listen_address, &options.upstream).await?;
    Ok(())
}
