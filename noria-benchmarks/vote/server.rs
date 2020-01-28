use super::{Backend, ConvenientSession};
use failure::Error;
use ssh2;
use std::borrow::Cow;
use std::io;
use std::io::prelude::*;
use std::{thread, time};
use tsunami::Session;

pub(crate) enum ServerHandle<'a> {
    Netsoup(ssh2::Channel<'a>),
    HandledBySystemd,
    Hybrid,
}

impl<'a> ServerHandle<'a> {
    fn end(self, server: &Session, backend: &Backend) -> Result<(), Error> {
        match self {
            ServerHandle::Netsoup(mut w) => {
                // assert_eq!(backend, &Backend::Netsoup);

                // we need to terminate the worker.
                // the SSH spec does have support for sending signals to remote workers, but
                // neither OpenSSH nor libssh2 support that feature:
                //
                //  - https://bugzilla.mindrot.org/show_bug.cgi?id=1424
                //  - https://www.libssh2.org/mail/libssh2-devel-archive-2009-09/0079.shtml
                //
                // so, instead we have to hack around it by finding the pid and killing it
                if w.eof() {
                    // terminated prematurely!
                    unimplemented!();
                }

                let _ = server.just_exec(&["pkill", "-f", "noria-server"])?;

                let mut stdout = String::new();
                let mut stderr = String::new();
                w.stderr().read_to_string(&mut stderr)?;
                w.read_to_string(&mut stdout)?;
                w.wait_eof()?;

                if !stderr.is_empty() {
                    println!("noria-server stdout");
                    println!("{}", stdout);
                    println!("noria-server stderr");
                    println!("{}", stderr);
                }

                // also stop zookeeper
                match server.just_exec(&["sudo", "systemctl", "stop", "zookeeper"]) {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => bail!(e),
                    Err(e) => Err(e)?,
                }
            }
            ServerHandle::Hybrid => {
                match server.just_exec(&[
                    "sudo",
                    "systemctl",
                    "stop",
                    Backend::Mysql.systemd_name().unwrap(),
                ]) {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => bail!(e),
                    Err(e) => Err(e)?,
                }
                match server.just_exec(&[
                    "sudo",
                    "systemctl",
                    "stop",
                    Backend::Memcached.systemd_name().unwrap(),
                ]) {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => bail!(e),
                    Err(e) => Err(e)?,
                }
            }
            ServerHandle::HandledBySystemd => match server.just_exec(&[
                "sudo",
                "systemctl",
                "stop",
                backend.systemd_name().unwrap(),
            ]) {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => bail!(e),
                Err(e) => Err(e)?,
            },
        }
        Ok(())
    }
}

#[must_use]
pub(crate) struct Server<'a> {
    pub(crate) server: &'a Session,
    listen_addr: &'a str,

    handle: ServerHandle<'a>,
}

impl<'a> Server<'a> {
    pub(crate) fn end(self, backend: &Backend) -> Result<(), Error> {
        self.handle.end(self.server, backend)
    }

    pub(crate) fn between_targets(self, backend: &Backend) -> Result<Self, Error> {
        match *backend {
            Backend::Netsoup { .. } | Backend::Memcached | Backend::Hybrid => {
                let s = self.server;
                let a = self.listen_addr;

                // these backends need to be cleared after every run
                eprintln!(" -> restarting server");
                self.end(backend)?;

                // give it some time to shut down
                thread::sleep(time::Duration::from_secs(1));

                // start a new one!
                let s = start(s, a, backend)?.or_else(|e| bail!(e))?;
                eprintln!(" .. server restart completed");
                Ok(s)
            }
            Backend::Mysql | Backend::Mssql => Ok(self),
        }
    }

    pub(crate) fn wait(&mut self, client: &Session, backend: &Backend) -> Result<(), Error> {
        if let Backend::Netsoup { .. } = *backend {
            // netsoup *worker* doesn't have a well-defined port :/
            thread::sleep(time::Duration::from_secs(10));
            return Ok(());
        }

        let start = time::Instant::now();
        client.set_timeout(10000);
        // sql server can be *really* slow to start b/c EBS is slow
        while start.elapsed() < time::Duration::from_secs(5 * 60) {
            let e: Result<(), ssh2::Error> = try {
                let mut c = client.channel_direct_tcpip(self.listen_addr, backend.port(), None)?;
                c.send_eof()?;
                c.wait_eof()?;
            };

            if let Err(e) = e {
                if e.code() == -21 {
                    // "connect failed"
                    continue;
                }
                Err(e)?;
            } else {
                return Ok(());
            }
        }
        bail!("server never started")
    }

    fn get_pid(&mut self, pgrep: &str) -> Result<Option<usize>, Error> {
        let mut c = self.server.exec(&["pgrep", pgrep])?;
        let mut stdout = String::new();
        c.read_to_string(&mut stdout)?;
        c.wait_eof()?;

        Ok(stdout.lines().next().and_then(|line| line.parse().ok()))
    }

    fn get_mem(&mut self, pgrep: &str) -> Result<Option<usize>, Error> {
        let pid = self
            .get_pid(pgrep)?
            .ok_or(format_err!("couldn't find server pid"))?;
        let f = format!("/proc/{}/status", pid);
        let mut c = self.server.exec(&["grep", "VmRSS", &f])?;

        let mut stdout = String::new();
        c.read_to_string(&mut stdout)?;
        c.wait_eof()?;

        Ok(stdout
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().skip(1).next())
            .and_then(|col| col.parse().ok()))
    }

    pub(crate) fn write_stats(
        &mut self,
        backend: &Backend,
        w: &mut dyn io::Write,
    ) -> Result<(), Error> {
        // first, get uptime (for load avgs)
        let mut c = self.server.exec(&["uptime"])?;
        w.write_all(b"uptime:\n")?;
        io::copy(&mut c, w)?;
        c.wait_eof()?;

        match *backend {
            Backend::Memcached => {
                let mem = self
                    .get_mem("memcached")?
                    .ok_or(format_err!("couldn't find memcached memory usage"))?;
                w.write_all(format!("memory: {}", mem).as_bytes())?;
            }
            Backend::Hybrid => {
                let mem = self
                    .get_mem("memcached")?
                    .ok_or(format_err!("couldn't find memcached memory usage"))?;
                w.write_all(format!("memcached: {}", mem).as_bytes())?;

                let mut c = self.server.exec(&[
                    "mysql",
                    "-N",
                    "-t",
                    "-u",
                    "soup",
                    "soup",
                    "<",
                    "noria/benchmarks/vote/mysql_stat.sql",
                ])?;

                w.write_all(b"tables:\n")?;
                io::copy(&mut c, w)?;
                c.wait_eof()?;
            }
            Backend::Netsoup { .. } => {
                let mem = self
                    .get_mem("noria-server")?
                    .ok_or(format_err!("couldn't find noria-server memory usage"))?;
                w.write_all(format!("memory: {}", mem).as_bytes())?;
            }
            Backend::Mysql => {
                let mut c = self.server.exec(&[
                    "mysql",
                    "-N",
                    "-t",
                    "-u",
                    "soup",
                    "soup",
                    "<",
                    "noria/benchmarks/vote/mysql_stat.sql",
                ])?;

                w.write_all(b"tables:\n")?;
                io::copy(&mut c, w)?;
                c.wait_eof()?;
            }
            Backend::Mssql => {
                let mut c = self
                    .server
                    .exec(&["du", "-s", "/opt/mssql-ramdisk/data/"])?;
                w.write_all(b"disk:\n")?;
                io::copy(&mut c, w)?;
                c.wait_eof()?;

                let mut c = self.server.exec(&[
                    "/opt/mssql-tools/bin/sqlcmd",
                    "-U",
                    "SA",
                    "-i",
                    "noria/benchmarks/vote/mssql_stat.sql",
                    // assume password is set in SQLCMDPASSWORD
                    "-S",
                    "127.0.0.1",
                    "-I",
                    "-d",
                    "soup",
                    "-h",
                    "-1",
                ])?;
                w.write_all(b"tables:\n")?;
                io::copy(&mut c, w)?;
                c.wait_eof()?;
            }
        }
        Ok(())
    }
}

pub(crate) fn start<'a>(
    server: &'a Session,
    listen_addr: &'a str,
    b: &Backend,
) -> Result<Result<Server<'a>, String>, Error> {
    let sh = match *b {
        Backend::Netsoup { shards } => {
            // build worker if it hasn't been built already
            match server.in_noria(&["cargo", "b", "--release", "--bin", "noria-server"]) {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Ok(Err(e)),
                Err(e) => return Err(e),
            }

            // wipe zookeeper state
            match server.just_exec(&["sudo", "systemctl", "stop", "zookeeper"]) {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Ok(Err(e)),
                Err(e) => return Err(e),
            }
            match server.just_exec(&["sudo", "rm", "-rf", "/var/lib/zookeeper/version-2"]) {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Ok(Err(e)),
                Err(e) => return Err(e),
            }

            // now that server components have been built, start zookeeper
            match server.just_exec(&["sudo", "systemctl", "start", "zookeeper"]) {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Ok(Err(e)),
                Err(e) => return Err(e),
            }

            // wait for zookeeper to be running
            let start = time::Instant::now();
            while server
                .just_exec(&["echo", "-n", ">", "/dev/tcp/127.0.0.1/2181"])?
                .is_err()
            {
                thread::sleep(time::Duration::from_secs(1));
                if start.elapsed() > time::Duration::from_secs(30) {
                    bail!("zookeeper wouldn't start");
                }
            }

            // then start the worker (which will also be a controller)
            // TODO: should we worry about the running directory being on an SSD here?
            let shards = format!("{}", shards.unwrap_or(0));
            let w = {
                let mut cmd: Vec<Cow<str>> =
                    ["cd", "noria", "&&"].iter().map(|&s| s.into()).collect();
                cmd.extend(vec![
                    "/home/ubuntu/target/release/noria-server".into(),
                    "--durability".into(),
                    "memory".into(),
                    "--shards".into(),
                    shards.into(),
                    "--deployment".into(),
                    "votebench".into(),
                    "--address".into(),
                    listen_addr.into(),
                ]);
                let cmd: Vec<_> = cmd.iter().map(|s| &**s).collect();
                server.exec(&cmd[..]).unwrap()
            };

            ServerHandle::Netsoup(w)
        }
        Backend::Hybrid => {
            match server.just_exec(&[
                "sudo",
                "systemctl",
                "start",
                Backend::Memcached.systemd_name().unwrap(),
            ]) {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Ok(Err(e)),
                Err(e) => return Err(e),
            }
            match server.just_exec(&[
                "sudo",
                "systemctl",
                "start",
                Backend::Mysql.systemd_name().unwrap(),
            ]) {
                Ok(Ok(_)) => ServerHandle::Hybrid,
                Ok(Err(e)) => return Ok(Err(e)),
                Err(e) => return Err(e),
            }
        }
        Backend::Memcached | Backend::Mysql | Backend::Mssql => {
            match server.just_exec(&["sudo", "systemctl", "start", b.systemd_name().unwrap()]) {
                Ok(Ok(_)) => ServerHandle::HandledBySystemd,
                Ok(Err(e)) => return Ok(Err(e)),
                Err(e) => return Err(e),
            }
        }
    };

    Ok(Ok(Server {
        server,
        listen_addr,
        handle: sh,
    }))
}
