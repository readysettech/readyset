use mysql_async::SslOpts;

use crate::UpstreamConfig;

pub fn mysql_ssl_opts_from(config: &UpstreamConfig) -> Option<SslOpts> {
    let mut ssl_opts: Option<SslOpts> = None;
    if config.disable_upstream_ssl_verification {
        ssl_opts.get_or_insert_default();
        ssl_opts = ssl_opts
            .take()
            .map(|ssl_opts| ssl_opts.with_danger_accept_invalid_certs(true));
    }
    if let Some(cert_path) = config.ssl_root_cert.clone() {
        ssl_opts.get_or_insert_default();
        ssl_opts = ssl_opts
            .take()
            .map(|ssl_opts| ssl_opts.with_root_certs(vec![cert_path.into()]));
    }
    ssl_opts
}
