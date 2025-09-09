use mysql_async::SslOpts;

use readyset_errors::ReadySetResult;

use crate::UpstreamConfig;

pub async fn mysql_ssl_opts_from(config: &UpstreamConfig) -> ReadySetResult<Option<SslOpts>> {
    let mut ssl_opts: Option<SslOpts> = None;
    if config.disable_upstream_ssl_verification {
        ssl_opts.get_or_insert_default();
        ssl_opts = ssl_opts
            .take()
            .map(|ssl_opts| ssl_opts.with_danger_accept_invalid_certs(true));
    }
    if let Some(certs) = config.get_root_certs().await? {
        ssl_opts.get_or_insert_default();
        ssl_opts = ssl_opts.take().map(|ssl_opts| {
            ssl_opts.with_root_certs(
                certs
                    .iter()
                    .map(pem::encode)
                    .map(String::into_bytes)
                    .map(Into::into)
                    .collect(),
            )
        });
    }
    Ok(ssl_opts)
}
