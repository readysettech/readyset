use mysql_async::SslOpts;
use native_tls::{Certificate, TlsConnector};
use pem::Pem;

use readyset_errors::ReadySetResult;

use crate::UpstreamConfig;
///
/// Specifies how to verify certs when establishing a TLS connection.
pub enum ServerCertVerification {
    /// Verify the server's cert like normal, using the system's root cert store.
    Default,
    /// Verify the server's cert by also using the specified root certs.
    CustomCertificate(Vec<Pem>),
    /// Do not verify the server's cert.
    None,
}

impl ServerCertVerification {
    pub async fn from(config: &UpstreamConfig) -> ReadySetResult<Self> {
        Ok(if config.disable_upstream_ssl_verification {
            Self::None
        } else if let Some(certs) = config.get_root_certs().await? {
            Self::CustomCertificate(certs)
        } else {
            Self::Default
        })
    }
}

/// Return a connector suitable for establishing a TLS connection.
pub fn get_tls_connector(
    verification: &ServerCertVerification,
) -> native_tls::Result<TlsConnector> {
    let mut builder = TlsConnector::builder();

    match verification {
        ServerCertVerification::Default => {}
        ServerCertVerification::CustomCertificate(certs) => {
            for cert in certs {
                builder.add_root_certificate(Certificate::from_der(cert.contents())?);
            }
        }
        ServerCertVerification::None => {
            builder.danger_accept_invalid_certs(true);
        }
    };

    let connector = builder.build()?;
    Ok(connector)
}

/// Return MySQL options suitable for establishing a TLS connection.
pub fn get_mysql_tls_config(verification: &ServerCertVerification) -> Option<SslOpts> {
    match verification {
        ServerCertVerification::Default => None,
        ServerCertVerification::CustomCertificate(certs) => Some(
            SslOpts::default().with_root_certs(
                certs
                    .iter()
                    .map(pem::encode)
                    .map(String::into_bytes)
                    .map(Into::into)
                    .collect(),
            ),
        ),
        ServerCertVerification::None => {
            Some(SslOpts::default().with_danger_accept_invalid_certs(true))
        }
    }
}
