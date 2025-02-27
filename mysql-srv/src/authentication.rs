//! Implementation of MySQL's authentication methods:
//!
//! - [Secure Password Authentication][0] (mysql_native_password)
//! - [Caching SHA-2 Authentication][1] (caching_sha2_password)
//!
//! [0]: https://web.archive.org/web/20210616065935/https://dev.mysql.com/doc/internals/en/secure-password-authentication.html
//! [1]: https://dev.mysql.com/doc/dev/mysql-server/latest/page_caching_sha2_authentication_exchanges.html

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use getrandom::getrandom;
use openssl::pkey::{PKey, Private};
use openssl::rsa::Rsa;
use sha1::{Digest, Sha1};
use sha2::Sha256;
use tracing::error;

use std::str::FromStr;
use thiserror::Error;

use crate::packet::PacketConn;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::MsqlSrvError;

pub type AuthData = [u8; 20];

/// The name of the legacy auth plugin
pub const MYSQL_NATIVE_PASSWORD: &str = "mysql_native_password";
/// The name of the default auth plugin since MySQL 8.0
pub const CACHING_SHA2_PASSWORD: &str = "caching_sha2_password";

// Authentication status flags
const CACHING_SHA2_PASSWORD_AUTH_STATUS_ERROR: u8 = 0x01;
const CACHING_SHA2_PASSWORD_AUTH_STATUS_FAST_AUTH: u8 = 0x03;
const CACHING_SHA2_PASSWORD_AUTH_STATUS_FULL_AUTH: u8 = 0x04;

const RSA_KEY_FILE_NAME: &str = "caching_sha2_rsa.pem";

static PUB_KEY_PEM: OnceLock<Mutex<String>> = OnceLock::new();

/// AuthCache saves the hashes of successful authentication  
/// attempts. These are used for sha2_caching_password fast
/// authentication.
#[derive(Debug)]
pub struct AuthCache {
    cache: RwLock<HashMap<String, [u8; 32]>>,
    private_key: PKey<Private>,
    pub_key_pem: String,
}

impl AuthCache {
    /// Create an AuthCache from an RSA key
    pub fn from_rsa(rsa: Rsa<Private>) -> Arc<Self> {
        let private_key = PKey::from_rsa(rsa).expect("failed to create private key");
        let pub_key_pem = String::from_utf8(
            private_key
                .public_key_to_pem()
                .expect("failed to encode public key"),
        )
        .expect("invalid PEM encoding");

        let mut pub_key_string = Self::get_pub_key_string().lock().unwrap();
        *pub_key_string = pub_key_pem.clone();

        Arc::new(Self {
            cache: RwLock::new(HashMap::new()),
            private_key,
            pub_key_pem,
        })
    }

    /// Create a new AuthCache and generate the RSA keys
    pub fn new(deployment_dir: Option<PathBuf>) -> Arc<Self> {
        // Check if the deployment directory is provided and
        // search for a saved RSA key.
        if let Some(ref rsa_path) = deployment_dir {
            // Read pcks12 file
            if let Ok(mut rsa_file) = std::fs::File::open(rsa_path.join(RSA_KEY_FILE_NAME)) {
                let mut rsa_file_contents = vec![];
                rsa_file
                    .read_to_end(&mut rsa_file_contents)
                    .expect("failed to read RSA file");

                // Load RSA from pem file
                let rsa =
                    Rsa::private_key_from_pem(&rsa_file_contents).expect("failed to load RSA key");
                return AuthCache::from_rsa(rsa);
            }
        }

        // Generate a new 2048-bit RSA key pair
        let rsa = Rsa::generate(2048).expect("failed to generate RSA key");

        // Save the RSA key to a file
        if let Some(rsa_path) = deployment_dir {
            let rsa_file_path = rsa_path.join(RSA_KEY_FILE_NAME);
            let mut rsa_file = File::create(rsa_file_path).expect("failed to create RSA file");
            rsa_file
                .write_all(&rsa.private_key_to_pem().expect("failed to encode RSA key"))
                .expect("failed to write RSA key");
        }

        AuthCache::from_rsa(rsa)
    }

    /// Get the public key in PEM format
    pub fn public_key_pem(&self) -> String {
        self.pub_key_pem.to_string()
    }

    /// Get the static public key
    pub fn get_pub_key_string() -> &'static Mutex<String> {
        PUB_KEY_PEM.get_or_init(|| Mutex::new(String::from("RSA key is not available")))
    }

    fn generate_fast_digest(&self, password: &[u8]) -> [u8; 32] {
        // First round: SHA256(password)
        let digest1 = sha256(password);

        // Second round: SHA256(SHA256(password))
        sha256(&digest1)
    }

    /// Store a successful authentication in the cache
    pub fn cache_auth(&self, username: &str, password: &[u8]) {
        match self.cache.write() {
            Ok(mut cache) => {
                cache.insert(username.to_string(), self.generate_fast_digest(password));
            }
            Err(e) => {
                error!("Failed to write to cache: {}", e);
            }
        }
    }

    /// Check if a user has a valid cached authentication
    ///
    /// Validation:
    /// scramble is: XOR(SHA256(password), SHA256(SHA256(SHA256(password)), auth_data))
    /// hash is: SHA2(SHA2(valid_password))
    /// Our aim is to check valid_password == password
    /// From hash and auth_data we generate: SHA2(hash,auth_data)
    ///     Let's call it x
    /// We then do : XOR(x, scramble) => Let's call this y
    ///     If password == valid_password, this should give us SHA2(password)
    /// We then do SHA2(y).
    ///     If password == valid_password, this should give us SHA2(SHA2(password))
    /// If SHA(y) == hash, then we have established that
    ///     password == valid_password
    pub fn check_cache(&self, username: &str, scramble: &[u8], auth_data: &AuthData) -> bool {
        let cache = self.cache.read();
        match cache {
            Ok(cache) => {
                if let Some(entry) = cache.get(username) {
                    let x = sha256(&[entry.as_slice(), auth_data.as_slice()].concat());
                    let y = xor_slice_modulus(&x, scramble);
                    let expected = sha256(&y);

                    if *entry == expected {
                        return true;
                    }
                }
            }
            Err(e) => {
                error!("Failed to read cache: {}", e);
            }
        }
        false
    }

    /// Decrypt password using the server's private key
    pub fn decrypt_password(
        &self,
        encrypted: &[u8],
        auth_data: &AuthData,
    ) -> Result<Vec<u8>, MsqlSrvError> {
        let mut password = vec![0; self.private_key.size()]; // Allocate enough space for decryption
        let len = self
            .private_key
            .rsa()
            .map_err(|_| MsqlSrvError::DecryptionError)?
            .private_decrypt(encrypted, &mut password, openssl::rsa::Padding::PKCS1_OAEP)
            .map_err(|_| MsqlSrvError::DecryptionError)?;

        password.truncate(len); // Trim buffer to actual decrypted length

        let xor_password = xor_slice_modulus(&password, auth_data);

        Ok(xor_password)
    }
}

/// Context for authentication process
#[derive(Debug, Clone, Copy)]
pub struct AuthContext<'a> {
    /// The username to authenticate
    pub username: &'a str,
    /// The password to authenticate
    pub password: &'a Option<Vec<u8>>,
    /// The handshake password
    pub handshake_password: &'a [u8],
    /// The authentication data
    pub auth_data: &'a AuthData,
    /// Whether authentication is required
    pub require_auth: bool,
}

/// Auth plugin enum
///
/// This enum is used to represent the authentication plugin to use.
/// It is used to switch the authentication plugin during the authentication process.
#[derive(Debug, Clone, Copy)]
pub enum AuthPlugin {
    /// MySQL Native Password authentication plugin
    Native(MysqlNativePassword),
    /// Caching SHA2 Password authentication plugin
    Sha2(CachingSha2Password),
}

#[derive(Debug, Error, Clone, Copy)]
#[error("Invalid authentication method")]
pub struct InvalidAuthenticationMethod;

impl FromStr for AuthPlugin {
    type Err = InvalidAuthenticationMethod;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            MYSQL_NATIVE_PASSWORD => Ok(AuthPlugin::Native(MysqlNativePassword)),
            CACHING_SHA2_PASSWORD => Ok(AuthPlugin::Sha2(CachingSha2Password)),
            _ => Err(InvalidAuthenticationMethod),
        }
    }
}

impl Default for AuthPlugin {
    fn default() -> Self {
        AuthPlugin::Sha2(CachingSha2Password)
    }
}

impl AuthPlugin {
    /// Create an AuthPlugin from a name
    pub fn from_name(s: &str) -> Option<Self> {
        match s {
            MYSQL_NATIVE_PASSWORD => Some(AuthPlugin::Native(MysqlNativePassword)),
            CACHING_SHA2_PASSWORD => Some(AuthPlugin::Sha2(CachingSha2Password)),
            _ => None,
        }
    }

    /// Get the name of the authentication plugin
    pub fn name(&self) -> &'static str {
        match self {
            AuthPlugin::Native(p) => p.name(),
            AuthPlugin::Sha2(p) => p.name(),
        }
    }

    /// Handle the authentication process
    pub async fn handle_authentication<S>(
        &self,
        ctx: &AuthContext<'_>,
        conn: &mut PacketConn<S>,
        auth_cache: &Arc<AuthCache>,
    ) -> Result<bool, io::Error>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        match self {
            AuthPlugin::Native(p) => p.handle_authentication(ctx, conn, auth_cache).await,
            AuthPlugin::Sha2(p) => p.handle_authentication(ctx, conn, auth_cache).await,
        }
    }

    /// Generate 20 random bytes of auth data for use as auth challenge data
    pub fn generate_auth_data(&self) -> Result<AuthData, MsqlSrvError> {
        match self {
            AuthPlugin::Native(p) => p.generate_auth_data(),
            AuthPlugin::Sha2(p) => p.generate_auth_data(),
        }
    }

    /// Get the authentication switch packet data
    pub fn get_switch_packet(&self, auth_data: &AuthData) -> Vec<u8> {
        match self {
            AuthPlugin::Native(p) => p.get_switch_packet(auth_data),
            AuthPlugin::Sha2(p) => p.get_switch_packet(auth_data),
        }
    }
}

/// Trait for implementing MySQL authentication plugins
pub trait AuthenticationPlugin: Send + Sync {
    /// Get the name of the authentication plugin
    fn name(&self) -> &'static str;

    /// Handle the authentication process
    async fn handle_authentication<S>(
        &self,
        ctx: &AuthContext<'_>,
        conn: &mut PacketConn<S>,
        auth_cache: &Arc<AuthCache>,
    ) -> Result<bool, io::Error>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send;

    /// Generate 20 random bytes of auth data for use as auth challenge data
    fn generate_auth_data(&self) -> Result<AuthData, MsqlSrvError> {
        let mut buf = [0u8; 20];
        match getrandom(&mut buf) {
            Ok(_) => {
                // MySQL's auth data must be printable ASCII characters
                for byte in &mut buf {
                    *byte &= 0x7f;
                    if *byte == b'\0' || *byte == b'$' {
                        *byte = *byte % 90 + 37;
                    }
                }
                Ok(buf)
            }
            Err(_) => Err(MsqlSrvError::GetRandomError),
        }
    }

    /// Get the authentication switch packet data
    fn get_switch_packet(&self, auth_data: &AuthData) -> Vec<u8> {
        let mut packet = Vec::with_capacity(1 + self.name().len() + 1 + auth_data.len() + 1);
        packet.push(0xfe); // Switch auth packet indicator
        packet.extend_from_slice(self.name().as_bytes());
        packet.push(0); // NULL terminator
        packet.extend_from_slice(auth_data);
        packet.push(0);
        packet
    }
}

/// MySQL Native Password authentication plugin
#[derive(Debug, Clone, Copy)]
pub struct MysqlNativePassword;

impl MysqlNativePassword {
    /// Hash a password alongside random challenge data per the mysql [secure password authentication
    /// algorithm][0].
    ///
    /// The algorithm is:
    ///
    /// ```notrust
    /// SHA1(password) XOR SHA1("20-bytes random data from server" <concat> SHA1(SHA1(password)))
    /// ```
    ///
    /// [0]: https://dev.mysql.com/doc/internals/en/secure-password-authentication.html
    fn hash_password_native(&self, password: &[u8], auth_data: &AuthData) -> [u8; 20] {
        let mut res = sha1(password);
        let mut salted = [0u8; 40];
        salted[..20].clone_from_slice(auth_data);
        salted[20..].clone_from_slice(&sha1(&res));
        xor_slice_mut(&mut res, &sha1(&salted));
        res
    }
}

impl AuthenticationPlugin for MysqlNativePassword {
    fn name(&self) -> &'static str {
        MYSQL_NATIVE_PASSWORD
    }

    async fn handle_authentication<S>(
        &self,
        ctx: &AuthContext<'_>,
        _conn: &mut PacketConn<S>,
        _auth_cache: &Arc<AuthCache>,
    ) -> Result<bool, io::Error>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        Ok(!ctx.require_auth
            || ctx.password.as_ref().is_some_and(|password| {
                let expected = self.hash_password_native(password, ctx.auth_data);
                expected == ctx.handshake_password
            }))
    }
}

#[derive(Debug)]
pub enum CachingSha2PasswordAuthStatus {
    Error,
    FastAuth,
    FullAuth,
}

impl From<CachingSha2PasswordAuthStatus> for u8 {
    fn from(status: CachingSha2PasswordAuthStatus) -> Self {
        match status {
            CachingSha2PasswordAuthStatus::Error => CACHING_SHA2_PASSWORD_AUTH_STATUS_ERROR,
            CachingSha2PasswordAuthStatus::FastAuth => CACHING_SHA2_PASSWORD_AUTH_STATUS_FAST_AUTH,
            CachingSha2PasswordAuthStatus::FullAuth => CACHING_SHA2_PASSWORD_AUTH_STATUS_FULL_AUTH,
        }
    }
}

/// Caching SHA2 Password authentication plugin
#[derive(Debug, Clone, Copy)]
pub struct CachingSha2Password;

impl CachingSha2Password {
    async fn send_auth_status_packet<S>(
        &self,
        conn: &mut PacketConn<S>,
        status: CachingSha2PasswordAuthStatus,
    ) -> Result<(), io::Error>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let packet = vec![0x01, u8::from(status)];
        conn.enqueue_packet(packet);
        conn.flush().await?;
        Ok(())
    }

    /// Check if the client has a valid cached authentication
    /// Handle caching_sha2_password authentication flow
    async fn check_fast_auth(
        &self,
        ctx: &AuthContext<'_>,
        auth_cache: &Arc<AuthCache>,
        is_secure_transport: bool,
    ) -> (CachingSha2PasswordAuthStatus, Option<Vec<u8>>) {
        if auth_cache.check_cache(ctx.username, ctx.handshake_password, ctx.auth_data) {
            return (CachingSha2PasswordAuthStatus::FastAuth, None);
        }
        if is_secure_transport {
            (CachingSha2PasswordAuthStatus::FullAuth, None)
        } else {
            (
                CachingSha2PasswordAuthStatus::FullAuth,
                Some(auth_cache.public_key_pem().into_bytes()),
            )
        }
    }

    async fn handle_full_auth<S>(
        &self,
        ctx: &AuthContext<'_>,
        conn: &mut PacketConn<S>,
        auth_cache: &Arc<AuthCache>,
        public_key: Option<Vec<u8>>,
    ) -> Result<bool, io::Error>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        // Request full auth
        self.send_auth_status_packet(conn, CachingSha2PasswordAuthStatus::FullAuth)
            .await?;

        let mut auth_success = false;

        // Receive encrypted password
        if let Some(packet) = conn.next().await? {
            conn.set_seq(packet.seq + 1);
            let handshake_password = if packet.data.len() == 1 && packet.data[0] == 0x02 {
                if let Some(key) = public_key {
                    let mut packet = vec![0x01];
                    packet.extend_from_slice(&key);
                    conn.enqueue_packet(packet);
                    conn.flush().await?;

                    // read the password
                    if let Some(packet) = conn.next().await? {
                        conn.set_seq(packet.seq + 1);
                        auth_cache.decrypt_password(&packet.data, ctx.auth_data)
                    } else {
                        Err(MsqlSrvError::DecryptionError)
                    }
                } else {
                    Err(MsqlSrvError::DecryptionError)
                }
            } else {
                Ok(packet.data.into())
            };
            if let Ok(decrypted) = handshake_password {
                if let Some(password) = ctx.password {
                    if *password == decrypted[..decrypted.len() - 1] {
                        auth_cache.cache_auth(ctx.username, password);
                        auth_success = true;
                    }
                }
            }
        }

        Ok(auth_success)
    }
}

impl AuthenticationPlugin for CachingSha2Password {
    fn name(&self) -> &'static str {
        CACHING_SHA2_PASSWORD
    }

    async fn handle_authentication<S>(
        &self,
        ctx: &AuthContext<'_>,
        conn: &mut PacketConn<S>,
        auth_cache: &Arc<AuthCache>,
    ) -> Result<bool, io::Error>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let mut auth_success = false;

        if !ctx.require_auth {
            // write successful fast auth if a password is sent
            // The handshake password is either a fast auth hash,
            // or a plain password zero terminated string.
            // When no password is provided, some client are sending
            // the empty string with \0 at the end.
            if ctx.handshake_password.len() > 1 {
                self.send_auth_status_packet(conn, CachingSha2PasswordAuthStatus::FastAuth)
                    .await?;
            }

            auth_success = true;
        } else {
            let (status, public_key) = self
                .check_fast_auth(ctx, auth_cache, conn.stream.is_secure())
                .await;

            match status {
                CachingSha2PasswordAuthStatus::FastAuth => {
                    // write successful fast auth
                    self.send_auth_status_packet(conn, CachingSha2PasswordAuthStatus::FastAuth)
                        .await?;

                    auth_success = true;
                }
                CachingSha2PasswordAuthStatus::FullAuth => {
                    auth_success = self
                        .handle_full_auth(ctx, conn, auth_cache, public_key)
                        .await?;
                }
                CachingSha2PasswordAuthStatus::Error => {}
            }
        }

        Ok(auth_success)
    }
}

fn xor_slice_modulus(b1: &[u8], b2: &[u8]) -> Vec<u8> {
    b1.iter()
        .enumerate()
        .map(|(i, &val)| val ^ b2[i % b2.len()])
        .collect()
}

/// Bytewise-XOR b1 with b2 in-place
fn xor_slice_mut<const N: usize>(b1: &mut [u8; N], b2: &[u8; N]) {
    b1.iter_mut().zip(b2.iter()).for_each(|(x, y)| *x ^= y);
}

/// Calculate SHA1 hash
fn sha1(input: &[u8]) -> [u8; 20] {
    let mut hasher = Sha1::new();
    hasher.update(input);
    hasher.finalize().into()
}

/// Calculate SHA256 hash
fn sha256(input: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(input);
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_native_password_works() {
        let auth_data: AuthData = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0];
        let password = b"password";
        let plugin = MysqlNativePassword;
        let result = plugin.hash_password_native(password, &auth_data);
        assert_eq!(
            result,
            [
                98, 3, 19, 63, 63, 49, 91, 179, 27, 253, 105, 140, 3, 177, 140, 44, 225, 127, 86,
                219
            ]
        );
    }
}
