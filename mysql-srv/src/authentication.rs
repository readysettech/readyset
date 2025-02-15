//! Implementation of MySQL's authentication methods:
//!
//! - [Secure Password Authentication][0] (mysql_native_password)
//! - [Caching SHA-2 Authentication][1] (caching_sha2_password)
//!
//! [0]: https://dev.mysql.com/doc/internals/en/secure-password-authentication.html
//! [1]: https://dev.mysql.com/doc/dev/mysql-server/latest/page_caching_sha2_authentication_exchanges.html

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use getrandom::getrandom;
use openssl::pkey::{PKey, Private};
use openssl::rsa::Rsa;
use sha1::{Digest, Sha1};
use sha2::Sha256;

use crate::error::MsqlSrvError;

use tokio::sync::Mutex;

pub type AuthData = [u8; 20];

/// The name of the legacy auth plugin
pub const MYSQL_NATIVE_PASSWORD: &str = "mysql_native_password";
/// The name of the default auth plugin since MySQL 8.0
pub const CACHING_SHA2_PASSWORD: &str = "caching_sha2_password";

// Authentication status flags
const AUTH_STATUS_ERROR: u8 = 0x01;
const AUTH_STATUS_FAST_AUTH: u8 = 0x03;
const AUTH_STATUS_FULL_AUTH: u8 = 0x04;

// Cache entry timeout (30 minutes)
const CACHE_TIMEOUT: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone)]
struct CacheEntry {
    hash: [u8; 32],
    last_used: Instant,
}

/// AuthCache saves the hashes of successful authentication  
/// attempts. These are used for sha2_caching_password fast
/// authentication.
#[derive(Debug, Clone)]
pub struct AuthCache {
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
    private_key: Arc<PKey<Private>>,
    pub_key_pem: Arc<String>,
}

impl Default for AuthCache {
    fn default() -> Self {
        // Generate a new 2048-bit RSA key pair
        let rsa = Rsa::generate(2048).expect("failed to generate RSA key");
        let private_key = PKey::from_rsa(rsa).expect("failed to create private key");
        let pub_key_pem = String::from_utf8(
            private_key
                .public_key_to_pem()
                .expect("failed to encode public key"),
        )
        .expect("invalid PEM encoding");

        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            private_key: Arc::new(private_key),
            pub_key_pem: Arc::new(pub_key_pem),
        }
    }
}

impl AuthCache {
    /// Create a new AuthCache and generate the RSA keys
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the public key in PEM format
    pub fn public_key_pem(&self) -> String {
        self.pub_key_pem.to_string()
    }

    fn generate_fast_digest(&self, password: &[u8]) -> [u8; 32] {
        // First round: SHA256(password)
        let digest1 = sha256(password);

        // Second round: SHA256(SHA256(password))
        sha256(&digest1)
    }

    /// Store a successful authentication in the cache
    pub fn cache_auth(&self, username: &str, password: &[u8]) {
        let mut cache = self.cache.write().unwrap();
        cache.insert(
            username.to_string(),
            CacheEntry {
                hash: self.generate_fast_digest(password),
                last_used: Instant::now(),
            },
        );
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
        let mut cache = self.cache.write().unwrap();
        if let Some(entry) = cache.get_mut(username) {
            let x = sha256(&[entry.hash.as_slice(), auth_data.as_slice()].concat());
            let y = xor_slice_modulus(&x, scramble);
            let expected = sha256(&y);

            if entry.hash == expected {
                if entry.last_used.elapsed() < CACHE_TIMEOUT {
                    entry.last_used = Instant::now();
                    return true;
                }
                // Entry expired, remove it
                cache.remove(username);
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

        /*let padding = rsa::Oaep::new::<Sha1>();
        let password = self
            .rsa_key
            .decrypt(padding, encrypted)
            .map_err(|_| MsqlSrvError::DecryptionError)?;*/

        let xor_password = xor_slice_modulus(&password, auth_data);

        Ok(xor_password)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum AuthPlugin {
    MysqlNativePassword,
    CachingSha2Password,
}

impl AuthPlugin {
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            MYSQL_NATIVE_PASSWORD => Some(AuthPlugin::MysqlNativePassword),
            CACHING_SHA2_PASSWORD => Some(AuthPlugin::CachingSha2Password),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            AuthPlugin::MysqlNativePassword => MYSQL_NATIVE_PASSWORD,
            AuthPlugin::CachingSha2Password => CACHING_SHA2_PASSWORD,
        }
    }

    pub fn get_switch_packet(&self, auth_data: AuthData) -> Vec<u8> {
        let mut auth_switch_request_packet =
            Vec::with_capacity(1 + self.name().len() + 1 + auth_data.len() + 1);
        auth_switch_request_packet.push(0xfe);
        auth_switch_request_packet.extend_from_slice(self.name().as_bytes());
        auth_switch_request_packet.push(0);
        auth_switch_request_packet.extend_from_slice(&auth_data);
        auth_switch_request_packet.push(0);
        auth_switch_request_packet
    }
}

#[derive(Debug)]
pub enum AuthStatus {
    Error,
    FastAuth,
    FullAuth,
}

impl From<AuthStatus> for u8 {
    fn from(status: AuthStatus) -> Self {
        match status {
            AuthStatus::Error => AUTH_STATUS_ERROR,
            AuthStatus::FastAuth => AUTH_STATUS_FAST_AUTH,
            AuthStatus::FullAuth => AUTH_STATUS_FULL_AUTH,
        }
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

/// Generate 20 random bytes of auth data for use as auth challenge data (see step 1 in the module
/// level documentation)
pub fn generate_auth_data() -> Result<AuthData, MsqlSrvError> {
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
pub fn hash_password_native(password: &[u8], auth_data: &AuthData) -> [u8; 20] {
    let mut res = sha1(password);
    let mut salted = [0u8; 40];
    salted[..20].clone_from_slice(auth_data);
    salted[20..].clone_from_slice(&sha1(&res));
    xor_slice_mut(&mut res, &sha1(&salted));
    res
}

/// Verify a password hash against the expected password using the specified auth plugin
pub fn verify_password(
    auth_plugin: AuthPlugin,
    password: &[u8],
    auth_data: &AuthData,
    client_response: &[u8],
) -> bool {
    match auth_plugin {
        AuthPlugin::MysqlNativePassword => {
            let expected = hash_password_native(password, auth_data);
            client_response == expected
        }
        AuthPlugin::CachingSha2Password => password == client_response,
    }
}

/// Handle caching_sha2_password authentication flow
pub async fn handle_sha2_auth(
    username: &str,
    auth_data: &AuthData,
    client_response: &[u8],
    auth_cache: &Arc<Mutex<AuthCache>>,
    is_secure_transport: bool,
) -> (AuthStatus, Option<Vec<u8>>) {
    let cache = auth_cache.lock().await;
    if cache.check_cache(username, client_response, auth_data) {
        return (AuthStatus::FastAuth, None);
    }
    if is_secure_transport {
        (AuthStatus::FullAuth, None)
    } else {
        (
            AuthStatus::FullAuth,
            Some(cache.public_key_pem().into_bytes()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_native_password_works() {
        let auth_data: AuthData = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0];
        let password = b"password";
        let result = hash_password_native(password, &auth_data);
        assert_eq!(
            result,
            [
                98, 3, 19, 63, 63, 49, 91, 179, 27, 253, 105, 140, 3, 177, 140, 44, 225, 127, 86,
                219
            ]
        );
    }
}
