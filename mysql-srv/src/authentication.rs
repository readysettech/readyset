//! Implementation of MySQL's authentication methods:
//!
//! - [Secure Password Authentication][0] (mysql_native_password)
//! - [Caching SHA-2 Authentication][1] (caching_sha2_password)
//!
//! [0]: https://web.archive.org/web/20210616065935/https://dev.mysql.com/doc/internals/en/secure-password-authentication.html
//! [1]: https://dev.mysql.com/doc/dev/mysql-server/latest/page_caching_sha2_authentication_exchanges.html

use std::collections::HashMap;
use std::fs;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, OnceLock, RwLock};

use getrandom::fill;
use rsa::pkcs1::{DecodeRsaPrivateKey, EncodeRsaPrivateKey};
use rsa::pkcs8::EncodePublicKey;
use rsa::{Oaep, RsaPrivateKey};
use sha1::{Digest, Sha1};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use thiserror::Error;
use tracing::error;

use crate::error::MsqlSrvError;

pub type AuthData = [u8; 20];

/// The name of the legacy auth plugin.
pub const MYSQL_NATIVE_PASSWORD: &str = "mysql_native_password";

/// The name of the default auth plugin since MySQL 8.0.
pub const CACHING_SHA2_PASSWORD: &str = "caching_sha2_password";

/// Kept for backwards compatibility during the migration (CL 1-2).
/// Removed in CL 3 when all callers switch to the new types.
pub const AUTH_PLUGIN_NAME: &str = "mysql_native_password";

const RSA_PRIVATE_KEY_FILE: &str = "caching_sha2_password_private_key.pem";
const RSA_PUBLIC_KEY_FILE: &str = "caching_sha2_password_public_key.pem";

/// Client requests the server's RSA public key during full-auth.
#[allow(dead_code)] // Used in CL 4
pub const RSA_PUBLIC_KEY_REQUEST: u8 = 0x02;
/// Server indicates fast-auth succeeded (cache hit).
#[allow(dead_code)] // Used in CL 4
pub const FAST_AUTH_SUCCESS: u8 = 0x03;
/// Server requests full authentication (cache miss).
#[allow(dead_code)] // Used in CL 4
pub const PERFORM_FULL_AUTH: u8 = 0x04;
/// Holds the RSA key pair used for `caching_sha2_password` full-auth exchanges.
///
/// Initialized once at startup via [`AuthKeys::initialize`] and accessed
/// thereafter via [`AuthKeys::get`]. No mutex is needed because the keys are
/// immutable after initialization.
#[derive(Debug)]
pub struct AuthKeys {
    private_key: RsaPrivateKey,
    pub_key_pem: String,
}

static AUTH_KEYS: OnceLock<AuthKeys> = OnceLock::new();

impl AuthKeys {
    /// Initialize the global RSA keys. Must be called exactly once at startup.
    ///
    /// If `deployment_dir` is `Some`, attempts to load an existing key pair
    /// from `<dir>/caching_sha2_password_private_key.pem` and
    /// `<dir>/caching_sha2_password_public_key.pem` (matching MySQL's
    /// default file names). If the files do not exist, a new 2048-bit RSA
    /// key pair is generated and saved there.
    ///
    /// If `deployment_dir` is `None`, a new ephemeral key pair is generated
    /// (not persisted).
    ///
    /// # Errors
    ///
    /// * [`MsqlSrvError::KeyAlreadyInitialized`] if called more than once.
    /// * [`MsqlSrvError::KeyCreationError`] if RSA generation fails.
    /// * [`MsqlSrvError::KeyLoadError`] if a PEM file exists but cannot be parsed.
    /// * [`MsqlSrvError::EncodingError`] if PEM encoding fails.
    pub fn initialize(deployment_dir: Option<PathBuf>) -> Result<(), MsqlSrvError> {
        let keys = if let Some(dir) = deployment_dir {
            Self::load_or_create_keys(dir)?
        } else {
            let mut rng = rsa::rand_core::OsRng;
            let private_key = RsaPrivateKey::new(&mut rng, 2048)
                .map_err(|e| MsqlSrvError::KeyCreationError(e.to_string()))?;
            Self::from_private_key(private_key)?
        };

        AUTH_KEYS
            .set(keys)
            .map_err(|_| MsqlSrvError::KeyAlreadyInitialized)
    }

    /// Access the initialized keys.
    ///
    /// # Panics
    ///
    /// Panics if [`AuthKeys::initialize`] has not been called. This is a
    /// startup invariant -- callers must ensure initialization before any
    /// connections are accepted.
    pub fn get() -> &'static AuthKeys {
        AUTH_KEYS
            .get()
            .expect("AuthKeys not initialized. Call AuthKeys::initialize() at startup")
    }

    /// Access the initialized keys, returning `None` if not yet initialized.
    pub fn try_get() -> Option<&'static AuthKeys> {
        AUTH_KEYS.get()
    }

    /// The RSA public key in PEM-encoded form.
    pub fn public_key_pem(&self) -> &str {
        &self.pub_key_pem
    }

    /// The RSA private key, for use in password decryption.
    pub(crate) fn private_key(&self) -> &RsaPrivateKey {
        &self.private_key
    }

    fn from_private_key(private_key: RsaPrivateKey) -> Result<Self, MsqlSrvError> {
        let pub_key_pem = private_key
            .to_public_key()
            .to_public_key_pem(rsa::pkcs8::LineEnding::LF)
            .map_err(|e| MsqlSrvError::EncodingError(e.to_string()))?;

        Ok(Self {
            private_key,
            pub_key_pem,
        })
    }

    fn load_or_create_keys(dir: PathBuf) -> Result<Self, MsqlSrvError> {
        let private_key_path = dir.join(RSA_PRIVATE_KEY_FILE);
        let public_key_path = dir.join(RSA_PUBLIC_KEY_FILE);

        fs::create_dir_all(&dir).map_err(|e| {
            MsqlSrvError::KeyLoadError(format!("failed to create key directory: {e}"))
        })?;

        if private_key_path.exists() {
            let pem_str = fs::read_to_string(&private_key_path)
                .map_err(|e| MsqlSrvError::KeyLoadError(e.to_string()))?;
            let private_key = RsaPrivateKey::from_pkcs1_pem(&pem_str)
                .map_err(|e| MsqlSrvError::KeyLoadError(e.to_string()))?;
            return Self::from_private_key(private_key);
        }

        let mut rng = rsa::rand_core::OsRng;
        let private_key = RsaPrivateKey::new(&mut rng, 2048)
            .map_err(|e| MsqlSrvError::KeyCreationError(e.to_string()))?;

        let keys = Self::from_private_key(private_key)?;

        Self::write_key_file(&private_key_path, 0o600, || {
            keys.private_key
                .to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
                .map_err(|e| MsqlSrvError::EncodingError(e.to_string()))
                .map(|pem| pem.as_str().to_owned())
        })?;

        Self::write_key_file(&public_key_path, 0o644, || Ok(keys.pub_key_pem.clone()))?;

        Ok(keys)
    }

    fn write_key_file(
        path: &std::path::Path,
        #[allow(unused_variables)] mode: u32,
        pem_fn: impl FnOnce() -> Result<String, MsqlSrvError>,
    ) -> Result<(), MsqlSrvError> {
        let pem = pem_fn()?;
        let mut opts = fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        opts.mode(mode);
        opts.open(path)
            .and_then(|mut f| f.write_all(pem.as_bytes()))
            .map_err(|e| {
                MsqlSrvError::KeyLoadError(format!("failed to write {}: {e}", path.display()))
            })
    }
}

/// Caches the double-SHA256 digest of successfully authenticated passwords.
///
/// Used by `caching_sha2_password` to skip the full RSA-based exchange on
/// subsequent connections from the same user (the "fast-auth" path).
#[derive(Debug)]
pub struct AuthCache {
    /// Maps username -> SHA256(SHA256(password)).
    ///
    /// Lock ordering: this is the only lock in this struct. Callers must not
    /// hold any other lock while accessing the cache.
    cache: RwLock<HashMap<String, [u8; 32]>>,
}

impl AuthCache {
    /// Create a new empty cache.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            cache: RwLock::new(HashMap::new()),
        })
    }

    /// Store the double-SHA256 digest of a successfully authenticated password.
    pub fn insert(&self, username: &str, password: &[u8]) {
        let digest = CachingSha2Password::generate_fast_digest(password);
        match self.cache.write() {
            Ok(mut cache) => {
                cache.insert(username.to_string(), digest);
            }
            Err(e) => {
                error!("Failed to write to auth cache: {}", e);
            }
        }
    }

    /// Validate a scramble against the cached digest for `username`.
    ///
    /// The validation algorithm (matching MySQL's `Validate_scramble::validate`):
    /// 1. Look up cached `hash = SHA256(SHA256(password))`
    /// 2. Compute `x = SHA256(hash || nonce)`
    /// 3. Compute `y = XOR(x, scramble)` -- yields `SHA256(password)` if correct
    /// 4. Check `SHA256(y) == hash`
    pub fn check(&self, username: &str, scramble: &[u8], nonce: &AuthData) -> bool {
        // Copy entry out before releasing the read lock so we don't hold it
        // during hash computation.
        let entry = match self.cache.read() {
            Ok(cache) => cache.get(username).copied(),
            Err(e) => {
                error!("Failed to read auth cache: {}", e);
                return false;
            }
        };

        if let Some(entry) = entry {
            let x: [u8; 32] = {
                let mut hasher = Sha256::new();
                hasher.update(entry);
                hasher.update(nonce);
                hasher.finalize().into()
            };
            let mut y = [0u8; 32];
            for (i, (&a, &b)) in x.iter().zip(scramble.iter()).enumerate() {
                y[i] = a ^ b;
            }
            let expected: [u8; 32] = sha256(&y);
            // Constant-time comparison to prevent timing side-channel attacks.
            return entry.ct_eq(&expected).into();
        }

        false
    }
}

/// Represents a MySQL authentication plugin.
#[derive(Debug, Clone, Copy)]
pub enum AuthPlugin {
    /// MySQL Native Password (SHA1-based, legacy).
    Native(MysqlNativePassword),
    /// Caching SHA-2 Password (SHA256-based, default since MySQL 8.0).
    Sha2(CachingSha2Password),
}

#[derive(Debug, Error, Clone)]
#[error("Invalid authentication method: {0}")]
pub struct InvalidAuthenticationMethod(pub String);

impl FromStr for AuthPlugin {
    type Err = InvalidAuthenticationMethod;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            MYSQL_NATIVE_PASSWORD => Ok(AuthPlugin::Native(MysqlNativePassword)),
            CACHING_SHA2_PASSWORD => Ok(AuthPlugin::Sha2(CachingSha2Password)),
            _ => Err(InvalidAuthenticationMethod(s.to_string())),
        }
    }
}

impl Default for AuthPlugin {
    fn default() -> Self {
        AuthPlugin::Native(MysqlNativePassword)
    }
}

impl std::fmt::Display for AuthPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl AuthPlugin {
    /// The wire-protocol name of this authentication plugin.
    pub fn name(&self) -> &'static str {
        match self {
            AuthPlugin::Native(_) => MYSQL_NATIVE_PASSWORD,
            AuthPlugin::Sha2(_) => CACHING_SHA2_PASSWORD,
        }
    }
}

/// Context for authentication process.
///
/// Bundles together the pieces of state that the authentication flow
/// needs. Constructed once per connection during the handshake and
/// threaded through the auth path.
#[derive(Debug)]
pub struct AuthContext<'a> {
    /// Username sent by the client in the handshake.
    pub username: &'a str,
    /// Plain-text password returned by [`MySqlShim::password_for_username`],
    /// or `None` when the backend does not know the user.
    pub password: Option<&'a [u8]>,
    /// The password bytes from the client handshake (or auth-switch response).
    pub handshake_password: &'a [u8],
    /// The 20-byte random challenge sent in the server greeting.
    pub auth_data: &'a AuthData,
    /// Whether the server requires authentication (false = allow-all).
    pub require_auth: bool,
}

/// MySQL Native Password authentication plugin (SHA1-based).
#[derive(Debug, Clone, Copy)]
pub struct MysqlNativePassword;

impl MysqlNativePassword {
    /// Hash a password alongside random challenge data per the MySQL
    /// [secure password authentication algorithm][0].
    ///
    /// ```text
    /// SHA1(password) XOR SHA1("20-bytes random data from server" <concat> SHA1(SHA1(password)))
    /// ```
    ///
    /// [0]: https://web.archive.org/web/20210616065935/https://dev.mysql.com/doc/internals/en/secure-password-authentication.html
    pub fn hash_password(&self, password: &[u8], auth_data: &AuthData) -> [u8; 20] {
        let mut res = sha1(password);
        let mut salted = [0u8; 40];
        salted[..20].copy_from_slice(auth_data);
        salted[20..].copy_from_slice(&sha1(&res));
        xor_slice_mut(&mut res, &sha1(&salted));
        res
    }
}

/// Caching SHA-2 Password authentication plugin (SHA256-based).
#[derive(Debug, Clone, Copy)]
pub struct CachingSha2Password;

impl CachingSha2Password {
    /// Compute the double-SHA256 digest used for the fast-auth cache.
    ///
    /// ```text
    /// SHA256(SHA256(password))
    /// ```
    pub fn generate_fast_digest(password: &[u8]) -> [u8; 32] {
        let digest1 = sha256(password);
        sha256(&digest1)
    }

    /// Decrypt an RSA-OAEP-encrypted password received from the client during
    /// the full-auth exchange.
    ///
    /// The client XORs the password (NUL-terminated) with `auth_data` before
    /// encrypting, so after decryption we XOR again to recover the original.
    ///
    /// # Errors
    ///
    /// * [`MsqlSrvError::DecryptionError`] if RSA decryption fails.
    pub fn decrypt_password(
        encrypted: &[u8],
        auth_data: &AuthData,
        keys: &AuthKeys,
    ) -> Result<Vec<u8>, MsqlSrvError> {
        let decrypted = keys
            .private_key()
            .decrypt(Oaep::new::<Sha1>(), encrypted)
            .map_err(|e| MsqlSrvError::DecryptionError(e.to_string()))?;

        Ok(xor_slice_modulus(&decrypted, auth_data))
    }
}

/// Generate 20 random bytes of auth data for use as auth challenge data.
pub fn generate_auth_data() -> Result<AuthData, MsqlSrvError> {
    let mut buf = [0u8; 20];
    match fill(&mut buf) {
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

/// Hash a password alongside random challenge data per the mysql secure
/// password authentication algorithm.
///
/// Delegates to [`MysqlNativePassword::hash_password`].
pub fn hash_password(password: &[u8], auth_data: &AuthData) -> [u8; 20] {
    MysqlNativePassword.hash_password(password, auth_data)
}

/// XOR `b1` with `b2`, cycling `b2` if it is shorter than `b1`.
fn xor_slice_modulus(b1: &[u8], b2: &[u8]) -> Vec<u8> {
    if b2.is_empty() {
        return b1.to_vec();
    }
    b1.iter()
        .enumerate()
        .map(|(i, &val)| val ^ b2[i % b2.len()])
        .collect()
}

/// Bytewise-XOR `b1` with `b2` in-place.
fn xor_slice_mut<const N: usize>(b1: &mut [u8; N], b2: &[u8; N]) {
    b1.iter_mut().zip(b2.iter()).for_each(|(x, y)| *x ^= y);
}

fn sha1(input: &[u8]) -> [u8; 20] {
    let mut hasher = Sha1::new();
    hasher.update(input);
    hasher.finalize().into()
}

fn sha256(input: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(input);
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use rsa::RsaPublicKey;

    use super::*;

    #[test]
    fn hash_native_password_works() {
        let auth_data: AuthData = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0];
        let password = b"password";
        let plugin = MysqlNativePassword;
        let result = plugin.hash_password(password, &auth_data);
        assert_eq!(
            result,
            [
                98, 3, 19, 63, 63, 49, 91, 179, 27, 253, 105, 140, 3, 177, 140, 44, 225, 127, 86,
                219
            ]
        );
    }

    #[test]
    fn hash_password_compat() {
        let auth_data: AuthData = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0];
        let password = b"password";
        assert_eq!(
            hash_password(password, &auth_data),
            MysqlNativePassword.hash_password(password, &auth_data)
        );
    }

    #[test]
    fn sha256_known_vector() {
        let result = sha256(b"password");
        assert_eq!(
            result,
            [
                0x5e, 0x88, 0x48, 0x98, 0xda, 0x28, 0x04, 0x71, 0x51, 0xd0, 0xe5, 0x6f, 0x8d, 0xc6,
                0x29, 0x27, 0x73, 0x60, 0x3d, 0x0d, 0x6a, 0xab, 0xbd, 0xd6, 0x2a, 0x11, 0xef, 0x72,
                0x1d, 0x15, 0x42, 0xd8
            ]
        );
    }

    #[test]
    fn fast_digest_known_vector() {
        let digest = CachingSha2Password::generate_fast_digest(b"password");
        assert_eq!(
            digest,
            [
                0x73, 0x64, 0x1c, 0x99, 0xf7, 0x71, 0x9f, 0x57, 0xd8, 0xf4, 0xbe, 0xb1, 0x1a, 0x30,
                0x3a, 0xfc, 0xd1, 0x90, 0x24, 0x3a, 0x51, 0xce, 0xd8, 0x78, 0x2c, 0xa6, 0xd3, 0xdb,
                0xe0, 0x14, 0xd1, 0x46
            ]
        );
    }

    #[test]
    fn xor_slice_modulus_works() {
        let a = [1u8, 2, 3, 4];
        let b = [0xffu8, 0xff];
        let result = xor_slice_modulus(&a, &b);
        assert_eq!(result, vec![0xfe, 0xfd, 0xfc, 0xfb]);
    }

    #[test]
    fn auth_cache_insert_and_check() {
        let auth_cache = AuthCache::new();

        auth_cache.insert("readyset", b"test");

        // Known-good scramble and auth_data for password "test"
        let scramble = [
            0xf9, 0x84, 0xa1, 0x9d, 0x9b, 0xa5, 0xef, 0x09, 0x61, 0x2d, 0xe0, 0x48, 0xe4, 0x88,
            0xfa, 0xa6, 0x38, 0x03, 0xd6, 0x51, 0x57, 0x13, 0x99, 0x59, 0x33, 0x9d, 0x86, 0x8e,
            0xf1, 0x31, 0x81, 0x9e,
        ];
        let auth_data: AuthData = [
            0x15, 0x2d, 0x62, 0x01, 0x34, 0x1d, 0x68, 0x47, 0x14, 0x60, 0x19, 0x4c, 0x73, 0x23,
            0x63, 0x75, 0x1b, 0x64, 0x28, 0x4e,
        ];

        assert!(auth_cache.check("readyset", &scramble, &auth_data));
        assert!(!auth_cache.check("wrong_user", &scramble, &auth_data));
    }

    #[test]
    fn auth_cache_wrong_password_fails() {
        let auth_cache = AuthCache::new();

        // Cache "noria" but scramble is for "test"
        auth_cache.insert("readyset", b"noria");

        let scramble = [
            0xf9, 0x84, 0xa1, 0x9d, 0x9b, 0xa5, 0xef, 0x09, 0x61, 0x2d, 0xe0, 0x48, 0xe4, 0x88,
            0xfa, 0xa6, 0x38, 0x03, 0xd6, 0x51, 0x57, 0x13, 0x99, 0x59, 0x33, 0x9d, 0x86, 0x8e,
            0xf1, 0x31, 0x81, 0x9e,
        ];
        let auth_data: AuthData = [
            0x15, 0x2d, 0x62, 0x01, 0x34, 0x1d, 0x68, 0x47, 0x14, 0x60, 0x19, 0x4c, 0x73, 0x23,
            0x63, 0x75, 0x1b, 0x64, 0x28, 0x4e,
        ];

        assert!(!auth_cache.check("readyset", &scramble, &auth_data));
    }

    #[test]
    fn auth_keys_persist_and_reload() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let priv_path = temp_dir.path().join(RSA_PRIVATE_KEY_FILE);
        let pub_path = temp_dir.path().join(RSA_PUBLIC_KEY_FILE);

        let keys1 = AuthKeys::load_or_create_keys(temp_dir.path().to_path_buf())
            .expect("first load_or_create_keys");
        assert!(priv_path.exists());
        assert!(pub_path.exists());
        let pem1 = keys1.public_key_pem().to_string();

        let keys2 = AuthKeys::load_or_create_keys(temp_dir.path().to_path_buf())
            .expect("second load_or_create_keys");
        assert_eq!(pem1, keys2.public_key_pem(), "reloaded key should match");
    }

    #[test]
    fn auth_keys_preexisting_pem() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let priv_path = temp_dir.path().join(RSA_PRIVATE_KEY_FILE);

        // Generate key externally and place only the private key
        let mut rng = rsa::rand_core::OsRng;
        let private_key = RsaPrivateKey::new(&mut rng, 2048).expect("generate RSA");
        let pem = private_key
            .to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
            .expect("PEM encode");
        fs::write(&priv_path, pem.as_bytes()).expect("write PEM");

        let expected_pub = private_key
            .to_public_key()
            .to_public_key_pem(rsa::pkcs8::LineEnding::LF)
            .expect("pub PEM");

        let keys = AuthKeys::load_or_create_keys(temp_dir.path().to_path_buf()).expect("load keys");
        assert_eq!(keys.public_key_pem(), expected_pub);
    }

    #[test]
    fn decrypt_password_roundtrip() {
        let mut rng = rsa::rand_core::OsRng;
        let private_key = RsaPrivateKey::new(&mut rng, 2048).expect("RSA generation");
        let public_key = RsaPublicKey::from(&private_key);
        let keys = AuthKeys::from_private_key(private_key).expect("AuthKeys");

        let auth_data: AuthData = [
            0x15, 0x2d, 0x62, 0x01, 0x34, 0x1d, 0x68, 0x47, 0x14, 0x60, 0x19, 0x4c, 0x73, 0x23,
            0x63, 0x75, 0x1b, 0x64, 0x28, 0x4e,
        ];
        let password = b"test_password\0";

        // Client side: XOR password with auth_data, then RSA encrypt
        let xored = xor_slice_modulus(password, &auth_data);
        let encrypted = public_key
            .encrypt(&mut rng, Oaep::new::<Sha1>(), &xored)
            .expect("RSA encrypt");

        // Server side: decrypt
        let recovered =
            CachingSha2Password::decrypt_password(&encrypted, &auth_data, &keys).expect("decrypt");

        assert_eq!(recovered.as_slice(), password);
    }

    #[test]
    fn auth_plugin_from_str() {
        assert!(matches!(
            AuthPlugin::from_str("mysql_native_password"),
            Ok(AuthPlugin::Native(_))
        ));
        assert!(matches!(
            AuthPlugin::from_str("caching_sha2_password"),
            Ok(AuthPlugin::Sha2(_))
        ));
        assert!(AuthPlugin::from_str("unknown").is_err());
    }
}
