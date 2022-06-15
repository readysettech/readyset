//! Implementation of MySQL's [Secure Password Authentication][0] authentication method.
//!
//! The way the authentication scheme works:
//!
//! 1. The server sends 20-bytes of [random data](AuthData) along with the initial handshake packet
//! 2. The client returns a 20-byte random response based on the algorithm in [`hash_password`]
//! 3. The server runs the same algorithm, and checks the response against the result
//!
//! [0]: https://dev.mysql.com/doc/internals/en/secure-password-authentication.html

use getrandom::getrandom;
use sha1::{Digest, Sha1};

use crate::error::MsqlSrvError;

pub type AuthData = [u8; 20];

/// The name of the (currently only) supported auth plugin
pub const AUTH_PLUGIN_NAME: &str = "mysql_native_password";

/// Bytewise-XOR b1 with b2 in-place
fn xor_slice_mut<const N: usize>(b1: &mut [u8; N], b2: &[u8; N]) {
    b1.iter_mut().zip(b2.iter()).for_each(|(x, y)| *x ^= y);
}

/// Generate 20 random bytes of auth data for use as auth challenge data (see step 1 in the module
/// level documentation)
pub fn generate_auth_data() -> Result<AuthData, MsqlSrvError> {
    let mut buf = [0u8; 20];
    match getrandom(&mut buf) {
        Ok(_) => Ok(buf),
        Err(_) => Err(MsqlSrvError::GetRandomError),
    }
}

fn sha1(input: &[u8]) -> [u8; 20] {
    let mut hasher = Sha1::new();
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
pub fn hash_password(password: &[u8], auth_data: &AuthData) -> [u8; 20] {
    let mut res = sha1(password);
    let mut salted = [0u8; 40];
    salted[..20].clone_from_slice(auth_data);
    salted[20..].clone_from_slice(&sha1(&res));
    xor_slice_mut(&mut res, &sha1(&salted));
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_password_works() {
        let auth_data: AuthData = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0];
        let password = b"password";
        let result = hash_password(password, &auth_data);
        assert_eq!(
            result,
            [
                98, 3, 19, 63, 63, 49, 91, 179, 27, 253, 105, 140, 3, 177, 140, 44, 225, 127, 86,
                219
            ]
        );
    }
}
