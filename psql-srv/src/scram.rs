//! Server protocol implementation of the SCRAM SASL mechanism, described in [RFC5802][rfc5802]
//!
//! [rfc5802]: https://www.rfc-editor.org/rfc/rfc5802

use std::borrow::Cow;
use std::fmt::{self, Display};
use std::str::{self, Utf8Error};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use hmac::digest::FixedOutput;
use hmac::{Hmac, Mac};
use rand::Rng;
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const SCRAM_SHA_256_AUTHENTICATION_METHOD: &str = "SCRAM-SHA-256";
pub const SCRAM_SHA_256_SSL_AUTHENTICATION_METHOD: &str = "SCRAM-SHA-256-PLUS";

/// Iteration count to use for SCRAM. This is the default value that postgresql uses, but is likely
/// too low (TODO: make this configurable!)
pub const SCRAM_ITERATION_COUNT: u32 = 4096;
const NONCE_LENGTH: usize = 24;
const SALT_LENGTH: usize = 12;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error parsing SCRAM message")]
    ParseFail,

    #[error("Invalid escape sequence in username")]
    InvalidEscapeSequence,

    #[error("Invalid channel binding data")]
    InvalidChannelBindingData,

    #[error(transparent)]
    Utf8(#[from] Utf8Error),

    #[error(transparent)]
    Base64Decode(#[from] base64::DecodeError),

    #[error(transparent)]
    HmacDigestLength(#[from] hmac::digest::InvalidLength),
}

pub type Result<T> = std::result::Result<T, Error>;

/// SCRAM message attributes, described in chapter 5.1 of [RFC5802][rfc5802]
///
/// [rfc5802]: https://www.rfc-editor.org/rfc/rfc5802
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScramMessageAttribute {
    /// > a: This is an optional attribute, and is part of the GS2 [[RFC5801][rfc5801]] bridge
    /// > between the GSS-API and SASL.  This attribute specifies an authorization identity.  A
    /// > client may include it in its first message to the server if it wants to authenticate as
    /// > one user, but subsequently act as a different user.
    ///
    /// [rfc5801]: https://www.rfc-editor.org/rfc/rfc5801
    AuthorizationIdentity,

    /// > n: This attribute specifies the name of the user whose password is used for
    /// > authentication
    /// > (a.k.a. "authentication identity" ([RFC4422][rfc4422])).
    ///
    /// [rfc4422]: https://www.rfc-editor.org/rfc/rfc4422
    UserName,

    /// > m: This attribute is reserved for future extensibility.  In this version of SCRAM, its
    /// > presence in a client or a server message MUST cause authentication failure when the
    /// > attribute is parsed by the other end.
    ReservedMext,

    /// > r: This attribute specifies a sequence of random printable ASCII characters excluding ','
    /// (which forms the nonce used as input to the hash function).
    Nonce,

    /// > c: This REQUIRED attribute specifies the base64-encoded GS2 header and channel binding
    /// > data.
    Gs2Header,

    /// > s: This attribute specifies the base64-encoded salt used by the server for this user.
    Salt,

    /// > p: This attribute specifies a base64-encoded ClientProof.
    ClientProof,

    /// > v: This attribute specifies a base64-encoded ServerSignature.
    ServerSignature,

    /// > e: This attribute specifies an error that occurred during authentication exchange.
    ErrorOccurred,

    /// Unknown attribute
    Unknown(u8),
}

impl From<u8> for ScramMessageAttribute {
    fn from(value: u8) -> Self {
        use ScramMessageAttribute::*;

        match value {
            b'a' => AuthorizationIdentity,
            b'n' => UserName,
            b'm' => ReservedMext,
            b'r' => Nonce,
            b'c' => Gs2Header,
            b's' => Salt,
            b'p' => ClientProof,
            b'v' => ServerSignature,
            b'e' => ErrorOccurred,
            _ => Unknown(value),
        }
    }
}

/// Flag specified by the client indicating its support or requirement for channel binding
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientChannelBindingSupport<'a> {
    /// Client requires channel binding, using the given channel binding name
    Required(&'a str),
    /// Client doesn't support channel binding
    NotSupported,
    /// Client supports channel binding, but thinks the server does not
    SupportedButNotUsed,
}

impl<'a> ClientChannelBindingSupport<'a> {
    /// Returns `true` if the client channel binding support is [`SupportedButNotUsed`].
    ///
    /// [`SupportedButNotUsed`]: ClientChannelBindingSupport::SupportedButNotUsed
    #[must_use]
    pub fn is_supported_but_not_used(&self) -> bool {
        matches!(self, Self::SupportedButNotUsed)
    }

    /// Returns `true` if the client channel binding support is [`Required`].
    ///
    /// [`Required`]: ClientChannelBindingSupport::Required
    #[must_use]
    pub fn is_required(&self) -> bool {
        matches!(self, Self::Required(..))
    }
}

fn unescape_username(username: &str) -> Result<Cow<str>> {
    let mut equals_sign_positions = username
        .chars()
        .enumerate()
        .filter(|(_, c)| *c == '=')
        .map(|(p, _)| p)
        .peekable();

    if equals_sign_positions.peek().is_none() {
        return Ok(Cow::Borrowed(username));
    }

    let mut res = String::with_capacity(username.len());
    let mut last_pos = 0;
    for pos in equals_sign_positions {
        res.push_str(&username[last_pos..pos]);

        let escape = username
            .get(pos..(pos + 3))
            .ok_or(Error::InvalidEscapeSequence)?;
        match escape {
            "=2C" => res.push(','),
            "=3D" => res.push('='),
            _ => return Err(Error::InvalidEscapeSequence),
        }

        last_pos = pos + 3;
    }

    res.push_str(&username[last_pos..username.len()]);

    Ok(Cow::Owned(res))
}

mod parse {
    use nom::branch::alt;
    use nom::bytes::complete::{tag, take, take_till};
    use nom::combinator::{all_consuming, map, map_res, opt, peek, recognize, rest, value, verify};
    use nom::multi::many0;
    use nom::sequence::{preceded, separated_pair, terminated};
    use nom::IResult;

    use super::*;

    fn client_channel_binding_support(i: &[u8]) -> IResult<&[u8], ClientChannelBindingSupport> {
        use ClientChannelBindingSupport::*;

        alt((
            value(SupportedButNotUsed, tag("y")),
            value(NotSupported, tag("n")),
            map(
                preceded(tag("p="), map_res(take_till(|c| c == b','), str::from_utf8)),
                Required,
            ),
        ))(i)
    }

    fn attribute_name(i: &[u8]) -> IResult<&[u8], ScramMessageAttribute> {
        map(take(1usize), |c: &[u8]| ScramMessageAttribute::from(c[0]))(i)
    }

    fn attribute_pair(i: &[u8]) -> IResult<&[u8], (ScramMessageAttribute, &[u8])> {
        separated_pair(attribute_name, tag("="), take_till(|c| c == b','))(i)
    }

    fn specific_attribute(attr: ScramMessageAttribute) -> impl Fn(&[u8]) -> IResult<&[u8], &[u8]> {
        move |i| map(verify(attribute_pair, |(k, _)| *k == attr), |(_, v)| v)(i)
    }

    fn gs2_header(i: &[u8]) -> IResult<&[u8], Gs2Header> {
        let (i, channel_binding_support) = client_channel_binding_support(i)?;
        let (i, _) = tag(",")(i)?;
        let (i, authzid) = opt(map_res(
            specific_attribute(ScramMessageAttribute::AuthorizationIdentity),
            |v| unescape_username(str::from_utf8(v)?),
        ))(i)?;
        let (i, _) = tag(",")(i)?;

        Ok((
            i,
            Gs2Header {
                channel_binding_support,
                authzid,
            },
        ))
    }

    pub(super) fn client_first_message(i: &[u8]) -> IResult<&[u8], ClientFirstMessage> {
        all_consuming(|i| {
            let (i, gs2_header) = gs2_header(i)?;
            let (i, bare) = map_res(peek(rest), str::from_utf8)(i)?;
            let (i, _) = verify(
                opt(terminated(
                    specific_attribute(ScramMessageAttribute::ReservedMext),
                    tag(","),
                )),
                |v| v.is_none(),
            )(i)?;
            let (i, username) =
                map_res(specific_attribute(ScramMessageAttribute::UserName), |v| {
                    unescape_username(str::from_utf8(v)?)
                })(i)?;
            let (i, _) = tag(",")(i)?;
            let (i, nonce) = map_res(
                specific_attribute(ScramMessageAttribute::Nonce),
                str::from_utf8,
            )(i)?;
            let (i, extensions) = many0(preceded(tag(","), attribute_pair))(i)?;

            Ok((
                i,
                ClientFirstMessage {
                    gs2_header,
                    bare,
                    username,
                    nonce,
                    extensions,
                },
            ))
        })(i)
    }

    pub(super) fn client_final_message(i: &[u8]) -> IResult<&[u8], ClientFinalMessage> {
        all_consuming(|i| {
            let parse_without_proof = |i| {
                let (i, cbind_input_base64) = map_res(
                    specific_attribute(ScramMessageAttribute::Gs2Header),
                    str::from_utf8,
                )(i)?;
                let (i, _) = tag(",")(i)?;
                let (i, nonce) = map_res(
                    specific_attribute(ScramMessageAttribute::Nonce),
                    str::from_utf8,
                )(i)?;
                let (i, extensions) = many0(preceded(
                    tag(","),
                    verify(attribute_pair, |(k, _)| {
                        *k != ScramMessageAttribute::ClientProof
                    }),
                ))(i)?;

                Ok((i, (cbind_input_base64, nonce, extensions)))
            };

            let (i, without_proof) =
                map_res(peek(recognize(parse_without_proof)), str::from_utf8)(i)?;
            let (i, (cbind_input_base64, nonce, extensions)) = parse_without_proof(i)?;
            let (i, _) = tag(",")(i)?;
            let (i, proof_base64) = map_res(
                specific_attribute(ScramMessageAttribute::ClientProof),
                str::from_utf8,
            )(i)?;

            Ok((
                i,
                ClientFinalMessage {
                    cbind_input_base64,
                    nonce,
                    extensions,
                    without_proof,
                    proof_base64,
                },
            ))
        })(i)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Gs2Header<'a> {
    channel_binding_support: ClientChannelBindingSupport<'a>,
    authzid: Option<Cow<'a, str>>,
}

/// First message sent by the client as part of the SCRAM authentication flow
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientFirstMessage<'a> {
    gs2_header: Gs2Header<'a>,
    bare: &'a str,
    /// Username specified by the client. Note that for postgresql authentication this is always
    /// ignored!
    username: Cow<'a, str>,
    nonce: &'a str,
    extensions: Vec<(ScramMessageAttribute, &'a [u8])>,
}

impl<'a> ClientFirstMessage<'a> {
    pub fn parse(input: &'a [u8]) -> Result<Self> {
        match parse::client_first_message(input) {
            Ok((_, res)) => Ok(res),
            Err(_) => Err(Error::ParseFail),
        }
    }

    pub fn channel_binding_support(&self) -> ClientChannelBindingSupport<'a> {
        self.gs2_header.channel_binding_support
    }

    /// Returns a reference to the `client-first-message-bare` (see the RFC) for this
    /// client-first-message
    pub fn bare(&self) -> &str {
        self.bare
    }
}

/// Final message sent by the client as part of the SCRAM authentication flow
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientFinalMessage<'a> {
    cbind_input_base64: &'a str,
    nonce: &'a str,
    extensions: Vec<(ScramMessageAttribute, &'a [u8])>,
    without_proof: &'a str,
    proof_base64: &'a str,
}

impl<'a> ClientFinalMessage<'a> {
    pub fn parse(input: &'a [u8]) -> Result<Self> {
        match parse::client_final_message(input) {
            Ok((_, res)) => Ok(res),
            Err(_) => Err(Error::ParseFail),
        }
    }

    /// Verify the client proof supplied as part of this client-final-message, against the given
    /// data from the rest of the SCRAM authentication flow. Returns `Some(ServerFinalMessage)` if
    /// verification succeeds, or `None` if authentication has failed
    pub fn verify(
        &self,
        salted_password: &[u8],
        client_first_message_bare: &str,
        server_first_message: &str,
        expected_channel_binding_data: Option<&[u8]>,
    ) -> Result<Option<ServerFinalMessage>> {
        if let Some(expected_channel_binding_data) = expected_channel_binding_data {
            const CBIND_INPUT_GS2_HEADER: &[u8] = b"p=tls-server-end-point,,";
            let cbind_input = BASE64.decode(self.cbind_input_base64)?;

            // cbind_input should be "p=tls-server-end-point,," + expected_channel_binding_data
            if cbind_input.len() < CBIND_INPUT_GS2_HEADER.len()
                || &cbind_input[..CBIND_INPUT_GS2_HEADER.len()] != CBIND_INPUT_GS2_HEADER
                || &cbind_input[CBIND_INPUT_GS2_HEADER.len()..] != expected_channel_binding_data
            {
                return Err(Error::InvalidChannelBindingData);
            }
        }

        let mut hmac = Hmac::<Sha256>::new_from_slice(salted_password)?;
        hmac.update(b"Client Key");
        let expected_client_key = hmac.finalize().into_bytes();

        let mut hash = Sha256::default();
        hash.update(expected_client_key);
        let expected_stored_key = hash.finalize_fixed();

        let auth_message = format!(
            "{client_first_message_bare},{server_first_message},{}",
            self.without_proof
        );

        let mut hmac = Hmac::<Sha256>::new_from_slice(&expected_stored_key)?;
        hmac.update(auth_message.as_bytes());
        let client_signature = hmac.finalize().into_bytes();

        let client_proof = BASE64.decode(self.proof_base64)?;
        let mut client_key = client_signature;
        for (key, proof) in client_key.iter_mut().zip(client_proof) {
            *key ^= proof
        }

        let mut hash = Sha256::default();
        hash.update(client_key);
        let stored_key = hash.finalize_fixed();

        if stored_key != expected_stored_key {
            return Ok(None);
        }

        let mut hmac = Hmac::<Sha256>::new_from_slice(salted_password)?;
        hmac.update(b"Server Key");
        let server_key = hmac.finalize().into_bytes();

        let mut hmac = Hmac::<Sha256>::new_from_slice(&server_key)?;
        hmac.update(auth_message.as_bytes());
        let server_signature = hmac.finalize().into_bytes();

        Ok(Some(ServerFinalMessage {
            signature: server_signature.as_slice().to_owned(),
        }))
    }
}

// Mostly adapted from the (private) function of the same name in postgres_protocol::sasl
//
// This name is taken from the RFC - see that for more information
fn normalize(pass: &[u8]) -> Cow<[u8]> {
    let pass = match str::from_utf8(pass) {
        Ok(pass) => pass,
        Err(_) => return pass.into(),
    };

    match stringprep::saslprep(pass) {
        Ok(Cow::Borrowed(pass)) => pass.as_bytes().into(),
        Ok(Cow::Owned(pass)) => pass.into_bytes().into(),
        Err(_) => pass.as_bytes().into(),
    }
}

// Mostly adapted from the (private) function of the same name in postgres_protocol::sasl
//
// This name is taken from the RFC - see that for more information
fn hi(str: &[u8], salt: &[u8], i: u32) -> Result<[u8; 32]> {
    let mut hmac = Hmac::<Sha256>::new_from_slice(str)?;
    hmac.update(salt);
    hmac.update(&[0, 0, 0, 1]);
    let mut prev = hmac.finalize().into_bytes();

    let mut hi = prev;

    for _ in 1..i {
        let mut hmac = Hmac::<Sha256>::new_from_slice(str).expect("already checked above");
        hmac.update(&prev);
        prev = hmac.finalize().into_bytes();

        for (hi, prev) in hi.iter_mut().zip(prev) {
            *hi ^= prev;
        }
    }

    Ok(hi.into())
}

/// First message sent by the server as part of the SCRAM authentication flow
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerFirstMessage {
    nonce: String,
    salt: [u8; SALT_LENGTH],
    salted_password: [u8; 32],
}

impl ServerFirstMessage {
    pub fn new(client_first_message: ClientFirstMessage, password: &[u8]) -> Result<Self> {
        // rand 0.5's ThreadRng is cryptographically secure
        let mut rng = rand::thread_rng();
        let server_nonce = (0..NONCE_LENGTH)
            .map(|_| {
                let mut v = rng.gen_range(0x21u8..0x7e);
                if v == 0x2c {
                    v = 0x7e
                }
                v as char
            })
            .collect::<String>();

        let nonce = format!("{}{}", client_first_message.nonce, server_nonce);
        let mut salt = [0u8; SALT_LENGTH];
        rng.fill(&mut salt);
        let salted_password = hi(&normalize(password), &salt, SCRAM_ITERATION_COUNT)?;

        Ok(Self {
            nonce,
            salt,
            salted_password,
        })
    }

    pub fn salted_password(&self) -> &[u8] {
        &self.salted_password
    }
}

impl Display for ServerFirstMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "r={},s={},i={}",
            self.nonce,
            BASE64.encode(self.salt),
            SCRAM_ITERATION_COUNT,
        )
    }
}

/// Final message sent by the server as part of the SCRAM authentication flow
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerFinalMessage {
    signature: Vec<u8>,
}

impl Display for ServerFinalMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v={}", BASE64.encode(&self.signature))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unescape_username {
        use super::*;

        #[test]
        fn without_escapes() {
            assert_eq!(unescape_username("asdf").unwrap(), "asdf");
        }

        #[test]
        fn with_escapes() {
            assert_eq!(
                unescape_username("asdf=2Cadsf=3Djkjk").unwrap(),
                "asdf,adsf=jkjk"
            );
        }

        #[test]
        fn with_invalid_escapes() {
            unescape_username("asdf=").unwrap_err();
        }
    }

    mod client_first_message {
        use super::*;

        #[test]
        fn rfc_example() {
            assert_eq!(
                ClientFirstMessage::parse(b"n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL").unwrap(),
                ClientFirstMessage {
                    gs2_header: Gs2Header {
                        channel_binding_support: ClientChannelBindingSupport::NotSupported,
                        authzid: None
                    },
                    bare: "n=user,r=fyko+d2lbbFgONRv9qkxdawL",
                    username: "user".into(),
                    nonce: "fyko+d2lbbFgONRv9qkxdawL",
                    extensions: vec![]
                }
            )
        }

        #[test]
        fn authzid_and_channel_binding_supported() {
            assert_eq!(
                ClientFirstMessage::parse(b"y,a=other user,n=user,r=abcdef=hijk").unwrap(),
                ClientFirstMessage {
                    gs2_header: Gs2Header {
                        channel_binding_support: ClientChannelBindingSupport::SupportedButNotUsed,
                        authzid: Some("other user".into())
                    },
                    bare: "n=user,r=abcdef=hijk",
                    username: "user".into(),
                    nonce: "abcdef=hijk",
                    extensions: vec![]
                }
            )
        }

        #[test]
        fn escaped_authzid_and_channel_binding_required() {
            assert_eq!(
                ClientFirstMessage::parse(b"p=tls-unique,a=one=2Ctwo,n=three=3Dfour,r=asdf")
                    .unwrap(),
                ClientFirstMessage {
                    gs2_header: Gs2Header {
                        channel_binding_support: ClientChannelBindingSupport::Required(
                            "tls-unique"
                        ),
                        authzid: Some("one,two".into())
                    },
                    bare: "n=three=3Dfour,r=asdf",
                    username: "three=four".into(),
                    nonce: "asdf",
                    extensions: vec![]
                }
            )
        }
    }

    mod client_final_message {
        use super::*;

        #[test]
        fn rfc_example() {
            let input = b"c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=";
            assert_eq!(
                ClientFinalMessage::parse(input).unwrap(),
                ClientFinalMessage {
                    cbind_input_base64: "biws",
                    nonce: "fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j",
                    extensions: vec![],
                    without_proof: "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j",
                    proof_base64: "v0X8v3Bz2T0CJGbJQyF0X+HI4Ts="
                }
            )
        }

        #[test]
        fn live_example() {
            let input = b"c=biws,r=J3~R0;ZEChu;M3*abhMXC=>}zSz'69=19O`:/\\P:^LKxE0Zc,p=AbJXx0UrYVVXkSKXSRa+6PD+myecApEVuJdTyK6N0MY=";
            assert_eq!(
                ClientFinalMessage::parse(input).unwrap(),
                ClientFinalMessage {
                    cbind_input_base64: "biws",
                    nonce: "J3~R0;ZEChu;M3*abhMXC=>}zSz'69=19O`:/\\P:^LKxE0Zc",
                    extensions: vec![],
                    without_proof: "c=biws,r=J3~R0;ZEChu;M3*abhMXC=>}zSz'69=19O`:/\\P:^LKxE0Zc",
                    proof_base64: "AbJXx0UrYVVXkSKXSRa+6PD+myecApEVuJdTyK6N0MY=",
                }
            )
        }
    }
}
