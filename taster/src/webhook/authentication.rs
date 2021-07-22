use hex::FromHex;
use hmac::{Hmac, Mac, NewMac};
use sha1::Sha1;

pub fn authenticate_payload(secret: &[u8], payload: &[u8], signature: &[u8]) -> bool {
    // https://developer.github.com/webhooks/securing/#validating-payloads-from-github
    let sans_prefix = &signature[5..signature.len()];
    Vec::from_hex(sans_prefix).map_or(false, |sigbytes| {
        let sbytes = secret;
        let mut mac = Hmac::<Sha1>::new_from_slice(sbytes).unwrap();
        let pbytes = payload;
        mac.update(pbytes);
        mac.verify(&sigbytes).is_ok()
    })
}
