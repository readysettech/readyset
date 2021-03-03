use crypto::hmac::Hmac;
use crypto::mac::Mac;
use crypto::mac::MacResult;
use crypto::sha1::Sha1;
use hex::FromHex;

pub fn authenticate_payload(secret: &[u8], payload: &[u8], signature: &[u8]) -> bool {
    // https://developer.github.com/webhooks/securing/#validating-payloads-from-github
    let sans_prefix = &signature[5..signature.len()];
    Vec::from_hex(sans_prefix).map_or(false, |sigbytes| {
        let sbytes = secret;
        let mut mac = Hmac::new(Sha1::new(), &sbytes);
        let pbytes = payload;
        mac.input(&pbytes);
        mac.result() == MacResult::new(&sigbytes)
    })
}
