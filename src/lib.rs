#[macro_use]
extern crate nom;

const U24_MAX: usize = 16777215;

named!(
    fullpacket<(u8, &[u8])>,
    do_parse!(
        tag!(&[0xff, 0xff, 0xff]) >> seq: take!(1) >> bytes: take!(U24_MAX) >> (seq[0], bytes)
    )
);

named!(
    onepacket<(u8, &[u8])>,
    map!(length_bytes!(map!(nom::le_u24, |i| i + 1)), |b| {
        (b[0], &b[1..])
    })
);

named!(
    pub packet<(u8, Vec<&[u8]>)>,
    do_parse!(
        full:
            fold_many0!(
                fullpacket,
                (0, Vec::new()),
                |(seq, mut bufs): (u8, Vec<_>), (nseq, buf)| {
                    if !bufs.is_empty() {
                        assert_eq!(nseq, seq + 1);
                    }
                    bufs.push(buf);
                    (nseq, bufs)
                }
            ) >> last: onepacket >> ({
            if !full.1.is_empty() {
                assert_eq!(last.0, full.0 + 1);
            }
            let mut buffs = full.1;
            let seq = last.0;
            if !last.1.is_empty() {
                buffs.push(last.1);
            }
            (seq, buffs)
        })
    )
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_one_ping() {
        assert_eq!(
            onepacket(&[0x01, 0, 0, 0, 0x10]).unwrap().1,
            (0, &[0x10][..])
        );
    }

    #[test]
    fn test_ping() {
        assert_eq!(
            packet(&[0x01, 0, 0, 0, 0x10]).unwrap().1,
            (0, vec![&[0x10][..]])
        );
    }

    #[test]
    fn test_long_exact() {
        let mut data = Vec::new();
        data.push(0xff);
        data.push(0xff);
        data.push(0xff);
        data.push(0);
        data.extend(&[0; U24_MAX][..]);
        data.push(0x00);
        data.push(0x00);
        data.push(0x00);
        data.push(1);

        let (rest, p) = packet(&data[..]).unwrap();
        assert!(rest.is_empty());
        assert_eq!(p.0, 1);
        assert_eq!(p.1.len(), 1);
        assert_eq!(p.1[0], &[0; U24_MAX][..]);
    }

    #[test]
    fn test_long_more() {
        let mut data = Vec::new();
        data.push(0xff);
        data.push(0xff);
        data.push(0xff);
        data.push(0);
        data.extend(&[0; U24_MAX][..]);
        data.push(0x01);
        data.push(0x00);
        data.push(0x00);
        data.push(1);
        data.push(0x10);

        let (rest, p) = packet(&data[..]).unwrap();
        assert!(rest.is_empty());
        assert_eq!(p.0, 1);
        assert_eq!(p.1.len(), 2);
        assert_eq!(p.1[0], &[0; U24_MAX][..]);
        assert_eq!(p.1[1], &[0x10][..]);
    }
}
