use anyhow::{bail, Result};
use ipnet::Ipv4Net;
use iprange::IpRange;

/// Given the CIDR block for a VPC, and an iterator over CIDR blocks of subnets that *already* exist
/// in the VPC, return CIDR blocks for subnets to create such that `needed_total_subnets` would
/// exist in the VPC.
///
/// If insufficient network space exists in the VPC for the required number of subnets, returns an
/// error.
pub(crate) fn subnet_cidrs<I>(
    vpc_cidr: Ipv4Net,
    existing_subnet_cidrs: I,
    needed_total_subnets: usize,
) -> Result<Vec<Ipv4Net>>
where
    I: IntoIterator<Item = Ipv4Net>,
{
    // First, figure out the network space in the VPC that isn't taken up by existing subnets
    let mut num_existing = 0usize;
    let mut available = IpRange::new();
    available.add(vpc_cidr);
    for existing in existing_subnet_cidrs {
        available.remove(existing);
        num_existing += 1;
    }
    available.simplify();

    // Then add networks from that available space until we have the required number of total
    // subnets
    let num_needed = needed_total_subnets - num_existing;
    let needed_extra_bits = f32::log2(num_needed as _).ceil() as u8;
    let mut res = Vec::with_capacity(num_needed);
    for network in &available {
        if num_needed - res.len() == 1 {
            res.push(network);
            return Ok(res);
        }

        for subnet in network.subnets(network.prefix_len() + needed_extra_bits)? {
            if res.len() >= num_needed {
                return Ok(res);
            }
            res.push(subnet);
        }
    }

    bail!("Insufficient subnet space available in VPC")
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use std::{cmp::min, net::Ipv4Addr};
    use test_strategy::proptest;

    use super::*;

    #[test]
    fn subnet_cidrs_10_slash_16_none_existing() {
        let res = subnet_cidrs("10.0.0.0/16".parse().unwrap(), vec![], 3).unwrap();
        assert_eq!(
            res,
            vec![
                "10.0.0.0/18".parse().unwrap(),
                "10.0.64.0/18".parse().unwrap(),
                "10.0.128.0/18".parse().unwrap(),
            ]
        )
    }

    #[test]
    fn subnet_cidrs_10_slash_16_slash_18_existing() {
        let res = subnet_cidrs(
            "10.0.0.0/16".parse().unwrap(),
            vec!["10.0.0.0/18".parse().unwrap()],
            3,
        )
        .unwrap();
        assert_eq!(
            res,
            vec![
                "10.0.128.0/18".parse().unwrap(),
                "10.0.192.0/18".parse().unwrap(),
            ]
        )
    }

    #[test]
    fn subnet_cidrs_10_slash_16_first_two_slash_18_existing() {
        let res = subnet_cidrs(
            "10.0.0.0/16".parse().unwrap(),
            vec![
                "10.0.0.0/18".parse().unwrap(),
                "10.0.64.0/18".parse().unwrap(),
            ],
            3,
        )
        .unwrap();
        assert_eq!(res, vec!["10.0.128.0/17".parse().unwrap(),])
    }

    #[test]
    fn subnet_cidrs_10_slash_16_first_and_last_slash_18_existing() {
        let res = subnet_cidrs(
            "10.0.0.0/16".parse().unwrap(),
            vec![
                "10.0.0.0/18".parse().unwrap(),
                "10.0.128.0/18".parse().unwrap(),
            ],
            3,
        )
        .unwrap();
        assert_eq!(res, vec!["10.0.64.0/18".parse().unwrap(),])
    }

    #[test]
    fn subnet_cidrs_sufficient_existing() {
        let res = subnet_cidrs(
            "10.0.0.0/16".parse().unwrap(),
            vec![
                "10.0.0.0/18".parse().unwrap(),
                "10.0.64.0/18".parse().unwrap(),
                "10.0.128.0/18".parse().unwrap(),
            ],
            3,
        )
        .unwrap();
        assert_eq!(res, vec![])
    }

    #[derive(Debug)]
    struct ArbitraryIpv4Net(Ipv4Net);

    impl Arbitrary for ArbitraryIpv4Net {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
            any::<Ipv4Addr>()
                .prop_flat_map(|addr| {
                    let min_prefix_len = min(
                        32u8 - (u32::from(addr).trailing_zeros() as u8),
                        // at least /8, to make sure we've got room
                        8,
                    );
                    let max_prefix_len = 24u8;
                    (Just(addr), min_prefix_len..=max_prefix_len)
                        .prop_map(|(addr, len)| ArbitraryIpv4Net(Ipv4Net::new(addr, len).unwrap()))
                })
                .boxed()
        }
    }

    #[proptest]
    fn subnet_cidrs_returns_valid_subnets(
        vpc_cidr: ArbitraryIpv4Net,
        #[strategy(1usize..=8)] needed_total_subnets: usize,
    ) {
        let res = subnet_cidrs(vpc_cidr.0, vec![], needed_total_subnets);
        prop_assume!(res.is_ok(), "{}", res.err().unwrap());
        let subnets = res.unwrap();
        assert_eq!(subnets.len(), needed_total_subnets);
        for (i, subnet) in subnets.iter().enumerate() {
            for (_, other_subnet) in subnets.iter().enumerate().filter(|(j, _)| *j != i) {
                let first_address = subnet.hosts().next().unwrap();
                let last_address =
                    Ipv4Addr::from(u32::from(subnet.addr()) | u32::from(subnet.hostmask()));
                assert!(!other_subnet.contains(&first_address));
                assert!(!other_subnet.contains(&last_address));
            }
        }
    }
}
