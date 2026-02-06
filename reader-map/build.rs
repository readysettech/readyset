fn main() {
    test_discovery::generate_test_manifest(test_discovery::TestDiscoveryConfig {
        skip_stems: vec!["proptest".to_string()],
        ..Default::default()
    });
}
