use std::fs::File;
use std::path::PathBuf;

use buildkite_pipeline::Pipeline;
use serde_yaml as yaml;

fn open_pipeline(relative_path: &str) -> File {
    let path = PathBuf::from(".")
        .canonicalize()
        .unwrap()
        .join("tests/fixtures")
        .join(relative_path);
    File::open(path).unwrap()
}

fn test_pipeline(relative_path: &str) {
    let pipeline_res = yaml::from_reader::<_, Pipeline>(open_pipeline(relative_path));
    assert!(pipeline_res.is_ok(), "{}", pipeline_res.err().unwrap());
}

macro_rules! pipeline_tests {
    () => {};
	($test_name: ident($path: expr); $($rest: tt)*) => {
		#[test]
        fn $test_name() {
            test_pipeline($path);
        }

        pipeline_tests!($($rest)*);
	};
}

pipeline_tests! {
    buildkite_yaml("pipeline.yml");
    buildkite_common_yaml("pipeline.common.yml");
    buildkite_nightly_yaml("pipeline.readyset-nightly.yml");
    buildkite_fuzz_yaml("pipeline.readyset-fuzz.yml");
}
