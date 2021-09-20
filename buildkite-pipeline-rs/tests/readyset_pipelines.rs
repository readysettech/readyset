use std::fs::File;
use std::path::PathBuf;

use serde_yaml as yaml;

use buildkite_pipeline::Pipeline;

fn open_pipeline(relative_path: &str) -> File {
    let mut path = PathBuf::from("..").canonicalize().unwrap();
    path.push(relative_path);
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
    buildkite_yaml("buildkite.yaml");
    buildkite_common_yaml(".buildkite/common.yaml");
    buildkite_nightly_yaml(".buildkite/nightly.yaml");
    buildkite_fuzz_yaml(".buildkite/fuzz.yaml");
}
