use std::str::FromStr;

use anyhow::bail;
use noria_data::DataType;

use crate::ColumnGenerationSpec;

/// An annotation for how to generate a parameter's value for a query. A
/// parameter annotation takes the following form:
///   <annotation type> <annotation type parameters>.
///
/// The annotation type indicates a general way of generating the parameter,
/// for example, `uniform` is a annotation type that may be used to generate
/// uniformly random values over a minimum and maximum value that can
/// be specified via the parameters, i.e. `uniform 4 100`.
pub struct DistributionAnnotation {
    pub spec: ColumnGenerationSpec,
    pub unique: bool,
}

impl FromStr for DistributionAnnotation {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut chunks = s.split_ascii_whitespace();

        let spec = match chunks.next().unwrap().to_ascii_lowercase().as_str() {
            "uniform" => {
                let from: i64 = chunks.next().unwrap().parse().unwrap();
                let to: i64 = chunks.next().unwrap().parse().unwrap();
                ColumnGenerationSpec::Uniform(DataType::Int(from), DataType::Int(to))
            }
            "zipf" => {
                let from: i64 = chunks.next().unwrap().parse().unwrap();
                let to: i64 = chunks.next().unwrap().parse().unwrap();
                let alpha: f64 = chunks.next().unwrap().parse().unwrap();
                ColumnGenerationSpec::Zipfian {
                    min: DataType::Int(from),
                    max: DataType::Int(to),
                    alpha,
                }
            }
            "regex" => {
                let regex = chunks.next().unwrap().trim_matches('"');
                ColumnGenerationSpec::RandomString(regex.to_owned())
            }
            // Creates unique groups of size `num`.
            "group" => {
                let num: u32 = chunks.next().unwrap().parse().unwrap();
                ColumnGenerationSpec::UniqueRepeated(num)
            }
            _ => bail!("Unrecognized annotation"),
        };

        let unique = chunks.next().map(str::to_ascii_lowercase).as_deref() == Some("unique");

        Ok(Self { spec, unique })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_uniform_annotation_spec() {
        let q = "uniform 4 100";
        let s = q.parse::<DistributionAnnotation>().unwrap();
        assert!(matches!(
            s.spec,
            ColumnGenerationSpec::Uniform(DataType::Int(4), DataType::Int(100))
        ));
    }

    #[test]
    fn parse_uniform_annotation_spec_with_unique() {
        let q = "uniform 4 100 UNIQUE";
        let s = q.parse::<DistributionAnnotation>().unwrap();
        assert!(matches!(
            (s.spec, s.unique),
            (
                ColumnGenerationSpec::Uniform(DataType::Int(4), DataType::Int(100)),
                true
            )
        ));
    }
}
