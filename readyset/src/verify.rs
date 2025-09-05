use std::fmt::{Display, Formatter};

use anyhow::{anyhow, Error, Result};

use crate::Options;

/// A collection of all the verification failures we see.
#[derive(Debug, thiserror::Error)]
struct Errors(Vec<Error>);

impl Display for Errors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Config verification failed (override this check with --verify-skip):"
        )?;
        for (i, e) in self.0.iter().enumerate() {
            writeln!(f, "  {}: {e}", i + 1)?;
        }
        Ok(())
    }
}

/// Run through a list of checks to make sure we're good to go before attempting to snapshot.
pub async fn verify(options: &Options) -> Result<()> {
    let mut errors = Vec::new();
    macro_rules! add_err {
        ($result:expr) => {
            match $result {
                Ok(r) => Some(r),
                Err(e) => {
                    errors.push(e);
                    None
                }
            }
        };
    }

    if options.verify_fail {
        let e: Result<()> = Err(anyhow!("Simulated failure (caused by --verify-fail)"));
        add_err!(e);
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(Errors(errors).into())
    }
}
