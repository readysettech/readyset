//! Constraint-based SQL query generator.
//!
//! This crate generates random SQL queries by declaring constraint patterns
//! and resolving them against a mutable schema state. The main entry point
//! is [`generator::Generator`].

pub mod compat;
pub mod constraint;
pub mod entropy;
pub mod generator;
pub mod pattern;
pub mod registry;
pub mod resolver;
pub mod state;
pub(crate) mod var;

// Re-export primary API types for convenience.
pub use generator::{ConstraintRegistry, DdlOutput, GenerateError, Generator, QueryOutput};
pub use resolver::{DdlStep, ParamMeta};
pub use state::{GenerationState, GeneratorConfig};

#[cfg(test)]
pub(crate) mod test_util {
    use rand::SeedableRng;
    use rand::rngs::SmallRng;
    use readyset_sql::{Dialect, DialectDisplay};

    use crate::entropy::Entropy;
    use crate::pattern::Pattern;
    use crate::resolver::resolve;
    use crate::state::{GenerationState, GeneratorConfig};

    pub fn resolve_pattern(pattern: &Pattern, dialect: Dialect) -> String {
        let config = GeneratorConfig {
            reuse_preference: 0.0,
            ..Default::default()
        };
        let mut state = GenerationState::new(dialect, config);
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);
        let output = resolve(
            &pattern.constraints,
            &pattern.vars,
            &mut state,
            &mut entropy,
        )
        .expect("should resolve");
        output.query.display(dialect).to_string()
    }
}
