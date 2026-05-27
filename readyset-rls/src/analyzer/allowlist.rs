//! Hard-coded allowlist of functions the RLS analyzer accepts inside a policy expression. STABLE
//! volatility alone is not sufficient: a function must appear here by name, so the analyzer never
//! auto-promotes based on volatility.

use std::sync::LazyLock;

use crate::types::{FunctionMeta, SessionInputType};

/// Allowlisted GUC names callable through `current_setting(name)` /
/// `current_setting(name, true)`. Anything outside this list refuses.
pub const ALLOWED_GUCS: &[&str] = &[
    "request.jwt.claims",
    "request.jwt.claim.sub",
    "request.jwt.claim.role",
    "request.jwt.claim.email",
    "request.method",
    "request.path",
];

/// Allowlisted functions, schema-qualified to match how the parser presents function calls
/// (`schema.name(...)`).
///
/// JWT-claim reads carry their path segments; empty segments address the session GUC carrying the
/// raw JWT blob.
pub static ALLOWED_FUNCTIONS: LazyLock<[FunctionMeta; 6]> = LazyLock::new(|| {
    [
        FunctionMeta {
            qualified_name: "auth.uid",
            reads: Box::new([SessionInputType::jwt_claim(&["sub"])]),
        },
        FunctionMeta {
            qualified_name: "auth.jwt",
            reads: Box::new([SessionInputType::jwt_claim(&[])]),
        },
        FunctionMeta {
            qualified_name: "auth.role",
            reads: Box::new([SessionInputType::jwt_claim(&["role"])]),
        },
        FunctionMeta {
            qualified_name: "pg_catalog.current_user",
            reads: Box::new([]),
        },
        FunctionMeta {
            qualified_name: "pg_catalog.session_user",
            reads: Box::new([]),
        },
        FunctionMeta {
            qualified_name: "pg_catalog.current_setting",
            // Specific GUC validation happens at the call site.
            reads: Box::new([]),
        },
    ]
});

/// Look up an allowlisted function by schema-qualified name. Returns `None` for any function not
/// on the list, including STABLE ones: the analyzer must not auto-promote.
pub fn lookup_function(qualified_name: &str) -> Option<&'static FunctionMeta> {
    ALLOWED_FUNCTIONS
        .iter()
        .find(|f| f.qualified_name.eq_ignore_ascii_case(qualified_name))
}

/// `true` when `guc` is allowed as the argument to
/// `current_setting(guc[, missing_ok])`.
pub fn is_allowed_guc(guc: &str) -> bool {
    ALLOWED_GUCS
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(guc))
}
