use readyset_data::DfType;
use readyset_schema::bind_vrel;
use readyset_schema::virtual_relation::{VrelContext, VrelRead, VrelRows};

const USERS_SCHEMA: &[(&str, DfType)] = &[("user", DfType::DEFAULT_TEXT)];

/// Backs `SELECT * FROM readyset.users`: one row per allowed username. Passwords are never
/// exposed. Computed per query, so it reflects runtime `ALTER READYSET ADD|MODIFY|DROP USER`
/// mutations immediately.
fn users_read(ctx: &VrelContext) -> VrelRead {
    let mut usernames = ctx.users.usernames();
    Box::pin(async move {
        usernames.sort();
        let rows: VrelRows = Box::new(usernames.into_iter().map(|user| vec![user.into()]));
        Ok(rows)
    })
}
bind_vrel!(users, USERS_SCHEMA, users_read);
