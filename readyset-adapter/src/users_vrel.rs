use readyset_data::DfType;
use readyset_schema::bind_vrel;
use readyset_schema::virtual_relation::{VrelContext, VrelRead, VrelRows};

const USERS_SCHEMA: &[(&str, DfType)] = &[
    ("user", DfType::DEFAULT_TEXT),
    ("has_old_password", DfType::Bool),
];

fn users_read(ctx: &VrelContext) -> VrelRead {
    let mut users = ctx.users.users();
    Box::pin(async move {
        users.sort();
        let rows: VrelRows = Box::new(
            users
                .into_iter()
                .map(|(user, has_old_password)| vec![user.into(), has_old_password.into()]),
        );
        Ok(rows)
    })
}
bind_vrel!(users, USERS_SCHEMA, users_read);
