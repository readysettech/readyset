use nom::{alphanumeric, multispace};

use common::{
    integer_literal, opt_multispace, sql_identifier, string_literal,
};

named!(pub table_options<()>, do_parse!(
       separated_list_complete!(table_options_separator, create_option)
        >>
        (
            // TODO: make the create options accessible
            ()
        )
));

named!(table_options_separator<()>, do_parse!(
    alt_complete!(
        map!(multispace, |_| ()) |
        do_parse!(
            opt_multispace >>
            tag!(",") >>
            opt_multispace >>
            ()
        )
    ) >> ()
));

named!(create_option<()>, alt_complete!(
        create_option_type |
        create_option_pack_keys |
        create_option_engine |
        create_option_auto_increment |
        create_option_default_charset |
        create_option_collate |
        create_option_comment |
        create_option_max_rows |
        create_option_avg_row_length |
        create_option_row_format |
        create_option_key_block_size
));

named!(create_option_type<()>, complete!(
    do_parse!(
        tag_no_case!("type") >>
        opt_multispace >>
        tag!("=") >>
        opt_multispace >>
        alphanumeric >>
        ()
    )
));

named!(create_option_pack_keys<()>, complete!(
    do_parse!(
        tag_no_case!("pack_keys") >>
        opt_multispace >>
        tag!("=") >>
        opt_multispace >>
        alt_complete!(tag!("0") | tag!("1")) >>
        ()
    )
));

named!(create_option_engine<()>, complete!(
    do_parse!(
        tag_no_case!("engine") >>
        opt_multispace >>
        tag!("=") >>
        opt_multispace >>
        opt!(alphanumeric) >>
        ()
    )
));

named!(create_option_auto_increment<()>, complete!(
    do_parse!(
        tag_no_case!("auto_increment") >>
        opt_multispace >>
        tag!("=") >>
        opt_multispace >>
        integer_literal >>
        ()
    )
));

named!(create_option_default_charset<()>, complete!(
    do_parse!(
        tag_no_case!("default charset") >>
        opt_multispace >>
        tag!("=") >>
        opt_multispace >>
        alt_complete!(
            tag!("utf8mb4") |
            tag!("utf8") |
            tag!("binary") |
            tag!("big5") |
            tag!("ucs2") |
            tag!("latin1")
            ) >>
        ()
    )
));

named!(create_option_collate<()>, complete!(
    do_parse!(
        tag_no_case!("collate") >>
        opt_multispace >>
        tag!("=") >>
        opt_multispace >>
        // TODO(malte): imprecise hack, should not accept everything
        sql_identifier >>
        ()
    )
));

named!(create_option_comment<()>, complete!(
    do_parse!(
        tag_no_case!("comment") >>
        opt_multispace >>
        tag!("=") >>
        opt_multispace >>
        string_literal >>
        ()
    )
));

named!(create_option_max_rows<()>, complete!(
    do_parse!(
        tag_no_case!("max_rows") >>
        opt_multispace >>
        opt!(tag!("=")) >>
        opt_multispace >>
        integer_literal >>
        ()
    )
));

named!(create_option_avg_row_length<()>, complete!(
    do_parse!(
        tag_no_case!("avg_row_length") >>
        opt_multispace >>
        opt!(tag!("=")) >>
        opt_multispace >>
        integer_literal >>
        ()
    )
));

named!(create_option_row_format<()>, complete!(
    do_parse!(
        tag_no_case!("row_format") >>
        opt_multispace >>
        opt!(tag!("=")) >>
        opt_multispace >>
        alt_complete!(
            tag_no_case!("DEFAULT")|
            tag_no_case!("DYNAMIC") |
            tag_no_case!("FIXED") |
            tag_no_case!("COMPRESSED") |
            tag_no_case!("REDUNDANT") |
            tag_no_case!("COMPACT")
        ) >>
        ()
    )
));

named!(create_option_key_block_size<()>, complete!(
    do_parse!(
        tag_no_case!("key_block_size") >>
        opt_multispace >>
        opt!(tag!("=")) >>
        opt_multispace >>
        integer_literal >>
        ()
    )
));

#[cfg(test)]
mod tests {
    use super::*;
    use nom::IResult;

    fn should_parse_all(qstring: &str) {
        assert_eq!(
            IResult::Done(&b""[..], ()),
            table_options(qstring.as_bytes())
        )
    }

    #[test]
    fn create_table_option_list_empty() {
        should_parse_all("");
    }

    #[test]
    fn create_table_option_list() {
        should_parse_all("ENGINE=InnoDB AUTO_INCREMENT=44782967 \
        DEFAULT CHARSET=binary ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8");
    }

    #[test]
    fn create_table_option_list_commaseparated() {
        should_parse_all("AUTO_INCREMENT=1,ENGINE=,KEY_BLOCK_SIZE=8");
    }
}
