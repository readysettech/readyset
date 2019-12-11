use nom::character::complete::{alphanumeric1, multispace0, multispace1};

use common::{
    integer_literal, sql_identifier, string_literal,
};

named!(pub table_options<&[u8], ()>, do_parse!(
       separated_list!(table_options_separator, create_option)
        >>
        (
            // TODO: make the create options accessible
            ()
        )
));

named!(table_options_separator<&[u8], ()>, do_parse!(
    alt!(
        map!(multispace1, |_| ()) |
        do_parse!(
            multispace0 >>
            tag!(",") >>
            multispace0 >>
            ()
        )
    ) >> ()
));

named!(create_option<&[u8], ()>, alt!(
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

named!(create_option_type<&[u8], ()>,
    do_parse!(
        tag_no_case!("type") >>
        multispace0 >>
        tag!("=") >>
        multispace0 >>
        alphanumeric1 >>
        ()
    )
);

named!(create_option_pack_keys<&[u8], ()>,
    do_parse!(
        tag_no_case!("pack_keys") >>
        multispace0 >>
        tag!("=") >>
        multispace0 >>
        alt!(tag!("0") | tag!("1")) >>
        ()
    )
);

named!(create_option_engine<&[u8], ()>,
    do_parse!(
        tag_no_case!("engine") >>
        multispace0 >>
        tag!("=") >>
        multispace0 >>
        opt!(alphanumeric1) >>
        ()
    )
);

named!(create_option_auto_increment<&[u8], ()>,
    do_parse!(
        tag_no_case!("auto_increment") >>
        multispace0 >>
        tag!("=") >>
        multispace0 >>
        integer_literal >>
        ()
    )
);

named!(create_option_default_charset<&[u8], ()>,
    do_parse!(
        tag_no_case!("default charset") >>
        multispace0 >>
        tag!("=") >>
        multispace0 >>
        alt!(
            tag!("utf8mb4") |
            tag!("utf8") |
            tag!("binary") |
            tag!("big5") |
            tag!("ucs2") |
            tag!("latin1")
            ) >>
        ()
    )
);

named!(create_option_collate<&[u8], ()>,
    do_parse!(
        tag_no_case!("collate") >>
        multispace0 >>
        tag!("=") >>
        multispace0 >>
        // TODO(malte): imprecise hack, should not accept everything
        sql_identifier >>
        ()
    )
);

named!(create_option_comment<&[u8], ()>,
    do_parse!(
        tag_no_case!("comment") >>
        multispace0 >>
        tag!("=") >>
        multispace0 >>
        string_literal >>
        ()
    )
);

named!(create_option_max_rows<&[u8], ()>,
    do_parse!(
        tag_no_case!("max_rows") >>
        multispace0 >>
        opt!(tag!("=")) >>
        multispace0 >>
        integer_literal >>
        ()
    )
);

named!(create_option_avg_row_length<&[u8], ()>,
    do_parse!(
        tag_no_case!("avg_row_length") >>
        multispace0 >>
        opt!(tag!("=")) >>
        multispace0 >>
        integer_literal >>
        ()
    )
);

named!(create_option_row_format<&[u8], ()>,
    do_parse!(
        tag_no_case!("row_format") >>
        multispace0 >>
        opt!(tag!("=")) >>
        multispace0 >>
        alt!(
            tag_no_case!("DEFAULT")|
            tag_no_case!("DYNAMIC") |
            tag_no_case!("FIXED") |
            tag_no_case!("COMPRESSED") |
            tag_no_case!("REDUNDANT") |
            tag_no_case!("COMPACT")
        ) >>
        ()
    )
);

named!(create_option_key_block_size<&[u8], ()>,
    do_parse!(
        tag_no_case!("key_block_size") >>
        multispace0 >>
        opt!(tag!("=")) >>
        multispace0 >>
        integer_literal >>
        ()
    )
);

#[cfg(test)]
mod tests {
    use super::*;

    fn should_parse_all(qstring: &str) {
        assert_eq!(
            Ok((&b""[..], ())),
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
