use vitess_grpc::query::{Field, MySqlFlag};

pub struct Column {
    pub name: String,
    pub grpc_type: vitess_grpc::query::Type,
    pub column_type: String,
    pub flags: u32,
}

impl Column {
    pub fn new(
        name: &str,
        grpc_type: vitess_grpc::query::Type,
        column_type: &str,
        flags: u32,
    ) -> Self {
        Self {
            name: name.to_string(),
            grpc_type,
            column_type: column_type.to_string(),
            flags,
        }
    }

    pub fn is_not_null(&self) -> bool {
        self.flags & MySqlFlag::NotNullFlag as u32 != 0
    }

    pub fn is_primary_key(&self) -> bool {
        self.flags & MySqlFlag::PriKeyFlag as u32 != 0
    }

    pub fn is_unique_key(&self) -> bool {
        self.flags & MySqlFlag::UniqueKeyFlag as u32 != 0
    }

    pub fn is_multiple_key(&self) -> bool {
        self.flags & MySqlFlag::MultipleKeyFlag as u32 != 0
    }

    pub fn is_blob(&self) -> bool {
        self.flags & MySqlFlag::BlobFlag as u32 != 0
    }

    pub fn is_unsigned(&self) -> bool {
        self.flags & MySqlFlag::UnsignedFlag as u32 != 0
    }

    pub fn is_zerofill(&self) -> bool {
        self.flags & MySqlFlag::ZerofillFlag as u32 != 0
    }

    pub fn is_binary(&self) -> bool {
        self.flags & MySqlFlag::BinaryFlag as u32 != 0
    }

    pub fn is_enum(&self) -> bool {
        self.flags & MySqlFlag::EnumFlag as u32 != 0
    }

    pub fn is_auto_increment(&self) -> bool {
        self.flags & MySqlFlag::AutoIncrementFlag as u32 != 0
    }

    pub fn is_timestamp(&self) -> bool {
        self.flags & MySqlFlag::TimestampFlag as u32 != 0
    }

    pub fn is_set(&self) -> bool {
        self.flags & MySqlFlag::SetFlag as u32 != 0
    }

    pub fn has_no_default_value(&self) -> bool {
        self.flags & MySqlFlag::NoDefaultValueFlag as u32 != 0
    }

    pub fn is_on_update_now(&self) -> bool {
        self.flags & MySqlFlag::OnUpdateNowFlag as u32 != 0
    }

    pub fn is_num(&self) -> bool {
        self.flags & MySqlFlag::NumFlag as u32 != 0
    }

    pub fn is_part_key(&self) -> bool {
        self.flags & MySqlFlag::PartKeyFlag as u32 != 0
    }

    pub fn is_unique(&self) -> bool {
        self.flags & MySqlFlag::UniqueFlag as u32 != 0
    }

    pub fn is_bincmp(&self) -> bool {
        self.flags & MySqlFlag::BincmpFlag as u32 != 0
    }
}

impl From<&Field> for Column {
    fn from(field: &Field) -> Self {
        Column::new(
            &field.name,
            vitess_grpc::query::Type::from_i32(field.r#type).unwrap(),
            &field.column_type,
            field.flags,
        )
    }
}
