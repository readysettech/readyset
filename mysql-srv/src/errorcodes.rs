// https://mariadb.com/kb/en/library/mariadb-error-codes/
// Generated using:
/*
extern crate reqwest;
extern crate select;

use std::io::Read;
use select::document::Document;
use select::predicate::{Child, Class, Name};
use std::collections::HashMap;

fn main() {
    let mut resp = reqwest::get("https://mariadb.com/kb/en/library/mariadb-error-codes/").unwrap();
    assert!(resp.status().is_success());

    let mut content = String::new();
    resp.read_to_string(&mut content).unwrap();

    let mut es = Vec::new();
    let document = Document::from(&*content);
    for tbl in document.find(Child(Class("cstm-style"), Name("table"))) {
        for row in tbl.find(Child(Name("tbody"), Name("tr"))) {
            let cols: Vec<_> = row.find(Name("td")).map(|c| c.text()).collect();
            if cols.is_empty() {
                // THs (because *someone* didn't learn about thead)
                continue;
            }

            let code = u16::from_str_radix(&cols[0], 10).unwrap();
            if code >= 1900 {
                // MariaDB-specific
                continue;
            }

            let mut cols = cols.into_iter();
            cols.next().unwrap(); // code
            let mut sqlstate = cols.next().unwrap();
            let name = if sqlstate.starts_with("ER_") {
                // someone messed up
                ::std::mem::replace(&mut sqlstate, "HY000".to_owned())
            } else {
                cols.next().unwrap()
            };
            let name = name.replace(" ", "");
            match &*name {
                "ER_CONST_EXPR_IN_PARTITION_FUNC_ERROR"
                | "ER_GTID_MODE_2_OR_3_REQUIRES_ENFORCE_GTID_CONSISTENCY_ON" => {
                    // defined twice, because of course
                    continue;
                }
                _ => {}
            }
            let desc = cols.next().unwrap();
            es.push((code, sqlstate, name, desc));
        }
    }

    println!("/// MySQL error type");
    println!("#[allow(non_camel_case_types)]");
    println!("#[derive(Clone, Copy, Eq, PartialEq, Debug, Hash, Ord, PartialOrd)]");
    println!("#[repr(u16)]");
    println!("pub enum ErrorKind {{");
    for &(code, _, ref name, ref desc) in &es {
        for l in desc.lines() {
            println!("    /// {}", l);
        }
        println!("    {} = {},", name, code);
    }
    println!("}}");
    println!("");
    println!("impl From<u16> for ErrorKind {{");
    println!("    fn from(x: u16) -> Self {{");
    println!("        match x {{");
    for &(code, _, ref name, _) in &es {
        println!("            {}_u16 => ErrorKind::{},", code, name);
    }
    println!("            _ => panic!(\"Unknown error type {{}}\", x),");
    println!("        }}");
    println!("    }}");
    println!("}}");
    println!("");
    println!("impl ErrorKind {{");
    println!("    /// SQLSTATE is a code which identifies SQL error conditions. It composed by five characters:");
    println!("    /// first two characters that indicate a class, and then three that indicate a subclass.");
    println!("    ///");
    println!("    /// There are three important standard classes.");
    println!("    ///");
    println!("    ///  - `00` means the operation completed successfully.");
    println!("    ///  - `01` contains warnings (`SQLWARNING`).");
    println!("    ///  - `02` is the `NOT FOUND` class.");
    println!("    ///");
    println!("    /// All other classes are exceptions (`SQLEXCEPTION`). Classes beginning with 0, 1, 2, 3, 4, A,");
    println!("    /// B, C, D, E, F and G are reserved for standard-defined classes, while other classes are");
    println!("    /// vendor-specific.");
    println!("    ///");
    println!("    /// The subclass, if it is set, indicates a particular condition, or a particular group of");
    println!("    /// conditions within the class. `000` means 'no subclass'.");
    println!("    ///");
    println!("    /// See also https://mariadb.com/kb/en/library/sqlstate/");
    println!("    pub fn sqlstate(&self) -> &'static [u8; 5] {{");
    println!("        match *self {{");
    let mut group = HashMap::new();
    for &(_, ref sqlstate, ref name, _) in &es {
        group.entry(sqlstate).or_insert_with(Vec::new).push(name);
    }
    for (sqlstate, names) in group {
        let n = names.len();
        for (i, name) in names.into_iter().enumerate() {
            print!("            ");
            if i != 0 {
                print!("| ");
            }
            print!("ErrorKind::{}", name);
            if i == n - 1 {
                print!(" => b\"{}\",", sqlstate);
            }
            println!("");
        }
    }
    println!("        }}");
    println!("    }}");
    println!("}}");
}
*/

/// MySQL error type
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Eq, PartialEq, Debug, Hash, Ord, PartialOrd)]
#[repr(u16)]
pub enum ErrorKind {
    /// hashchk
    ER_HASHCHK = 1000,
    /// isamchk
    ER_NISAMCHK = 1001,
    /// NO
    ER_NO = 1002,
    /// YES
    ER_YES = 1003,
    /// Can't create file '%s' (errno: %d)
    ER_CANT_CREATE_FILE = 1004,
    /// Can't create table '%s' (errno: %d)
    ER_CANT_CREATE_TABLE = 1005,
    /// Can't create database '%s' (errno: %d
    ER_CANT_CREATE_DB = 1006,
    /// Can't create database '%s'; database exists
    ER_DB_CREATE_EXISTS = 1007,
    /// Can't drop database '%s'; database doesn't exist
    ER_DB_DROP_EXISTS = 1008,
    /// Error dropping database (can't delete '%s', errno: %d)
    ER_DB_DROP_DELETE = 1009,
    /// Error dropping database (can't rmdir '%s', errno: %d)
    ER_DB_DROP_RMDIR = 1010,
    /// Error on delete of '%s' (errno: %d)
    ER_CANT_DELETE_FILE = 1011,
    /// Can't read record in system table
    ER_CANT_FIND_SYSTEM_REC = 1012,
    /// Can't get status of '%s' (errno: %d)
    ER_CANT_GET_STAT = 1013,
    /// Can't get working directory (errno: %d)
    ER_CANT_GET_WD = 1014,
    /// Can't lock file (errno: %d)
    ER_CANT_LOCK = 1015,
    /// Can't open file: '%s' (errno: %d)
    ER_CANT_OPEN_FILE = 1016,
    /// Can't find file: '%s' (errno: %d)
    ER_FILE_NOT_FOUND = 1017,
    /// Can't read dir of '%s' (errno: %d)
    ER_CANT_READ_DIR = 1018,
    /// Can't change dir to '%s' (errno: %d)
    ER_CANT_SET_WD = 1019,
    /// Record has changed since last read in table '%s'
    ER_CHECKREAD = 1020,
    /// Disk full (%s); waiting for someone to free some space...
    ER_DISK_FULL = 1021,
    /// Can't write; duplicate key in table '%s'
    ER_DUP_KEY = 1022,
    /// Error on close of '%s' (errno: %d)
    ER_ERROR_ON_CLOSE = 1023,
    /// Error reading file '%s' (errno: %d)
    ER_ERROR_ON_READ = 1024,
    /// Error on rename of '%s' to '%s' (errno: %d)
    ER_ERROR_ON_RENAME = 1025,
    /// Error writing file '%s' (errno: %d)
    ER_ERROR_ON_WRITE = 1026,
    /// '%s' is locked against change
    ER_FILE_USED = 1027,
    /// Sort aborted
    ER_FILSORT_ABORT = 1028,
    /// View '%s' doesn't exist for '%s'
    ER_FORM_NOT_FOUND = 1029,
    /// Got error %d from storage engine
    ER_GET_ERRN = 1030,
    /// Table storage engine for '%s' doesn't have this option
    ER_ILLEGAL_HA = 1031,
    /// Can't find record in '%s'
    ER_KEY_NOT_FOUND = 1032,
    /// Incorrect information in file: '%s'
    ER_NOT_FORM_FILE = 1033,
    /// Incorrect key file for table '%s'; try to repair it
    ER_NOT_KEYFILE = 1034,
    /// Old key file for table '%s'; repair it!
    ER_OLD_KEYFILE = 1035,
    /// Table '%s' is read only
    ER_OPEN_AS_READONLY = 1036,
    /// Out of memory; restart server and try again (needed %d bytes)
    ER_OUTOFMEMORY = 1037,
    /// Out of sort memory, consider increasing server sort buffer size
    ER_OUT_OF_SORTMEMORY = 1038,
    /// Unexpected EOF found when reading file '%s' (Errno: %d)
    ER_UNEXPECTED_EOF = 1039,
    /// Too many connections
    ER_CON_COUNT_ERROR = 1040,
    /// Out of memory; check if mysqld or some other process uses all available memory; if not, you
    /// may have to use 'ulimit' to allow mysqld to use more memory or you can add more swap space
    ER_OUT_OF_RESOURCES = 1041,
    /// Can't get hostname for your address
    ER_BAD_HOST_ERROR = 1042,
    /// Bad handshake
    ER_HANDSHAKE_ERROR = 1043,
    /// Access denied for user '%s'@'%s' to database '%s'
    ER_DBACCESS_DENIED_ERROR = 1044,
    /// Access denied for user '%s'@'%s' (using password: %s)
    ER_ACCESS_DENIED_ERROR = 1045,
    /// No database selected
    ER_NO_DB_ERROR = 1046,
    /// Unknown command
    ER_UNKNOWN_COM_ERROR = 1047,
    /// Column '%s' cannot be null
    ER_BAD_NULL_ERROR = 1048,
    /// Unknown database '%s'
    ER_BAD_DB_ERROR = 1049,
    /// Table '%s' already exists
    ER_TABLE_EXISTS_ERROR = 1050,
    /// Unknown table '%s'
    ER_BAD_TABLE_ERROR = 1051,
    /// Column '%s' in %s is ambiguous
    ER_NON_UNIQ_ERROR = 1052,
    /// Server shutdown in progress
    ER_SERVER_SHUTDOWN = 1053,
    /// Unknown column '%s' in '%s'
    ER_BAD_FIELD_ERROR = 1054,
    /// '%s' isn't in GROUP BY
    ER_WRONG_FIELD_WITH_GROUP = 1055,
    /// Can't group on '%s'
    ER_WRONG_GROUP_FIELD = 1056,
    /// Statement has sum functions and columns in same statement
    ER_WRONG_SUM_SELECT = 1057,
    /// Column count doesn't match value count
    ER_WRONG_VALUE_COUNT = 1058,
    /// Identifier name '%s' is too long
    ER_TOO_LONG_IDENT = 1059,
    /// Duplicate column name '%s'
    ER_DUP_FIELDNAME = 1060,
    /// Duplicate key name '%s'
    ER_DUP_KEYNAME = 1061,
    /// Duplicate entry '%s' for key %d
    ER_DUP_ENTRY = 1062,
    /// Incorrect column specifier for column '%s'
    ER_WRONG_FIELD_SPEC = 1063,
    /// %s near '%s' at line %d
    ER_PARSE_ERROR = 1064,
    /// Query was empty
    ER_EMPTY_QUERY = 1065,
    /// Not unique table/alias: '%s'
    ER_NONUNIQ_TABLE = 1066,
    /// Invalid default value for '%s'
    ER_INVALID_DEFAULT = 1067,
    /// Multiple primary key defined
    ER_MULTIPLE_PRI_KEY = 1068,
    /// Too many keys specified; max %d keys allowed
    ER_TOO_MANY_KEYS = 1069,
    /// Too many key parts specified; max %d parts allowed
    ER_TOO_MANY_KEY_PARTS = 1070,
    /// Specified key was too long; max key length is %d bytes
    ER_TOO_LONG_KEY = 1071,
    /// Key column '%s' doesn't exist in table
    ER_KEY_COLUMN_DOES_NOT_EXITS = 1072,
    /// BLOB column '%s' can't be used in key specification with the used table type
    ER_BLOB_USED_AS_KEY = 1073,
    /// Column length too big for column '%s' (max = %lu); use BLOB or TEXT instead
    ER_TOO_BIG_FIELDLENGTH = 1074,
    /// Incorrect table definition; there can be only one auto column and it must be defined as a
    /// key
    ER_WRONG_AUTO_KEY = 1075,
    /// %s: ready for connections. Version: '%s' socket: '%s' port: %d
    ER_READY = 1076,
    /// %s: Normal shutdown
    ER_NORMAL_SHUTDOWN = 1077,
    /// %s: Got signal %d. Aborting!
    ER_GOT_SIGNAL = 1078,
    /// %s: Shutdown complete
    ER_SHUTDOWN_COMPLETE = 1079,
    /// %s: Forcing close of thread %ld user: '%s'
    ER_FORCING_CLOSE = 1080,
    /// Can't create IP socket
    ER_IPSOCK_ERROR = 1081,
    /// Table '%s' has no index like the one used in CREATE INDEX; recreate the table
    ER_NO_SUCH_INDEX = 1082,
    /// Field separator argument is not what is expected; check the manual
    ER_WRONG_FIELD_TERMINATORS = 1083,
    /// You can't use fixed rowlength with BLOBs; please use 'fields terminated by'
    ER_BLOBS_AND_NO_TERMINATED = 1084,
    /// The file '%s' must be in the database directory or be readable by all
    ER_TEXTFILE_NOT_READABLE = 1085,
    /// File '%s' already exists
    ER_FILE_EXISTS_ERROR = 1086,
    /// Records: %ld Deleted: %ld Skipped: %ld Warnings: %ld
    ER_LOAD_INF = 1087,
    /// Records: %ld Duplicates: %ld
    ER_ALTER_INF = 1088,
    /// Incorrect prefix key; the used key part isn't a string, the used length is longer than the
    /// key part, or the storage engine doesn't support unique prefix keys
    ER_WRONG_SUB_KEY = 1089,
    /// You can't delete all columns with ALTER TABLE; use DROP TABLE instead
    ER_CANT_REMOVE_ALL_FIELDS = 1090,
    /// Can't DROP '%s'; check that column/key exists
    ER_CANT_DROP_FIELD_OR_KEY = 1091,
    /// Records: %ld Duplicates: %ld Warnings: %ld
    ER_INSERT_INF = 1092,
    /// You can't specify target table '%s' for update in FROM clause
    ER_UPDATE_TABLE_USED = 1093,
    /// Unknown thread id: %lu
    ER_NO_SUCH_THREAD = 1094,
    /// You are not owner of thread %lu
    ER_KILL_DENIED_ERROR = 1095,
    /// No tables used
    ER_NO_TABLES_USED = 1096,
    /// Too many strings for column %s and SET
    ER_TOO_BIG_SET = 1097,
    /// Can't generate a unique log-filename %s.(1-999)
    ER_NO_UNIQUE_LOGFILE = 1098,
    /// Table '%s' was locked with a READ lock and can't be updated
    ER_TABLE_NOT_LOCKED_FOR_WRITE = 1099,
    /// Table '%s' was not locked with LOCK TABLES
    ER_TABLE_NOT_LOCKED = 1100,
    /// BLOB/TEXT column '%s' can't have a default value
    ER_BLOB_CANT_HAVE_DEFAULT = 1101,
    /// Incorrect database name '%s'
    ER_WRONG_DB_NAME = 1102,
    /// Incorrect table name '%s'
    ER_WRONG_TABLE_NAME = 1103,
    /// The SELECT would examine more than MAX_JOIN_SIZE rows; check your WHERE and use SET
    /// SQL_BIG_SELECTS=1 or SET MAX_JOIN_SIZE=# if the SELECT is okay
    ER_TOO_BIG_SELECT = 1104,
    /// Unknown error
    ER_UNKNOWN_ERROR = 1105,
    /// Unknown procedure '%s'
    ER_UNKNOWN_PROCEDURE = 1106,
    /// Incorrect parameter count to procedure '%s'
    ER_WRONG_PARAMCOUNT_TO_PROCEDURE = 1107,
    /// Incorrect parameters to procedure '%s'
    ER_WRONG_PARAMETERS_TO_PROCEDURE = 1108,
    /// Unknown table '%s' in %s
    ER_UNKNOWN_TABLE = 1109,
    /// Column '%s' specified twice
    ER_FIELD_SPECIFIED_TWICE = 1110,
    /// Invalid use of group function
    ER_INVALID_GROUP_FUNC_USE = 1111,
    /// Table '%s' uses an extension that doesn't exist in this MariaDB version
    ER_UNSUPPORTED_EXTENSION = 1112,
    /// A table must have at least 1 column
    ER_TABLE_MUST_HAVE_COLUMNS = 1113,
    /// The table '%s' is full
    ER_RECORD_FILE_FULL = 1114,
    /// Unknown character set: '%s'
    ER_UNKNOWN_CHARACTER_SET = 1115,
    /// Too many tables; MariaDB can only use %d tables in a join
    ER_TOO_MANY_TABLES = 1116,
    /// Too many columns
    ER_TOO_MANY_FIELDS = 1117,
    /// Row size too large. The maximum row size for the used table type, not counting BLOBs, is
    /// %ld. You have to change some columns to TEXT or BLOBs
    ER_TOO_BIG_ROWSIZE = 1118,
    /// Thread stack overrun: Used: %ld of a %ld stack. Use 'mysqld --thread_stack=#' to specify a
    /// bigger stack if needed
    ER_STACK_OVERRUN = 1119,
    /// Cross dependency found in OUTER JOIN; examine your ON conditions
    ER_WRONG_OUTER_JOIN = 1120,
    /// Table handler doesn't support NULL in given index. Please change column '%s' to be NOT NULL
    /// or use another handler
    ER_NULL_COLUMN_IN_INDEX = 1121,
    /// Can't load function '%s'
    ER_CANT_FIND_UDF = 1122,
    /// Can't initialize function '%s'; %s
    ER_CANT_INITIALIZE_UDF = 1123,
    /// No paths allowed for shared library
    ER_UDF_NO_PATHS = 1124,
    /// Function '%s' already exists
    ER_UDF_EXISTS = 1125,
    /// Can't open shared library '%s' (Errno: %d %s)
    ER_CANT_OPEN_LIBRARY = 1126,
    /// Can't find symbol '%s' in library
    ER_CANT_FIND_DL_ENTRY = 1127,
    /// Function '%s' is not defined
    ER_FUNCTION_NOT_DEFINED = 1128,
    /// Host '%s' is blocked because of many connection errors; unblock with 'mysqladmin
    /// flush-hosts'
    ER_HOST_IS_BLOCKED = 1129,
    /// Host '%s' is not allowed to connect to this MariaDB server
    ER_HOST_NOT_PRIVILEGED = 1130,
    /// You are using MariaDB as an anonymous user and anonymous users are not allowed to change
    /// passwords
    ER_PASSWORD_ANONYMOUS_USER = 1131,
    /// You must have privileges to update tables in the mysql database to be able to change
    /// passwords for others
    ER_PASSWORD_NOT_ALLOWED = 1132,
    /// Can't find any matching row in the user table
    ER_PASSWORD_NO_MATCH = 1133,
    /// Rows matched: %ld Changed: %ld Warnings: %ld
    ER_UPDATE_INF = 1134,
    /// Can't create a new thread (Errno %d); if you are not out of available memory, you can
    /// consult the manual for a possible OS-dependent bug
    ER_CANT_CREATE_THREAD = 1135,
    /// Column count doesn't match value count at row %ld
    ER_WRONG_VALUE_COUNT_ON_ROW = 1136,
    /// Can't reopen table: '%s'
    ER_CANT_REOPEN_TABLE = 1137,
    /// Invalid use of NULL value
    ER_INVALID_USE_OF_NULL = 1138,
    /// Got error '%s' from regexp
    ER_REGEXP_ERROR = 1139,
    /// Mixing of GROUP columns (MIN(),MAX(),COUNT(),...) with no GROUP columns is illegal if there
    /// is no GROUP BY clause
    ER_MIX_OF_GROUP_FUNC_AND_FIELDS = 1140,
    /// There is no such grant defined for user '%s' on host '%s'
    ER_NONEXISTING_GRANT = 1141,
    /// %s command denied to user '%s'@'%s' for table '%s'
    ER_TABLEACCESS_DENIED_ERROR = 1142,
    /// %s command denied to user '%s'@'%s' for column '%s' in table '%s'
    ER_COLUMNACCESS_DENIED_ERROR = 1143,
    /// Illegal GRANT/REVOKE command; please consult the manual to see which privileges can be used
    ER_ILLEGAL_GRANT_FOR_TABLE = 1144,
    /// The host or user argument to GRANT is too long
    ER_GRANT_WRONG_HOST_OR_USER = 1145,
    /// Table '%s.%s' doesn't exist
    ER_NO_SUCH_TABLE = 1146,
    /// There is no such grant defined for user '%s' on host '%s' on table '%s'
    ER_NONEXISTING_TABLE_GRANT = 1147,
    /// The used command is not allowed with this MariaDB version
    ER_NOT_ALLOWED_COMMAND = 1148,
    /// You have an error in your SQL syntax; check the manual that corresponds to your MariaDB
    /// server version for the right syntax to use
    ER_SYNTAX_ERROR = 1149,
    /// Delayed insert thread couldn't get requested lock for table %s
    ER_DELAYED_CANT_CHANGE_LOCK = 1150,
    /// Too many delayed threads in use
    ER_TOO_MANY_DELAYED_THREADS = 1151,
    /// Aborted connection %ld to db: '%s' user: '%s' (%s)
    ER_ABORTING_CONNECTION = 1152,
    /// Got a packet bigger than 'max_allowed_packet' bytes
    ER_NET_PACKET_TOO_LARGE = 1153,
    /// Got a read error from the connection pipe
    ER_NET_READ_ERROR_FROM_PIPE = 1154,
    /// Got an error from fcntl()
    ER_NET_FCNTL_ERROR = 1155,
    /// Got packets out of order
    ER_NET_PACKETS_OUT_OF_ORDER = 1156,
    /// Couldn't uncompress communication packet
    ER_NET_UNCOMPRESS_ERROR = 1157,
    /// Got an error reading communication packets
    ER_NET_READ_ERROR = 1158,
    /// Got timeout reading communication packets
    ER_NET_READ_INTERRUPTED = 1159,
    /// Got an error writing communication packets
    ER_NET_ERROR_ON_WRITE = 1160,
    /// Got timeout writing communication packets
    ER_NET_WRITE_INTERRUPTED = 1161,
    /// Result string is longer than 'max_allowed_packet' bytes
    ER_TOO_LONG_STRING = 1162,
    /// The used table type doesn't support BLOB/TEXT columns
    ER_TABLE_CANT_HANDLE_BLOB = 1163,
    /// The used table type doesn't support AUTO_INCREMENT columns
    ER_TABLE_CANT_HANDLE_AUTO_INCREMENT = 1164,
    /// INSERT DELAYED can't be used with table '%s' because it is locked with LOCK TABLES
    ER_DELAYED_INSERT_TABLE_LOCKED = 1165,
    /// Incorrect column name '%s'
    ER_WRONG_COLUMN_NAME = 1166,
    /// The used storage engine can't index column '%s'
    ER_WRONG_KEY_COLUMN = 1167,
    /// Unable to open underlying table which is differently defined or of non-MyISAM type or
    /// doesn't exist
    ER_WRONG_MRG_TABLE = 1168,
    /// Can't write, because of unique constraint, to table '%s'
    ER_DUP_UNIQUE = 1169,
    /// BLOB/TEXT column '%s' used in key specification without a key length
    ER_BLOB_KEY_WITHOUT_LENGTH = 1170,
    /// All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead
    ER_PRIMARY_CANT_HAVE_NULL = 1171,
    /// Result consisted of more than one row
    ER_TOO_MANY_ROWS = 1172,
    /// This table type requires a primary key
    ER_REQUIRES_PRIMARY_KEY = 1173,
    /// This version of MariaDB is not compiled with RAID support
    ER_NO_RAID_COMPILED = 1174,
    /// You are using safe update mode and you tried to update a table without a WHERE that uses a
    /// KEY column
    ER_UPDATE_WITHOUT_KEY_IN_SAFE_MODE = 1175,
    /// Key '%s' doesn't exist in table '%s'
    ER_KEY_DOES_NOT_EXITS = 1176,
    /// Can't open table
    ER_CHECK_NO_SUCH_TABLE = 1177,
    /// The storage engine for the table doesn't support %s
    ER_CHECK_NOT_IMPLEMENTED = 1178,
    /// You are not allowed to execute this command in a transaction
    ER_CANT_DO_THIS_DURING_AN_TRANSACTION = 1179,
    /// Got error %d during COMMIT
    ER_ERROR_DURING_COMMIT = 1180,
    /// Got error %d during ROLLBACK
    ER_ERROR_DURING_ROLLBACK = 1181,
    /// Got error %d during FLUSH_LOGS
    ER_ERROR_DURING_FLUSH_LOGS = 1182,
    /// Got error %d during CHECKPOINT
    ER_ERROR_DURING_CHECKPOINT = 1183,
    /// Aborted connection %ld to db: '%s' user: '%s' host: '%s' (%s)
    ER_NEW_ABORTING_CONNECTION = 1184,
    /// The storage engine for the table does not support binary table dump
    ER_DUMP_NOT_IMPLEMENTED = 1185,
    /// Binlog closed, cannot RESET MASTER
    ER_FLUSH_MASTER_BINLOG_CLOSED = 1186,
    /// Failed rebuilding the index of dumped table '%s'
    ER_INDEX_REBUILD = 1187,
    /// Error from master: '%s'
    ER_MASTER = 1188,
    /// Net error reading from master
    ER_MASTER_NET_READ = 1189,
    /// Net error writing to master
    ER_MASTER_NET_WRITE = 1190,
    /// Can't find FULLTEXT index matching the column list
    ER_FT_MATCHING_KEY_NOT_FOUND = 1191,
    /// Can't execute the given command because you have active locked tables or an active
    /// transaction
    ER_LOCK_OR_ACTIVE_TRANSACTION = 1192,
    /// Unknown system variable '%s'
    ER_UNKNOWN_SYSTEM_VARIABLE = 1193,
    /// Table '%s' is marked as crashed and should be repaired
    ER_CRASHED_ON_USAGE = 1194,
    /// Table '%s' is marked as crashed and last (automatic?) repair failed
    ER_CRASHED_ON_REPAIR = 1195,
    /// Some non-transactional changed tables couldn't be rolled back
    ER_WARNING_NOT_COMPLETE_ROLLBACK = 1196,
    /// Multi-statement transaction required more than 'max_binlog_cache_size' bytes of storage;
    /// increase this mysqld variable and try again
    ER_TRANS_CACHE_FULL = 1197,
    /// This operation cannot be performed with a running slave; run STOP SLAVE first
    ER_SLAVE_MUST_STOP = 1198,
    /// This operation requires a running slave; configure slave and do START SLAVE
    ER_SLAVE_NOT_RUNNING = 1199,
    /// The server is not configured as slave; fix in config file or with CHANGE MASTER TO
    ER_BAD_SLAVE = 1200,
    /// Could not initialize master info structure; more error messages can be found in the MariaDB
    /// error log
    ER_MASTER_INF = 1201,
    /// Could not create slave thread; check system resources
    ER_SLAVE_THREAD = 1202,
    /// User %s already has more than 'max_user_connections' active connections
    ER_TOO_MANY_USER_CONNECTIONS = 1203,
    /// You may only use constant expressions with SET
    ER_SET_CONSTANTS_ONLY = 1204,
    /// Lock wait timeout exceeded; try restarting transaction
    ER_LOCK_WAIT_TIMEOUT = 1205,
    /// The total number of locks exceeds the lock table size
    ER_LOCK_TABLE_FULL = 1206,
    /// Update locks cannot be acquired during a READ UNCOMMITTED transaction
    ER_READ_ONLY_TRANSACTION = 1207,
    /// DROP DATABASE not allowed while thread is holding global read lock
    ER_DROP_DB_WITH_READ_LOCK = 1208,
    /// CREATE DATABASE not allowed while thread is holding global read lock
    ER_CREATE_DB_WITH_READ_LOCK = 1209,
    /// Incorrect arguments to %s
    ER_WRONG_ARGUMENTS = 1210,
    /// '%s'@'%s' is not allowed to create new users
    ER_NO_PERMISSION_TO_CREATE_USER = 1211,
    /// Incorrect table definition; all MERGE tables must be in the same database
    ER_UNION_TABLES_IN_DIFFERENT_DIR = 1212,
    /// Deadlock found when trying to get lock; try restarting transaction
    ER_LOCK_DEADLOCK = 1213,
    /// The used table type doesn't support FULLTEXT indexes
    ER_TABLE_CANT_HANDLE_FT = 1214,
    /// Cannot add foreign key constraint
    ER_CANNOT_ADD_FOREIGN = 1215,
    /// Cannot add or update a child row: a foreign key constraint fails
    ER_NO_REFERENCED_ROW = 1216,
    /// Cannot delete or update a parent row: a foreign key constraint fails
    ER_ROW_IS_REFERENCED = 1217,
    /// Error connecting to master: %s
    ER_CONNECT_TO_MASTER = 1218,
    /// Error running query on master: %s
    ER_QUERY_ON_MASTER = 1219,
    /// Error when executing command %s: %s
    ER_ERROR_WHEN_EXECUTING_COMMAND = 1220,
    /// Incorrect usage of %s and %s
    ER_WRONG_USAGE = 1221,
    /// The used SELECT statements have a different number of columns
    ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT = 1222,
    /// Can't execute the query because you have a conflicting read lock
    ER_CANT_UPDATE_WITH_READLOCK = 1223,
    /// Mixing of transactional and non-transactional tables is disabled
    ER_MIXING_NOT_ALLOWED = 1224,
    /// Option '%s' used twice in statement
    ER_DUP_ARGUMENT = 1225,
    /// User '%s' has exceeded the '%s' resource (current value: %ld)
    ER_USER_LIMIT_REACHED = 1226,
    /// Access denied; you need (at least one of) the %s privilege(s) for this operation
    ER_SPECIFIC_ACCESS_DENIED_ERROR = 1227,
    /// Variable '%s' is a SESSION variable and can't be used with SET GLOBAL
    ER_LOCAL_VARIABLE = 1228,
    /// Variable '%s' is a GLOBAL variable and should be set with SET GLOBAL
    ER_GLOBAL_VARIABLE = 1229,
    /// Variable '%s' doesn't have a default value
    ER_NO_DEFAULT = 1230,
    /// Variable '%s' can't be set to the value of '%s'
    ER_WRONG_VALUE_FOR_VAR = 1231,
    /// Incorrect argument type to variable '%s'
    ER_WRONG_TYPE_FOR_VAR = 1232,
    /// Variable '%s' can only be set, not read
    ER_VAR_CANT_BE_READ = 1233,
    /// Incorrect usage/placement of '%s'
    ER_CANT_USE_OPTION_HERE = 1234,
    /// This version of MariaDB doesn't yet support '%s'
    ER_NOT_SUPPORTED_YET = 1235,
    /// Got fatal error %d from master when reading data from binary log: '%s'
    ER_MASTER_FATAL_ERROR_READING_BINLOG = 1236,
    /// Slave SQL thread ignored the query because of replicate-*-table rules
    ER_SLAVE_IGNORED_TABLE = 1237,
    /// Variable '%s' is a %s variable
    ER_INCORRECT_GLOBAL_LOCAL_VAR = 1238,
    /// Incorrect foreign key definition for '%s': %s
    ER_WRONG_FK_DEF = 1239,
    /// Key reference and table reference don't match
    ER_KEY_REF_DO_NOT_MATCH_TABLE_REF = 1240,
    /// Operand should contain %d column(s)
    ER_OPERAND_COLUMNS = 1241,
    /// Subquery returns more than 1 row
    ER_SUBQUERY_NO_1_ROW = 1242,
    /// Unknown prepared statement handler (%.*s) given to %s
    ER_UNKNOWN_STMT_HANDLER = 1243,
    /// Help database is corrupt or does not exist
    ER_CORRUPT_HELP_DB = 1244,
    /// Cyclic reference on subqueries
    ER_CYCLIC_REFERENCE = 1245,
    /// Converting column '%s' from %s to %s
    ER_AUTO_CONVERT = 1246,
    /// Reference '%s' not supported (%s)
    ER_ILLEGAL_REFERENCE = 1247,
    /// Every derived table must have its own alias
    ER_DERIVED_MUST_HAVE_ALIAS = 1248,
    /// Select %u was reduced during optimization
    ER_SELECT_REDUCED = 1249,
    /// Table '%s' from one of the SELECTs cannot be used in %s
    ER_TABLENAME_NOT_ALLOWED_HERE = 1250,
    /// Client does not support authentication protocol requested by server; consider upgrading
    /// MariaDB client
    ER_NOT_SUPPORTED_AUTH_MODE = 1251,
    /// All parts of a SPATIAL index must be NOT NULL
    ER_SPATIAL_CANT_HAVE_NULL = 1252,
    /// COLLATION '%s' is not valid for CHARACTER SET '%s'
    ER_COLLATION_CHARSET_MISMATCH = 1253,
    /// Slave is already running
    ER_SLAVE_WAS_RUNNING = 1254,
    /// Slave already has been stopped
    ER_SLAVE_WAS_NOT_RUNNING = 1255,
    /// Uncompressed data size too large; the maximum size is %d (probably, length of uncompressed
    /// data was corrupted)
    ER_TOO_BIG_FOR_UNCOMPRESS = 1256,
    /// ZLIB: Not enough memory
    ER_ZLIB_Z_MEM_ERROR = 1257,
    /// ZLIB: Not enough room in the output buffer (probably, length of uncompressed data was
    /// corrupted)
    ER_ZLIB_Z_BUF_ERROR = 1258,
    /// ZLIB: Input data corrupted
    ER_ZLIB_Z_DATA_ERROR = 1259,
    /// Row %u was cut by GROUP_CONCAT()
    ER_CUT_VALUE_GROUP_CONCAT = 1260,
    /// Row %ld doesn't contain data for all columns
    ER_WARN_TOO_FEW_RECORDS = 1261,
    /// Row %ld was truncated; it contained more data than there were input columns
    ER_WARN_TOO_MANY_RECORDS = 1262,
    /// Column set to default value; NULL supplied to NOT NULL column '%s' at row %ld
    ER_WARN_NULL_TO_NOTNULL = 1263,
    /// Out of range value for column '%s' at row %ld
    ER_WARN_DATA_OUT_OF_RANGE = 1264,
    /// Data truncated for column '%s' at row %ld
    WARN_DATA_TRUNCATED = 1265,
    /// Using storage engine %s for table '%s'
    ER_WARN_USING_OTHER_HANDLER = 1266,
    /// Illegal mix of collations (%s,%s) and (%s,%s) for operation '%s'
    ER_CANT_AGGREGATE_2COLLATIONS = 1267,
    /// Cannot drop one or more of the requested users
    ER_DROP_USER = 1268,
    /// Can't revoke all privileges for one or more of the requested users
    ER_REVOKE_GRANTS = 1269,
    /// Illegal mix of collations (%s,%s), (%s,%s), (%s,%s) for operation '%s'
    ER_CANT_AGGREGATE_3COLLATIONS = 1270,
    /// Illegal mix of collations for operation '%s'
    ER_CANT_AGGREGATE_NCOLLATIONS = 1271,
    /// Variable '%s' is not a variable component (can't be used as XXXX.variable_name)
    ER_VARIABLE_IS_NOT_STRUCT = 1272,
    /// Unknown collation: '%s'
    ER_UNKNOWN_COLLATION = 1273,
    /// SSL parameters in CHANGE MASTER are ignored because this MariaDB slave was compiled without
    /// SSL support; they can be used later if MariaDB slave with SSL is started
    ER_SLAVE_IGNORED_SSL_PARAMS = 1274,
    /// Server is running in --secure-auth mode, but '%s'@'%s' has a password in the old format;
    /// please change the password to the new format
    ER_SERVER_IS_IN_SECURE_AUTH_MODE = 1275,
    /// Field or reference '%s%s%s%s%s' of SELECT #%d was resolved in SELECT #%d
    ER_WARN_FIELD_RESOLVED = 1276,
    /// Incorrect parameter or combination of parameters for START SLAVE UNTIL
    ER_BAD_SLAVE_UNTIL_COND = 1277,
    /// It is recommended to use --skip-slave-start when doing step-by-step replication with START
    /// SLAVE UNTIL; otherwise, you will get problems if you get an unexpected slave's mysqld
    /// restart
    ER_MISSING_SKIP_SLAVE = 1278,
    /// SQL thread is not to be started so UNTIL options are ignored
    ER_UNTIL_COND_IGNORED = 1279,
    /// Incorrect index name '%s'
    ER_WRONG_NAME_FOR_INDEX = 1280,
    /// Incorrect catalog name '%s'
    ER_WRONG_NAME_FOR_CATALOG = 1281,
    /// Query cache failed to set size %lu; new query cache size is %lu
    ER_WARN_QC_RESIZE = 1282,
    /// Column '%s' cannot be part of FULLTEXT index
    ER_BAD_FT_COLUMN = 1283,
    /// Unknown key cache '%s'
    ER_UNKNOWN_KEY_CACHE = 1284,
    /// MariaDB is started in --skip-name-resolve mode; you must restart it without this switch for
    /// this grant to work
    ER_WARN_HOSTNAME_WONT_WORK = 1285,
    /// Unknown storage engine '%s'
    ER_UNKNOWN_STORAGE_ENGINE = 1286,
    /// '%s' is deprecated and will be removed in a future release. Please use %s instead
    ER_WARN_DEPRECATED_SYNTAX = 1287,
    /// The target table %s of the %s is not updatable
    ER_NON_UPDATABLE_TABLE = 1288,
    /// The '%s' feature is disabled; you need MariaDB built with '%s' to have it working
    ER_FEATURE_DISABLED = 1289,
    /// The MariaDB server is running with the %s option so it cannot execute this statement
    ER_OPTION_PREVENTS_STATEMENT = 1290,
    /// Column '%s' has duplicated value '%s' in %s
    ER_DUPLICATED_VALUE_IN_TYPE = 1291,
    /// Truncated incorrect %s value: '%s'
    ER_TRUNCATED_WRONG_VALUE = 1292,
    /// Incorrect table definition; there can be only one TIMESTAMP column with CURRENT_TIMESTAMP
    /// in DEFAULT or ON UPDATE clause
    ER_TOO_MUCH_AUTO_TIMESTAMP_COLS = 1293,
    /// Invalid ON UPDATE clause for '%s' column
    ER_INVALID_ON_UPDATE = 1294,
    /// This command is not supported in the prepared statement protocol yet
    ER_UNSUPPORTED_PS = 1295,
    /// Got error %d '%s' from %s
    ER_GET_ERRMSG = 1296,
    /// Got temporary error %d '%s' from %s
    ER_GET_TEMPORARY_ERRMSG = 1297,
    /// Unknown or incorrect time zone: '%s'
    ER_UNKNOWN_TIME_ZONE = 1298,
    /// Invalid TIMESTAMP value in column '%s' at row %ld
    ER_WARN_INVALID_TIMESTAMP = 1299,
    /// Invalid %s character string: '%s'
    ER_INVALID_CHARACTER_STRING = 1300,
    /// Result of %s() was larger than max_allowed_packet (%ld) - truncated
    ER_WARN_ALLOWED_PACKET_OVERFLOWED = 1301,
    /// Conflicting declarations: '%s%s' and '%s%s'
    ER_CONFLICTING_DECLARATIONS = 1302,
    /// Can't create a %s from within another stored routine
    ER_SP_NO_RECURSIVE_CREATE = 1303,
    /// %s %s already exists
    ER_SP_ALREADY_EXISTS = 1304,
    /// %s %s does not exist
    ER_SP_DOES_NOT_EXIST = 1305,
    /// Failed to DROP %s %s
    ER_SP_DROP_FAILED = 1306,
    /// Failed to CREATE %s %s
    ER_SP_STORE_FAILED = 1307,
    /// %s with no matching label: %s
    ER_SP_LILABEL_MISMATCH = 1308,
    /// Redefining label %s
    ER_SP_LABEL_REDEFINE = 1309,
    /// End-label %s without match
    ER_SP_LABEL_MISMATCH = 1310,
    /// Referring to uninitialized variable %s
    ER_SP_UNINIT_VAR = 1311,
    /// PROCEDURE %s can't return a result set in the given context
    ER_SP_BADSELECT = 1312,
    /// RETURN is only allowed in a FUNCTION
    ER_SP_BADRETURN = 1313,
    /// %s is not allowed in stored procedures
    ER_SP_BADSTATEMENT = 1314,
    /// The update log is deprecated and replaced by the binary log; SET SQL_LOG_UPDATE has been
    /// ignored. This option will be removed in MariaDB 5.6.
    ER_UPDATE_LOG_DEPRECATED_IGNORED = 1315,
    /// The update log is deprecated and replaced by the binary log; SET SQL_LOG_UPDATE has been
    /// translated to SET SQL_LOG_BIN. This option will be removed in MariaDB 5.6.
    ER_UPDATE_LOG_DEPRECATED_TRANSLATED = 1316,
    /// Query execution was interrupted
    ER_QUERY_INTERRUPTED = 1317,
    /// Incorrect number of arguments for %s %s; expected %u, got %u
    ER_SP_WRONG_NO_OF_ARGS = 1318,
    /// Undefined CONDITION: %s
    ER_SP_COND_MISMATCH = 1319,
    /// No RETURN found in FUNCTION %s
    ER_SP_NORETURN = 1320,
    /// FUNCTION %s ended without RETURN
    ER_SP_NORETURNEND = 1321,
    /// Cursor statement must be a SELECT
    ER_SP_BAD_CURSOR_QUERY = 1322,
    /// Cursor SELECT must not have INTO
    ER_SP_BAD_CURSOR_SELECT = 1323,
    /// Undefined CURSOR: %s
    ER_SP_CURSOR_MISMATCH = 1324,
    /// Cursor is already open
    ER_SP_CURSOR_ALREADY_OPEN = 1325,
    /// Cursor is not open
    ER_SP_CURSOR_NOT_OPEN = 1326,
    /// Undeclared variable: %s
    ER_SP_UNDECLARED_VAR = 1327,
    /// Incorrect number of FETCH variables
    ER_SP_WRONG_NO_OF_FETCH_ARGS = 1328,
    /// No data - zero rows fetched, selected, or processed
    ER_SP_FETCH_NO_DATA = 1329,
    /// Duplicate parameter: %s
    ER_SP_DUP_PARAM = 1330,
    /// Duplicate variable: %s
    ER_SP_DUP_VAR = 1331,
    /// Duplicate condition: %s
    ER_SP_DUP_COND = 1332,
    /// Duplicate cursor: %s
    ER_SP_DUP_CURS = 1333,
    /// Failed to ALTER %s %s
    ER_SP_CANT_ALTER = 1334,
    /// Subquery value not supported
    ER_SP_SUBSELECT_NYI = 1335,
    /// %s is not allowed in stored function or trigger
    ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG = 1336,
    /// Variable or condition declaration after cursor or handler declaration
    ER_SP_VARCOND_AFTER_CURSHNDLR = 1337,
    /// Cursor declaration after handler declaration
    ER_SP_CURSOR_AFTER_HANDLER = 1338,
    /// Case not found for CASE statement
    ER_SP_CASE_NOT_FOUND = 1339,
    /// Configuration file '%s' is too big
    ER_FPARSER_TOO_BIG_FILE = 1340,
    /// Malformed file type header in file '%s'
    ER_FPARSER_BAD_HEADER = 1341,
    /// Unexpected end of file while parsing comment '%s'
    ER_FPARSER_EOF_IN_COMMENT = 1342,
    /// Error while parsing parameter '%s' (line: '%s')
    ER_FPARSER_ERROR_IN_PARAMETER = 1343,
    /// Unexpected end of file while skipping unknown parameter '%s'
    ER_FPARSER_EOF_IN_UNKNOWN_PARAMETER = 1344,
    /// EXPLAIN/SHOW can not be issued; lacking privileges for underlying table
    ER_VIEW_NO_EXPLAIN = 1345,
    /// File '%s' has unknown type '%s' in its header
    ER_FRM_UNKNOWN_TYPE = 1346,
    /// '%s.%s' is not %s
    ER_WRONG_OBJECT = 1347,
    /// Column '%s' is not updatable
    ER_NONUPDATEABLE_COLUMN = 1348,
    /// View's SELECT contains a subquery in the FROM clause
    ER_VIEW_SELECT_DERIVED = 1349,
    /// View's SELECT contains a '%s' clause
    ER_VIEW_SELECT_CLAUSE = 1350,
    /// View's SELECT contains a variable or parameter
    ER_VIEW_SELECT_VARIABLE = 1351,
    /// View's SELECT refers to a temporary table '%s'
    ER_VIEW_SELECT_TMPTABLE = 1352,
    /// View's SELECT and view's field list have different column counts
    ER_VIEW_WRONG_LIST = 1353,
    /// View merge algorithm can't be used here for now (assumed undefined algorithm)
    ER_WARN_VIEW_MERGE = 1354,
    /// View being updated does not have complete key of underlying table in it
    ER_WARN_VIEW_WITHOUT_KEY = 1355,
    /// View '%s.%s' references invalid table(s) or column(s) or function(s) or definer/invoker of
    /// view lack rights to use them
    ER_VIEW_INVALID = 1356,
    /// Can't drop or alter a %s from within another stored routine
    ER_SP_NO_DROP_SP = 1357,
    /// GOTO is not allowed in a stored procedure handler
    ER_SP_GOTO_IN_HNDLR = 1358,
    /// Trigger already exists
    ER_TRG_ALREADY_EXISTS = 1359,
    /// Trigger does not exist
    ER_TRG_DOES_NOT_EXIST = 1360,
    /// Trigger's '%s' is view or temporary table
    ER_TRG_ON_VIEW_OR_TEMP_TABLE = 1361,
    /// Updating of %s row is not allowed in %strigger
    ER_TRG_CANT_CHANGE_ROW = 1362,
    /// There is no %s row in %s trigger
    ER_TRG_NO_SUCH_ROW_IN_TRG = 1363,
    /// Field '%s' doesn't have a default value
    ER_NO_DEFAULT_FOR_FIELD = 1364,
    /// Division by 0
    ER_DIVISION_BY_ZER = 1365,
    /// Incorrect %s value: '%s' for column '%s' at row %ld
    ER_TRUNCATED_WRONG_VALUE_FOR_FIELD = 1366,
    /// Illegal %s '%s' value found during parsing
    ER_ILLEGAL_VALUE_FOR_TYPE = 1367,
    /// CHECK OPTION on non-updatable view '%s.%s'
    ER_VIEW_NONUPD_CHECK = 1368,
    /// CHECK OPTION failed '%s.%s'
    ER_VIEW_CHECK_FAILED = 1369,
    /// %s command denied to user '%s'@'%s' for routine '%s'
    ER_PROCACCESS_DENIED_ERROR = 1370,
    /// Failed purging old relay logs: %s
    ER_RELAY_LOG_FAIL = 1371,
    /// Password hash should be a %d-digit hexadecimal number
    ER_PASSWD_LENGTH = 1372,
    /// Target log not found in binlog index
    ER_UNKNOWN_TARGET_BINLOG = 1373,
    /// I/O error reading log index file
    ER_IO_ERR_LOG_INDEX_READ = 1374,
    /// Server configuration does not permit binlog purge
    ER_BINLOG_PURGE_PROHIBITED = 1375,
    /// Failed on fseek()
    ER_FSEEK_FAIL = 1376,
    /// Fatal error during log purge
    ER_BINLOG_PURGE_FATAL_ERR = 1377,
    /// A purgeable log is in use, will not purge
    ER_LOG_IN_USE = 1378,
    /// Unknown error during log purge
    ER_LOG_PURGE_UNKNOWN_ERR = 1379,
    /// Failed initializing relay log position: %s
    ER_RELAY_LOG_INIT = 1380,
    /// You are not using binary logging
    ER_NO_BINARY_LOGGING = 1381,
    /// The '%s' syntax is reserved for purposes internal to the MariaDB server
    ER_RESERVED_SYNTAX = 1382,
    /// WSAStartup Failed
    ER_WSAS_FAILED = 1383,
    /// Can't handle procedures with different groups yet
    ER_DIFF_GROUPS_PROC = 1384,
    /// Select must have a group with this procedure
    ER_NO_GROUP_FOR_PROC = 1385,
    /// Can't use ORDER clause with this procedure
    ER_ORDER_WITH_PROC = 1386,
    /// Binary logging and replication forbid changing the global server %s
    ER_LOGGING_PROHIBIT_CHANGING_OF = 1387,
    /// Can't map file: %s, errno: %d
    ER_NO_FILE_MAPPING = 1388,
    /// Wrong magic in %s
    ER_WRONG_MAGIC = 1389,
    /// Prepared statement contains too many placeholders
    ER_PS_MANY_PARAM = 1390,
    /// Key part '%s' length cannot be 0
    ER_KEY_PART_0 = 1391,
    /// View text checksum failed
    ER_VIEW_CHECKSUM = 1392,
    /// Can not modify more than one base table through a join view '%s.%s'
    ER_VIEW_MULTIUPDATE = 1393,
    /// Can not insert into join view '%s.%s' without fields list
    ER_VIEW_NO_INSERT_FIELD_LIST = 1394,
    /// Can not delete from join view '%s.%s'
    ER_VIEW_DELETE_MERGE_VIEW = 1395,
    /// Operation %s failed for %s
    ER_CANNOT_USER = 1396,
    /// XAER_NOTA: Unknown XID
    ER_XAER_NOTA = 1397,
    /// XAER_INVAL: Invalid arguments (or unsupported command)
    ER_XAER_INVAL = 1398,
    /// XAER_RMFAIL: The command cannot be executed when global transaction is in the %s state
    ER_XAER_RMFAIL = 1399,
    /// XAER_OUTSIDE: Some work is done outside global transaction
    ER_XAER_OUTSIDE = 1400,
    /// XAER_RMERR: Fatal error occurred in the transaction branch - check your data for
    /// consistency
    ER_XAER_RMERR = 1401,
    /// XA_RBROLLBACK: Transaction branch was rolled back
    ER_XA_RBROLLBACK = 1402,
    /// There is no such grant defined for user '%s' on host '%s' on routine '%s'
    ER_NONEXISTING_PROC_GRANT = 1403,
    /// Failed to grant EXECUTE and ALTER ROUTINE privileges
    ER_PROC_AUTO_GRANT_FAIL = 1404,
    /// Failed to revoke all privileges to dropped routine
    ER_PROC_AUTO_REVOKE_FAIL = 1405,
    /// Data too long for column '%s' at row %ld
    ER_DATA_TOO_LONG = 1406,
    /// Bad SQLSTATE: '%s'
    ER_SP_BAD_SQLSTATE = 1407,
    /// %s: ready for connections. Version: '%s' socket: '%s' port: %d %s
    ER_STARTUP = 1408,
    /// Can't load value from file with fixed size rows to variable
    ER_LOAD_FROM_FIXED_SIZE_ROWS_TO_VAR = 1409,
    /// You are not allowed to create a user with GRANT
    ER_CANT_CREATE_USER_WITH_GRANT = 1410,
    /// Incorrect %s value: '%s' for function %s
    ER_WRONG_VALUE_FOR_TYPE = 1411,
    /// Table definition has changed, please retry transaction
    ER_TABLE_DEF_CHANGED = 1412,
    /// Duplicate handler declared in the same block
    ER_SP_DUP_HANDLER = 1413,
    /// OUT or INOUT argument %d for routine %s is not a variable or NEW pseudo-variable in BEFORE
    /// trigger
    ER_SP_NOT_VAR_ARG = 1414,
    /// Not allowed to return a result set from a %s
    ER_SP_NO_RETSET = 1415,
    /// Cannot get geometry object from data you send to the GEOMETRY field
    ER_CANT_CREATE_GEOMETRY_OBJECT = 1416,
    /// A routine failed and has neither NO SQL nor READS SQL DATA in its declaration and binary
    /// logging is enabled; if non-transactional tables were updated, the binary log will miss
    /// their changes
    ER_FAILED_ROUTINE_BREAK_BINLOG = 1417,
    /// This function has none of DETERMINISTIC, NO SQL, or READS SQL DATA in its declaration and
    /// binary logging is enabled (you *might* want to use the less safe
    /// log_bin_trust_function_creators variable)
    ER_BINLOG_UNSAFE_ROUTINE = 1418,
    /// You do not have the SUPER privilege and binary logging is enabled (you *might* want to use
    /// the less safe log_bin_trust_function_creators variable)
    ER_BINLOG_CREATE_ROUTINE_NEED_SUPER = 1419,
    /// You can't execute a prepared statement which has an open cursor associated with it. Reset
    /// the statement to re-execute it.
    ER_EXEC_STMT_WITH_OPEN_CURSOR = 1420,
    /// The statement (%lu) has no open cursor.
    ER_STMT_HAS_NO_OPEN_CURSOR = 1421,
    /// Explicit or implicit commit is not allowed in stored function or trigger.
    ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG = 1422,
    /// Field of view '%s.%s' underlying table doesn't have a default value
    ER_NO_DEFAULT_FOR_VIEW_FIELD = 1423,
    /// Recursive stored functions and triggers are not allowed.
    ER_SP_NO_RECURSION = 1424,
    /// Too big scale %d specified for column '%s'. Maximum is %lu.
    ER_TOO_BIG_SCALE = 1425,
    /// Too big precision %d specified for column '%s'. Maximum is %lu.
    ER_TOO_BIG_PRECISION = 1426,
    /// For float(M,D, double(M,D or decimal(M,D, M must be >= D (column '%s').
    ER_M_BIGGER_THAN_D = 1427,
    /// You can't combine write-locking of system tables with other tables or lock types
    ER_WRONG_LOCK_OF_SYSTEM_TABLE = 1428,
    /// Unable to connect to foreign data source: %s
    ER_CONNECT_TO_FOREIGN_DATA_SOURCE = 1429,
    /// There was a problem processing the query on the foreign data source. Data source error: %s
    ER_QUERY_ON_FOREIGN_DATA_SOURCE = 1430,
    /// The foreign data source you are trying to reference does not exist. Data source error: %s
    ER_FOREIGN_DATA_SOURCE_DOESNT_EXIST = 1431,
    /// Can't create federated table. The data source connection string '%s' is not in the correct
    /// format
    ER_FOREIGN_DATA_STRING_INVALID_CANT_CREATE = 1432,
    /// The data source connection string '%s' is not in the correct format
    ER_FOREIGN_DATA_STRING_INVALID = 1433,
    /// Can't create federated table. Foreign data src error: %s
    ER_CANT_CREATE_FEDERATED_TABLE = 1434,
    /// Trigger in wrong schema
    ER_TRG_IN_WRONG_SCHEMA = 1435,
    /// Thread stack overrun: %ld bytes used of a %ld byte stack, and %ld bytes needed. Use 'mysqld
    /// --thread_stack=#' to specify a bigger stack.
    ER_STACK_OVERRUN_NEED_MORE = 1436,
    /// Routine body for '%s' is too long
    ER_TOO_LONG_BODY = 1437,
    /// Cannot drop default keycache
    ER_WARN_CANT_DROP_DEFAULT_KEYCACHE = 1438,
    /// Display width out of range for column '%s' (max = %lu)
    ER_TOO_BIG_DISPLAYWIDTH = 1439,
    /// XAER_DUPID: The XID already exists
    ER_XAER_DUPID = 1440,
    /// Datetime function: %s field overflow
    ER_DATETIME_FUNCTION_OVERFLOW = 1441,
    /// Can't update table '%s' in stored function/trigger because it is already used by statement
    /// which invoked this stored function/trigger.
    ER_CANT_UPDATE_USED_TABLE_IN_SF_OR_TRG = 1442,
    /// The definition of table '%s' prevents operation %s on table '%s'.
    ER_VIEW_PREVENT_UPDATE = 1443,
    /// The prepared statement contains a stored routine call that refers to that same statement.
    /// It's not allowed to execute a prepared statement in such a recursive manner
    ER_PS_NO_RECURSION = 1444,
    /// Not allowed to set autocommit from a stored function or trigger
    ER_SP_CANT_SET_AUTOCOMMIT = 1445,
    /// Definer is not fully qualified
    ER_MALFORMED_DEFINER = 1446,
    /// View '%s'.'%s' has no definer information (old table format). Current user is used as
    /// definer. Please recreate the view!
    ER_VIEW_FRM_NO_USER = 1447,
    /// You need the SUPER privilege for creation view with '%s'@'%s' definer
    ER_VIEW_OTHER_USER = 1448,
    /// The user specified as a definer ('%s'@'%s') does not exist
    ER_NO_SUCH_USER = 1449,
    /// Changing schema from '%s' to '%s' is not allowed.
    ER_FORBID_SCHEMA_CHANGE = 1450,
    /// Cannot delete or update a parent row: a foreign key constraint fails (%s)
    ER_ROW_IS_REFERENCED_2 = 1451,
    /// Cannot add or update a child row: a foreign key constraint fails (%s)
    ER_NO_REFERENCED_ROW_2 = 1452,
    /// Variable '%s' must be quoted with `...`, or renamed
    ER_SP_BAD_VAR_SHADOW = 1453,
    /// No definer attribute for trigger '%s'.'%s'. The trigger will be activated under the
    /// authorization of the invoker, which may have insufficient privileges. Please recreate the
    /// trigger.
    ER_TRG_NO_DEFINER = 1454,
    /// '%s' has an old format, you should re-create the '%s' object(s)
    ER_OLD_FILE_FORMAT = 1455,
    /// Recursive limit %d (as set by the max_sp_recursion_depth variable) was exceeded for routine
    /// %s
    ER_SP_RECURSION_LIMIT = 1456,
    /// Failed to load routine %s. The table mysql.proc is missing, corrupt, or contains bad data
    /// (internal code %d)
    ER_SP_PROC_TABLE_CORRUPT = 1457,
    /// Incorrect routine name '%s'
    ER_SP_WRONG_NAME = 1458,
    /// Table upgrade required. Please do "REPAIR TABLE `%s`" or dump/reload to fix it!
    ER_TABLE_NEEDS_UPGRADE = 1459,
    /// AGGREGATE is not supported for stored functions
    ER_SP_NO_AGGREGATE = 1460,
    /// Can't create more than max_prepared_stmt_count statements (current value: %lu)
    ER_MAX_PREPARED_STMT_COUNT_REACHED = 1461,
    /// `%s`.`%s` contains view recursion
    ER_VIEW_RECURSIVE = 1462,
    /// Non-grouping field '%s' is used in %s clause
    ER_NON_GROUPING_FIELD_USED = 1463,
    /// The used table type doesn't support SPATIAL indexes
    ER_TABLE_CANT_HANDLE_SPKEYS = 1464,
    /// Triggers can not be created on system tables
    ER_NO_TRIGGERS_ON_SYSTEM_SCHEMA = 1465,
    /// Leading spaces are removed from name '%s'
    ER_REMOVED_SPACES = 1466,
    /// Failed to read auto-increment value from storage engine
    ER_AUTOINC_READ_FAILED = 1467,
    /// user name
    ER_USERNAME = 1468,
    /// host name
    ER_HOSTNAME = 1469,
    /// String '%s' is too long for %s (should be no longer than %d)
    ER_WRONG_STRING_LENGTH = 1470,
    /// The target table %s of the %s is not insertable-into
    ER_NON_INSERTABLE_TABLE = 1471,
    /// Table '%s' is differently defined or of non-MyISAM type or doesn't exist
    ER_ADMIN_WRONG_MRG_TABLE = 1472,
    /// Too high level of nesting for select
    ER_TOO_HIGH_LEVEL_OF_NESTING_FOR_SELECT = 1473,
    /// Name '%s' has become ''
    ER_NAME_BECOMES_EMPTY = 1474,
    /// First character of the FIELDS TERMINATED string is ambiguous; please use non-optional and
    /// non-empty FIELDS ENCLOSED BY
    ER_AMBIGUOUS_FIELD_TERM = 1475,
    /// The foreign server, %s, you are trying to create already exists.
    ER_FOREIGN_SERVER_EXISTS = 1476,
    /// The foreign server name you are trying to reference does not exist. Data source error: %s
    ER_FOREIGN_SERVER_DOESNT_EXIST = 1477,
    /// Table storage engine '%s' does not support the create option '%s'
    ER_ILLEGAL_HA_CREATE_OPTION = 1478,
    /// Syntax error: %s PARTITIONING requires definition of VALUES %s for each partition
    ER_PARTITION_REQUIRES_VALUES_ERROR = 1479,
    /// Only %s PARTITIONING can use VALUES %s in partition definition
    ER_PARTITION_WRONG_VALUES_ERROR = 1480,
    /// MAXVALUE can only be used in last partition definition
    ER_PARTITION_MAXVALUE_ERROR = 1481,
    /// Subpartitions can only be hash partitions and by key
    ER_PARTITION_SUBPARTITION_ERROR = 1482,
    /// Must define subpartitions on all partitions if on one partition
    ER_PARTITION_SUBPART_MIX_ERROR = 1483,
    /// Wrong number of partitions defined, mismatch with previous setting
    ER_PARTITION_WRONG_NO_PART_ERROR = 1484,
    /// Wrong number of subpartitions defined, mismatch with previous setting
    ER_PARTITION_WRONG_NO_SUBPART_ERROR = 1485,
    /// Constant, random or timezone-dependent expressions in (sub)partitioning function are not
    /// allowed
    ER_WRONG_EXPR_IN_PARTITION_FUNC_ERROR = 1486,
    /// Expression in RANGE/LIST VALUES must be constant
    ER_NO_CONST_EXPR_IN_RANGE_OR_LIST_ERROR = 1487,
    /// Field in list of fields for partition function not found in table
    ER_FIELD_NOT_FOUND_PART_ERROR = 1488,
    /// List of fields is only allowed in KEY partitions
    ER_LIST_OF_FIELDS_ONLY_IN_HASH_ERROR = 1489,
    /// The partition info in the frm file is not consistent with what can be written into the frm
    /// file
    ER_INCONSISTENT_PARTITION_INFO_ERROR = 1490,
    /// The %s function returns the wrong type
    ER_PARTITION_FUNC_NOT_ALLOWED_ERROR = 1491,
    /// For %s partitions each partition must be defined
    ER_PARTITIONS_MUST_BE_DEFINED_ERROR = 1492,
    /// VALUES LESS THAN value must be strictly increasing for each partition
    ER_RANGE_NOT_INCREASING_ERROR = 1493,
    /// VALUES value must be of same type as partition function
    ER_INCONSISTENT_TYPE_OF_FUNCTIONS_ERROR = 1494,
    /// Multiple definition of same constant in list partitioning
    ER_MULTIPLE_DEF_CONST_IN_LIST_PART_ERROR = 1495,
    /// Partitioning can not be used stand-alone in query
    ER_PARTITION_ENTRY_ERROR = 1496,
    /// The mix of handlers in the partitions is not allowed in this version of MariaDB
    ER_MIX_HANDLER_ERROR = 1497,
    /// For the partitioned engine it is necessary to define all %s
    ER_PARTITION_NOT_DEFINED_ERROR = 1498,
    /// Too many partitions (including subpartitions) were defined
    ER_TOO_MANY_PARTITIONS_ERROR = 1499,
    /// It is only possible to mix RANGE/LIST partitioning with HASH/KEY partitioning for
    /// subpartitioning
    ER_SUBPARTITION_ERROR = 1500,
    /// Failed to create specific handler file
    ER_CANT_CREATE_HANDLER_FILE = 1501,
    /// A BLOB field is not allowed in partition function
    ER_BLOB_FIELD_IN_PART_FUNC_ERROR = 1502,
    /// A %s must include all columns in the table's partitioning function
    ER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF = 1503,
    /// Number of %s = 0 is not an allowed value
    ER_NO_PARTS_ERROR = 1504,
    /// Partition management on a not partitioned table is not possible
    ER_PARTITION_MGMT_ON_NONPARTITIONED = 1505,
    /// Foreign key clause is not yet supported in conjunction with partitioning
    ER_FOREIGN_KEY_ON_PARTITIONED = 1506,
    /// Error in list of partitions to %s
    ER_DROP_PARTITION_NON_EXISTENT = 1507,
    /// Cannot remove all partitions, use DROP TABLE instead
    ER_DROP_LAST_PARTITION = 1508,
    /// COALESCE PARTITION can only be used on HASH/KEY partitions
    ER_COALESCE_ONLY_ON_HASH_PARTITION = 1509,
    /// REORGANIZE PARTITION can only be used to reorganize partitions not to change their numbers
    ER_REORG_HASH_ONLY_ON_SAME_N = 1510,
    /// REORGANIZE PARTITION without parameters can only be used on auto-partitioned tables using
    /// HASH PARTITIONs
    ER_REORG_NO_PARAM_ERROR = 1511,
    /// %s PARTITION can only be used on RANGE/LIST partitions
    ER_ONLY_ON_RANGE_LIST_PARTITION = 1512,
    /// Trying to Add partition(s) with wrong number of subpartitions
    ER_ADD_PARTITION_SUBPART_ERROR = 1513,
    /// At least one partition must be added
    ER_ADD_PARTITION_NO_NEW_PARTITION = 1514,
    /// At least one partition must be coalesced
    ER_COALESCE_PARTITION_NO_PARTITION = 1515,
    /// More partitions to reorganize than there are partitions
    ER_REORG_PARTITION_NOT_EXIST = 1516,
    /// Duplicate partition name %s
    ER_SAME_NAME_PARTITION = 1517,
    /// It is not allowed to shut off binlog on this command
    ER_NO_BINLOG_ERROR = 1518,
    /// When reorganizing a set of partitions they must be in consecutive order
    ER_CONSECUTIVE_REORG_PARTITIONS = 1519,
    /// Reorganize of range partitions cannot change total ranges except for last partition where
    /// it can extend the range
    ER_REORG_OUTSIDE_RANGE = 1520,
    /// Partition function not supported in this version for this handler
    ER_PARTITION_FUNCTION_FAILURE = 1521,
    /// Partition state cannot be defined from CREATE/ALTER TABLE
    ER_PART_STATE_ERROR = 1522,
    /// The %s handler only supports 32 bit integers in VALUES
    ER_LIMITED_PART_RANGE = 1523,
    /// Plugin '%s' is not loaded
    ER_PLUGIN_IS_NOT_LOADED = 1524,
    /// Incorrect %s value: '%s'
    ER_WRONG_VALUE = 1525,
    /// Table has no partition for value %s
    ER_NO_PARTITION_FOR_GIVEN_VALUE = 1526,
    /// It is not allowed to specify %s more than once
    ER_FILEGROUP_OPTION_ONLY_ONCE = 1527,
    /// Failed to create %s
    ER_CREATE_FILEGROUP_FAILED = 1528,
    /// Failed to drop %s
    ER_DROP_FILEGROUP_FAILED = 1529,
    /// The handler doesn't support autoextend of tablespaces
    ER_TABLESPACE_AUTO_EXTEND_ERROR = 1530,
    /// A size parameter was incorrectly specified, either number or on the form 10M
    ER_WRONG_SIZE_NUMBER = 1531,
    /// The size number was correct but we don't allow the digit part to be more than 2 billion
    ER_SIZE_OVERFLOW_ERROR = 1532,
    /// Failed to alter: %s
    ER_ALTER_FILEGROUP_FAILED = 1533,
    /// Writing one row to the row-based binary log failed
    ER_BINLOG_ROW_LOGGING_FAILED = 1534,
    /// Table definition on master and slave does not match: %s
    ER_BINLOG_ROW_WRONG_TABLE_DEF = 1535,
    /// Slave running with --log-slave-updates must use row-based binary logging to be able to
    /// replicate row-based binary log events
    ER_BINLOG_ROW_RBR_TO_SBR = 1536,
    /// Event '%s' already exists
    ER_EVENT_ALREADY_EXISTS = 1537,
    /// Failed to store event %s. Error code %d from storage engine.
    ER_EVENT_STORE_FAILED = 1538,
    /// Unknown event '%s'
    ER_EVENT_DOES_NOT_EXIST = 1539,
    /// Failed to alter event '%s'
    ER_EVENT_CANT_ALTER = 1540,
    /// Failed to drop %s
    ER_EVENT_DROP_FAILED = 1541,
    /// INTERVAL is either not positive or too big
    ER_EVENT_INTERVAL_NOT_POSITIVE_OR_TOO_BIG = 1542,
    /// ENDS is either invalid or before STARTS
    ER_EVENT_ENDS_BEFORE_STARTS = 1543,
    /// Event execution time is in the past. Event has been disabled
    ER_EVENT_EXEC_TIME_IN_THE_PAST = 1544,
    /// Failed to open mysql.event
    ER_EVENT_OPEN_TABLE_FAILED = 1545,
    /// No datetime expression provided
    ER_EVENT_NEITHER_M_EXPR_NOR_M_AT = 1546,
    /// Column count of mysql.%s is wrong. Expected %d, found %d. The table is probably corrupted
    ER_COL_COUNT_DOESNT_MATCH_CORRUPTED = 1547,
    /// Cannot load from mysql.%s. The table is probably corrupted
    ER_CANNOT_LOAD_FROM_TABLE = 1548,
    /// Failed to delete the event from mysql.event
    ER_EVENT_CANNOT_DELETE = 1549,
    /// Error during compilation of event's body
    ER_EVENT_COMPILE_ERROR = 1550,
    /// Same old and new event name
    ER_EVENT_SAME_NAME = 1551,
    /// Data for column '%s' too long
    ER_EVENT_DATA_TOO_LONG = 1552,
    /// Cannot drop index '%s': needed in a foreign key constraint
    ER_DROP_INDEX_FK = 1553,
    /// The syntax '%s' is deprecated and will be removed in MariaDB %s. Please use %s instead
    ER_WARN_DEPRECATED_SYNTAX_WITH_VER = 1554,
    /// You can't write-lock a log table. Only read access is possible
    ER_CANT_WRITE_LOCK_LOG_TABLE = 1555,
    /// You can't use locks with log tables.
    ER_CANT_LOCK_LOG_TABLE = 1556,
    /// Upholding foreign key constraints for table '%s', entry '%s', key %d would lead to a
    /// duplicate entry
    ER_FOREIGN_DUPLICATE_KEY = 1557,
    /// Column count of mysql.%s is wrong. Expected %d, found %d. Created with MariaDB %d, now
    /// running %d. Please use mysql_upgrade to fix this error.
    ER_COL_COUNT_DOESNT_MATCH_PLEASE_UPDATE = 1558,
    /// Cannot switch out of the row-based binary log format when the session has open temporary
    /// tables
    ER_TEMP_TABLE_PREVENTS_SWITCH_OUT_OF_RBR = 1559,
    /// Cannot change the binary logging format inside a stored function or trigger
    ER_STORED_FUNCTION_PREVENTS_SWITCH_BINLOG_FORMAT = 1560,
    /// The NDB cluster engine does not support changing the binlog format on the fly yet
    ER_NDB_CANT_SWITCH_BINLOG_FORMAT = 1561,
    /// Cannot create temporary table with partitions
    ER_PARTITION_NO_TEMPORARY = 1562,
    /// Partition constant is out of partition function domain
    ER_PARTITION_CONST_DOMAIN_ERROR = 1563,
    /// This partition function is not allowed
    ER_PARTITION_FUNCTION_IS_NOT_ALLOWED = 1564,
    /// Error in DDL log
    ER_DDL_LOG_ERROR = 1565,
    /// Not allowed to use NULL value in VALUES LESS THAN
    ER_NULL_IN_VALUES_LESS_THAN = 1566,
    /// Incorrect partition name
    ER_WRONG_PARTITION_NAME = 1567,
    /// Transaction isolation level can't be changed while a transaction is in progress
    ER_CANT_CHANGE_TX_ISOLATION = 1568,
    /// ALTER TABLE causes auto_increment resequencing, resulting in duplicate entry '%s' for key
    /// '%s'
    ER_DUP_ENTRY_AUTOINCREMENT_CASE = 1569,
    /// Internal scheduler error %d
    ER_EVENT_MODIFY_QUEUE_ERROR = 1570,
    /// Error during starting/stopping of the scheduler. Error code %u
    ER_EVENT_SET_VAR_ERROR = 1571,
    /// Engine cannot be used in partitioned tables
    ER_PARTITION_MERGE_ERROR = 1572,
    /// Cannot activate '%s' log
    ER_CANT_ACTIVATE_LOG = 1573,
    /// The server was not built with row-based replication
    ER_RBR_NOT_AVAILABLE = 1574,
    /// Decoding of base64 string failed
    ER_BASE64_DECODE_ERROR = 1575,
    /// Recursion of EVENT DDL statements is forbidden when body is present
    ER_EVENT_RECURSION_FORBIDDEN = 1576,
    /// Cannot proceed because system tables used by Event Scheduler were found damaged at server
    /// start
    ER_EVENTS_DB_ERROR = 1577,
    /// Only integers allowed as number here
    ER_ONLY_INTEGERS_ALLOWED = 1578,
    /// This storage engine cannot be used for log tables"
    ER_UNSUPORTED_LOG_ENGINE = 1579,
    /// You cannot '%s' a log table if logging is enabled
    ER_BAD_LOG_STATEMENT = 1580,
    /// Cannot rename '%s'. When logging enabled, rename to/from log table must rename two tables:
    /// the log table to an archive table and another table back to '%s'
    ER_CANT_RENAME_LOG_TABLE = 1581,
    /// Incorrect parameter count in the call to native function '%s'
    ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT = 1582,
    /// Incorrect parameters in the call to native function '%s'
    ER_WRONG_PARAMETERS_TO_NATIVE_FCT = 1583,
    /// Incorrect parameters in the call to stored function '%s'
    ER_WRONG_PARAMETERS_TO_STORED_FCT = 1584,
    /// This function '%s' has the same name as a native function
    ER_NATIVE_FCT_NAME_COLLISION = 1585,
    /// Duplicate entry '%s' for key '%s'
    ER_DUP_ENTRY_WITH_KEY_NAME = 1586,
    /// Too many files opened, please execute the command again
    ER_BINLOG_PURGE_EMFILE = 1587,
    /// Event execution time is in the past and ON COMPLETION NOT PRESERVE is set. The event was
    /// dropped immediately after creation.
    ER_EVENT_CANNOT_CREATE_IN_THE_PAST = 1588,
    /// Event execution time is in the past and ON COMPLETION NOT PRESERVE is set. The event was
    /// dropped immediately after creation.
    ER_EVENT_CANNOT_ALTER_IN_THE_PAST = 1589,
    /// The incident %s occured on the master. Message: %s
    ER_SLAVE_INCIDENT = 1590,
    /// Table has no partition for some existing values
    ER_NO_PARTITION_FOR_GIVEN_VALUE_SILENT = 1591,
    /// Unsafe statement written to the binary log using statement format since BINLOG_FORMAT =
    /// STATEMENT. %s
    ER_BINLOG_UNSAFE_STATEMENT = 1592,
    /// Fatal error: %s
    ER_SLAVE_FATAL_ERROR = 1593,
    /// Relay log read failure: %s
    ER_SLAVE_RELAY_LOG_READ_FAILURE = 1594,
    /// Relay log write failure: %s
    ER_SLAVE_RELAY_LOG_WRITE_FAILURE = 1595,
    /// Failed to create %s
    ER_SLAVE_CREATE_EVENT_FAILURE = 1596,
    /// Master command %s failed: %s
    ER_SLAVE_MASTER_COM_FAILURE = 1597,
    /// Binary logging not possible. Message: %s
    ER_BINLOG_LOGGING_IMPOSSIBLE = 1598,
    /// View `%s`.`%s` has no creation context
    ER_VIEW_NO_CREATION_CTX = 1599,
    /// Creation context of view `%s`.`%s' is invalid
    ER_VIEW_INVALID_CREATION_CTX = 1600,
    /// Creation context of stored routine `%s`.`%s` is invalid
    ER_SR_INVALID_CREATION_CTX = 1601,
    /// Corrupted TRG file for table `%s`.`%s`
    ER_TRG_CORRUPTED_FILE = 1602,
    /// Triggers for table `%s`.`%s` have no creation context
    ER_TRG_NO_CREATION_CTX = 1603,
    /// Trigger creation context of table `%s`.`%s` is invalid
    ER_TRG_INVALID_CREATION_CTX = 1604,
    /// Creation context of event `%s`.`%s` is invalid
    ER_EVENT_INVALID_CREATION_CTX = 1605,
    /// Cannot open table for trigger `%s`.`%s`
    ER_TRG_CANT_OPEN_TABLE = 1606,
    /// Cannot create stored routine `%s`. Check warnings
    ER_CANT_CREATE_SROUTINE = 1607,
    /// You should never see it
    ER_UNUSED_11 = 1608,
    /// The BINLOG statement of type `%s` was not preceded by a format description BINLOG
    /// statement.
    ER_NO_FORMAT_DESCRIPTION_EVENT_BEFORE_BINLOG_STATEMENT = 1609,
    /// Corrupted replication event was detected
    ER_SLAVE_CORRUPT_EVENT = 1610,
    /// Invalid column reference (%s) in LOAD DATA
    ER_LOAD_DATA_INVALID_COLUMN = 1611,
    /// Being purged log %s was not found
    ER_LOG_PURGE_NO_FILE = 1612,
    /// XA_RBTIMEOUT: Transaction branch was rolled back: took too long
    ER_XA_RBTIMEOUT = 1613,
    /// XA_RBDEADLOCK: Transaction branch was rolled back: deadlock was detected
    ER_XA_RBDEADLOCK = 1614,
    /// Prepared statement needs to be re-prepared
    ER_NEED_REPREPARE = 1615,
    /// DELAYED option not supported for table '%s'
    ER_DELAYED_NOT_SUPPORTED = 1616,
    /// The master info structure does not exist
    WARN_NO_MASTER_INF = 1617,
    /// <%s> option ignored
    WARN_OPTION_IGNORED = 1618,
    /// Built-in plugins cannot be deleted
    WARN_PLUGIN_DELETE_BUILTIN = 1619,
    /// Plugin is busy and will be uninstalled on shutdown
    WARN_PLUGIN_BUSY = 1620,
    /// %s variable '%s' is read-only. Use SET %s to assign the value
    ER_VARIABLE_IS_READONLY = 1621,
    /// Storage engine %s does not support rollback for this statement. Transaction rolled back and
    /// must be restarted
    ER_WARN_ENGINE_TRANSACTION_ROLLBACK = 1622,
    /// Unexpected master's heartbeat data: %s
    ER_SLAVE_HEARTBEAT_FAILURE = 1623,
    /// The requested value for the heartbeat period is either negative or exceeds the maximum
    /// allowed (%s seconds).
    ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE = 1624,
    /// Bad schema for mysql.ndb_replication table. Message: %s
    ER_NDB_REPLICATION_SCHEMA_ERROR = 1625,
    /// Error in parsing conflict function. Message: %s
    ER_CONFLICT_FN_PARSE_ERROR = 1626,
    /// Write to exceptions table failed. Message: %s"
    ER_EXCEPTIONS_WRITE_ERROR = 1627,
    /// Comment for table '%s' is too long (max = %lu)
    ER_TOO_LONG_TABLE_COMMENT = 1628,
    /// Comment for field '%s' is too long (max = %lu)
    ER_TOO_LONG_FIELD_COMMENT = 1629,
    /// FUNCTION %s does not exist. Check the 'Function Name Parsing and Resolution' section in the
    /// Reference Manual
    ER_FUNC_INEXISTENT_NAME_COLLISION = 1630,
    /// Database
    ER_DATABASE_NAME = 1631,
    /// Table
    ER_TABLE_NAME = 1632,
    /// Partition
    ER_PARTITION_NAME = 1633,
    /// Subpartition
    ER_SUBPARTITION_NAME = 1634,
    /// Temporary
    ER_TEMPORARY_NAME = 1635,
    /// Renamed
    ER_RENAMED_NAME = 1636,
    /// Too many active concurrent transactions
    ER_TOO_MANY_CONCURRENT_TRXS = 1637,
    /// Non-ASCII separator arguments are not fully supported
    WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED = 1638,
    /// debug sync point wait timed out
    ER_DEBUG_SYNC_TIMEOUT = 1639,
    /// debug sync point hit limit reached
    ER_DEBUG_SYNC_HIT_LIMIT = 1640,
    /// Duplicate condition information item '%s'
    ER_DUP_SIGNAL_SET = 1641,
    /// Unhandled user-defined warning condition
    ER_SIGNAL_WARN = 1642,
    /// Unhandled user-defined not found condition
    ER_SIGNAL_NOT_FOUND = 1643,
    /// Unhandled user-defined exception condition
    ER_SIGNAL_EXCEPTION = 1644,
    /// RESIGNAL when handler not active
    ER_RESIGNAL_WITHOUT_ACTIVE_HANDLER = 1645,
    /// SIGNAL/RESIGNAL can only use a CONDITION defined with SQLSTATE
    ER_SIGNAL_BAD_CONDITION_TYPE = 1646,
    /// Data truncated for condition item '%s'
    WARN_COND_ITEM_TRUNCATED = 1647,
    /// Data too long for condition item '%s'
    ER_COND_ITEM_TOO_LONG = 1648,
    /// Unknown locale: '%s'
    ER_UNKNOWN_LOCALE = 1649,
    /// The requested server id %d clashes with the slave startup option --replicate-same-server-id
    ER_SLAVE_IGNORE_SERVER_IDS = 1650,
    /// Query cache is disabled; restart the server with query_cache_type=1 to enable it
    ER_QUERY_CACHE_DISABLED = 1651,
    /// Duplicate partition field name '%s'
    ER_SAME_NAME_PARTITION_FIELD = 1652,
    /// Inconsistency in usage of column lists for partitioning
    ER_PARTITION_COLUMN_LIST_ERROR = 1653,
    /// Partition column values of incorrect type
    ER_WRONG_TYPE_COLUMN_VALUE_ERROR = 1654,
    /// Too many fields in '%s'
    ER_TOO_MANY_PARTITION_FUNC_FIELDS_ERROR = 1655,
    /// Cannot use MAXVALUE as value in VALUES IN
    ER_MAXVALUE_IN_VALUES_IN = 1656,
    /// Cannot have more than one value for this type of %s partitioning
    ER_TOO_MANY_VALUES_ERROR = 1657,
    /// Row expressions in VALUES IN only allowed for multi-field column partitioning
    ER_ROW_SINGLE_PARTITION_FIELD_ERROR = 1658,
    /// Field '%s' is of a not allowed type for this type of partitioning
    ER_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD = 1659,
    /// The total length of the partitioning fields is too large
    ER_PARTITION_FIELDS_TOO_LONG = 1660,
    /// Cannot execute statement: impossible to write to binary log since both row-incapable
    /// engines and statement-incapable engines are involved.
    ER_BINLOG_ROW_ENGINE_AND_STMT_ENGINE = 1661,
    /// Cannot execute statement: impossible to write to binary log since BINLOG_FORMAT = ROW and
    /// at least one table uses a storage engine limited to statement-based logging.
    ER_BINLOG_ROW_MODE_AND_STMT_ENGINE = 1662,
    /// Cannot execute statement: impossible to write to binary log since statement is unsafe,
    /// storage engine is limited to statement-based logging, and BINLOG_FORMAT = MIXED. %s
    ER_BINLOG_UNSAFE_AND_STMT_ENGINE = 1663,
    /// Cannot execute statement: impossible to write to binary log since statement is in row
    /// format and at least one table uses a storage engine limited to statement-based logging.
    ER_BINLOG_ROW_INJECTION_AND_STMT_ENGINE = 1664,
    /// Cannot execute statement: impossible to write to binary log since BINLOG_FORMAT = STATEMENT
    /// and at least one table uses a storage engine limited to row-based logging.%s
    ER_BINLOG_STMT_MODE_AND_ROW_ENGINE = 1665,
    /// Cannot execute statement: impossible to write to binary log since statement is in row
    /// format and BINLOG_FORMAT = STATEMENT.
    ER_BINLOG_ROW_INJECTION_AND_STMT_MODE = 1666,
    /// Cannot execute statement: impossible to write to binary log since more than one engine is
    /// involved and at least one engine is self-logging.
    ER_BINLOG_MULTIPLE_ENGINES_AND_SELF_LOGGING_ENGINE = 1667,
    /// The statement is unsafe because it uses a LIMIT clause. This is unsafe because the set of
    /// rows included cannot be predicted.
    ER_BINLOG_UNSAFE_LIMIT = 1668,
    /// The statement is unsafe because it uses INSERT DELAYED. This is unsafe because the times
    /// when rows are inserted cannot be predicted.
    ER_BINLOG_UNSAFE_INSERT_DELAYED = 1669,
    /// The statement is unsafe because it uses the general log, slow query log, or
    /// performance_schema table(s). This is unsafe because system tables may differ on slaves.
    ER_BINLOG_UNSAFE_SYSTEM_TABLE = 1670,
    /// Statement is unsafe because it invokes a trigger or a stored function that inserts into an
    /// AUTO_INCREMENT column. Inserted values cannot be logged correctly.
    ER_BINLOG_UNSAFE_AUTOINC_COLUMNS = 1671,
    /// Statement is unsafe because it uses a UDF which may not return the same value on the slave.
    ER_BINLOG_UNSAFE_UDF = 1672,
    /// Statement is unsafe because it uses a system variable that may have a different value on
    /// the slave.
    ER_BINLOG_UNSAFE_SYSTEM_VARIABLE = 1673,
    /// Statement is unsafe because it uses a system function that may return a different value on
    /// the slave.
    ER_BINLOG_UNSAFE_SYSTEM_FUNCTION = 1674,
    /// Statement is unsafe because it accesses a non-transactional table after accessing a
    /// transactional table within the same transaction.
    ER_BINLOG_UNSAFE_NONTRANS_AFTER_TRANS = 1675,
    /// %s Statement: %s
    ER_MESSAGE_AND_STATEMENT = 1676,
    /// Column %d of table '%s.%s' cannot be converted from type '%s' to type '%s'
    ER_SLAVE_CONVERSION_FAILED = 1677,
    /// Can't create conversion table for table '%s.%s'
    ER_SLAVE_CANT_CREATE_CONVERSION = 1678,
    /// Cannot modify @@session.binlog_format inside a transaction
    ER_INSIDE_TRANSACTION_PREVENTS_SWITCH_BINLOG_FORMAT = 1679,
    /// The path specified for %s is too long.
    ER_PATH_LENGTH = 1680,
    /// '%s' is deprecated and will be removed in a future release.
    ER_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT = 1681,
    /// Native table '%s'.'%s' has the wrong structure
    ER_WRONG_NATIVE_TABLE_STRUCTURE = 1682,
    /// Invalid performance_schema usage.
    ER_WRONG_PERFSCHEMA_USAGE = 1683,
    /// Table '%s'.'%s' was skipped since its definition is being modified by concurrent DDL
    /// statement
    ER_WARN_I_S_SKIPPED_TABLE = 1684,
    /// Cannot modify @@session.binlog_direct_non_transactional_updates inside a transaction
    ER_INSIDE_TRANSACTION_PREVENTS_SWITCH_BINLOG_DIRECT = 1685,
    /// Cannot change the binlog direct flag inside a stored function or trigger
    ER_STORED_FUNCTION_PREVENTS_SWITCH_BINLOG_DIRECT = 1686,
    /// A SPATIAL index may only contain a geometrical type column
    ER_SPATIAL_MUST_HAVE_GEOM_COL = 1687,
    /// Comment for index '%s' is too long (max = %lu)
    ER_TOO_LONG_INDEX_COMMENT = 1688,
    /// Wait on a lock was aborted due to a pending exclusive lock
    ER_LOCK_ABORTED = 1689,
    /// %s value is out of range in '%s'
    ER_DATA_OUT_OF_RANGE = 1690,
    /// A variable of a non-integer based type in LIMIT clause
    ER_WRONG_SPVAR_TYPE_IN_LIMIT = 1691,
    /// Mixing self-logging and non-self-logging engines in a statement is unsafe.
    ER_BINLOG_UNSAFE_MULTIPLE_ENGINES_AND_SELF_LOGGING_ENGINE = 1692,
    /// Statement accesses nontransactional table as well as transactional or temporary table, and
    /// writes to any of them.
    ER_BINLOG_UNSAFE_MIXED_STATEMENT = 1693,
    /// Cannot modify @@session.sql_log_bin inside a transaction
    ER_INSIDE_TRANSACTION_PREVENTS_SWITCH_SQL_LOG_BIN = 1694,
    /// Cannot change the sql_log_bin inside a stored function or trigger
    ER_STORED_FUNCTION_PREVENTS_SWITCH_SQL_LOG_BIN = 1695,
    /// Failed to read from the .par file
    ER_FAILED_READ_FROM_PAR_FILE = 1696,
    /// VALUES value for partition '%s' must have type INT
    ER_VALUES_IS_NOT_INT_TYPE_ERROR = 1697,
    /// Access denied for user '%s'@'%s'
    ER_ACCESS_DENIED_NO_PASSWORD_ERROR = 1698,
    /// SET PASSWORD has no significance for users authenticating via plugins
    ER_SET_PASSWORD_AUTH_PLUGIN = 1699,
    /// GRANT with IDENTIFIED WITH is illegal because the user %-.*s already exists
    ER_GRANT_PLUGIN_USER_EXISTS = 1700,
    /// Cannot truncate a table referenced in a foreign key constraint (%s)
    ER_TRUNCATE_ILLEGAL_FK = 1701,
    /// Plugin '%s' is force_plus_permanent and can not be unloaded
    ER_PLUGIN_IS_PERMANENT = 1702,
    /// The requested value for the heartbeat period is less than 1 millisecond. The value is reset
    /// to 0, meaning that heartbeating will effectively be disabled.
    ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE_MIN = 1703,
    /// The requested value for the heartbeat period exceeds the value of slave_net_timeout
    /// seconds. A sensible value for the period should be less than the timeout.
    ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE_MAX = 1704,
    /// Multi-row statements required more than 'max_binlog_stmt_cache_size' bytes of storage;
    /// increase this mysqld variable and try again
    ER_STMT_CACHE_FULL = 1705,
    /// Primary key/partition key update is not allowed since the table is updated both as '%s' and
    /// '%s'.
    ER_MULTI_UPDATE_KEY_CONFLICT = 1706,
    /// Table rebuild required. Please do "ALTER TABLE `%s` FORCE" or dump/reload to fix it!
    ER_TABLE_NEEDS_REBUILD = 1707,
    /// The value of '%s' should be no less than the value of '%s'
    WARN_OPTION_BELOW_LIMIT = 1708,
    /// Index column size too large. The maximum column size is %lu bytes.
    ER_INDEX_COLUMN_TOO_LONG = 1709,
    /// Trigger '%s' has an error in its body: '%s'
    ER_ERROR_IN_TRIGGER_BODY = 1710,
    /// Unknown trigger has an error in its body: '%s'
    ER_ERROR_IN_UNKNOWN_TRIGGER_BODY = 1711,
    /// Index %s is corrupted
    ER_INDEX_CORRUPT = 1712,
    /// Undo log record is too big.
    ER_UNDO_RECORD_TOO_BIG = 1713,
    /// INSERT IGNORE... SELECT is unsafe because the order in which rows are retrieved by the
    /// SELECT determines which (if any) rows are ignored. This order cannot be predicted and may
    /// differ on master and the slave.
    ER_BINLOG_UNSAFE_INSERT_IGNORE_SELECT = 1714,
    /// INSERT... SELECT... ON DUPLICATE KEY UPDATE is unsafe because the order in which rows are
    /// retrieved by the SELECT determines which (if any) rows are updated. This order cannot be
    /// predicted and may differ on master and the slave.
    ER_BINLOG_UNSAFE_INSERT_SELECT_UPDATE = 1715,
    /// REPLACE... SELECT is unsafe because the order in which rows are retrieved by the SELECT
    /// determines which (if any) rows are replaced. This order cannot be predicted and may differ
    /// on master and the slave.
    ER_BINLOG_UNSAFE_REPLACE_SELECT = 1716,
    /// CREATE... IGNORE SELECT is unsafe because the order in which rows are retrieved by the
    /// SELECT determines which (if any) rows are ignored. This order cannot be predicted and may
    /// differ on master and the slave.
    ER_BINLOG_UNSAFE_CREATE_IGNORE_SELECT = 1717,
    /// CREATE... REPLACE SELECT is unsafe because the order in which rows are retrieved by the
    /// SELECT determines which (if any) rows are replaced. This order cannot be predicted and may
    /// differ on master and the slave.
    ER_BINLOG_UNSAFE_CREATE_REPLACE_SELECT = 1718,
    /// UPDATE IGNORE is unsafe because the order in which rows are updated determines which (if
    /// any) rows are ignored. This order cannot be predicted and may differ on master and the
    /// slave.
    ER_BINLOG_UNSAFE_UPDATE_IGNORE = 1719,
    /// Plugin '%s' is marked as not dynamically uninstallable. You have to stop the server to
    /// uninstall it.
    ER_PLUGIN_NO_UNINSTALL = 1720,
    /// Plugin '%s' is marked as not dynamically installable. You have to stop the server to
    /// install it.
    ER_PLUGIN_NO_INSTALL = 1721,
    /// Statements writing to a table with an auto-increment column after selecting from another
    /// table are unsafe because the order in which rows are retrieved determines what (if any)
    /// rows will be written. This order cannot be predicted and may differ on master and the
    /// slave.
    ER_BINLOG_UNSAFE_WRITE_AUTOINC_SELECT = 1722,
    /// CREATE TABLE... SELECT... on a table with an auto-increment column is unsafe because the
    /// order in which rows are retrieved by the SELECT determines which (if any) rows are
    /// inserted. This order cannot be predicted and may differ on master and the slave.
    ER_BINLOG_UNSAFE_CREATE_SELECT_AUTOINC = 1723,
    /// INSERT... ON DUPLICATE KEY UPDATE on a table with more than one UNIQUE KEY is unsafe
    ER_BINLOG_UNSAFE_INSERT_TWO_KEYS = 1724,
    /// Table is being used in foreign key check.
    ER_TABLE_IN_FK_CHECK = 1725,
    /// Storage engine '%s' does not support system tables. [%s.%s]
    ER_UNSUPPORTED_ENGINE = 1726,
    /// INSERT into autoincrement field which is not the first part in the composed primary key is
    /// unsafe.
    ER_BINLOG_UNSAFE_AUTOINC_NOT_FIRST = 1727,
    /// Cannot load from %s.%s. The table is probably corrupted
    ER_CANNOT_LOAD_FROM_TABLE_V2 = 1728,
    /// The requested value %s for the master delay exceeds the maximum %u
    ER_MASTER_DELAY_VALUE_OUT_OF_RANGE = 1729,
    /// Only Format_description_log_event and row events are allowed in BINLOG statements (but %s
    /// was provided
    ER_ONLY_FD_AND_RBR_EVENTS_ALLOWED_IN_BINLOG_STATEMENT = 1730,
    /// Non matching attribute '%s' between partition and table
    ER_PARTITION_EXCHANGE_DIFFERENT_OPTION = 1731,
    /// Table to exchange with partition is partitioned: '%s'
    ER_PARTITION_EXCHANGE_PART_TABLE = 1732,
    /// Table to exchange with partition is temporary: '%s'
    ER_PARTITION_EXCHANGE_TEMP_TABLE = 1733,
    /// Subpartitioned table, use subpartition instead of partition
    ER_PARTITION_INSTEAD_OF_SUBPARTITION = 1734,
    /// Unknown partition '%s' in table '%s'
    ER_UNKNOWN_PARTITION = 1735,
    /// Tables have different definitions
    ER_TABLES_DIFFERENT_METADATA = 1736,
    /// Found a row that does not match the partition
    ER_ROW_DOES_NOT_MATCH_PARTITION = 1737,
    /// Option binlog_cache_size (%lu) is greater than max_binlog_cache_size (%lu); setting
    /// binlog_cache_size equal to max_binlog_cache_size.
    ER_BINLOG_CACHE_SIZE_GREATER_THAN_MAX = 1738,
    /// Cannot use %s access on index '%s' due to type or collation conversion on field '%s'
    ER_WARN_INDEX_NOT_APPLICABLE = 1739,
    /// Table to exchange with partition has foreign key references: '%s'
    ER_PARTITION_EXCHANGE_FOREIGN_KEY = 1740,
    /// Key value '%s' was not found in table '%s.%s'
    ER_NO_SUCH_KEY_VALUE = 1741,
    /// Data for column '%s' too long
    ER_RPL_INFO_DATA_TOO_LONG = 1742,
    /// Replication event checksum verification failed while reading from network.
    ER_NETWORK_READ_EVENT_CHECKSUM_FAILURE = 1743,
    /// Replication event checksum verification failed while reading from a log file.
    ER_BINLOG_READ_EVENT_CHECKSUM_FAILURE = 1744,
    /// Option binlog_stmt_cache_size (%lu) is greater than max_binlog_stmt_cache_size (%lu);
    /// setting binlog_stmt_cache_size equal to max_binlog_stmt_cache_size.
    ER_BINLOG_STMT_CACHE_SIZE_GREATER_THAN_MAX = 1745,
    /// Can't update table '%s' while '%s' is being created.
    ER_CANT_UPDATE_TABLE_IN_CREATE_TABLE_SELECT = 1746,
    /// PARTITION () clause on non partitioned table
    ER_PARTITION_CLAUSE_ON_NONPARTITIONED = 1747,
    /// Found a row not matching the given partition set
    ER_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET = 1748,
    /// partition '%s' doesn't exist
    ER_NO_SUCH_PARTITION_UNUSED = 1749,
    /// Failure while changing the type of replication repository: %s.
    ER_CHANGE_RPL_INFO_REPOSITORY_FAILURE = 1750,
    /// The creation of some temporary tables could not be rolled back.
    ER_WARNING_NOT_COMPLETE_ROLLBACK_WITH_CREATED_TEMP_TABLE = 1751,
    /// Some temporary tables were dropped, but these operations could not be rolled back.
    ER_WARNING_NOT_COMPLETE_ROLLBACK_WITH_DROPPED_TEMP_TABLE = 1752,
    /// %s is not supported in multi-threaded slave mode. %s
    ER_MTS_FEATURE_IS_NOT_SUPPORTED = 1753,
    /// The number of modified databases exceeds the maximum %d; the database names will not be
    /// included in the replication event metadata.
    ER_MTS_UPDATED_DBS_GREATER_MAX = 1754,
    /// Cannot execute the current event group in the parallel mode. Encountered event %s,
    /// relay-log name %s, position %s which prevents execution of this event group in parallel
    /// mode. Reason: %s.
    ER_MTS_CANT_PARALLEL = 1755,
    /// %s
    ER_MTS_INCONSISTENT_DATA = 1756,
    /// FULLTEXT index is not supported for partitioned tables.
    ER_FULLTEXT_NOT_SUPPORTED_WITH_PARTITIONING = 1757,
    /// Invalid condition number
    ER_DA_INVALID_CONDITION_NUMBER = 1758,
    /// Sending passwords in plain text without SSL/TLS is extremely insecure.
    ER_INSECURE_PLAIN_TEXT = 1759,
    /// Storing MySQL user name or password information in the master info repository is not secure
    /// and is therefore not recommended. Please consider using the USER and PASSWORD connection
    /// options for START SLAVE; see the 'START SLAVE Syntax' in the MySQL Manual for more
    /// information.
    ER_INSECURE_CHANGE_MASTER = 1760,
    /// Foreign key constraint for table '%s', record '%s' would lead to a duplicate entry in table
    /// '%s', key '%s'
    ER_FOREIGN_DUPLICATE_KEY_WITH_CHILD_INFO = 1761,
    /// Foreign key constraint for table '%s', record '%s' would lead to a duplicate entry in a
    /// child table
    ER_FOREIGN_DUPLICATE_KEY_WITHOUT_CHILD_INFO = 1762,
    /// Setting authentication options is not possible when only the Slave SQL Thread is being
    /// started.
    ER_SQLTHREAD_WITH_SECURE_SLAVE = 1763,
    /// The table does not have FULLTEXT index to support this query
    ER_TABLE_HAS_NO_FT = 1764,
    /// The system variable %s cannot be set in stored functions or triggers.
    ER_VARIABLE_NOT_SETTABLE_IN_SF_OR_TRIGGER = 1765,
    /// The system variable %s cannot be set when there is an ongoing transaction.
    ER_VARIABLE_NOT_SETTABLE_IN_TRANSACTION = 1766,
    /// The system variable @@SESSION.GTID_NEXT has the value %s, which is not listed in
    /// @@SESSION.GTID_NEXT_LIST.
    ER_GTID_NEXT_IS_NOT_IN_GTID_NEXT_LIST = 1767,
    /// The system variable @@SESSION.GTID_NEXT cannot change inside a transaction.
    ER_CANT_CHANGE_GTID_NEXT_IN_TRANSACTION_WHEN_GTID_NEXT_LIST_IS_NULL = 1768,
    /// The statement 'SET %s' cannot invoke a stored function.
    ER_SET_STATEMENT_CANNOT_INVOKE_FUNCTION = 1769,
    /// The system variable @@SESSION.GTID_NEXT cannot be 'AUTOMATIC' when @@SESSION.GTID_NEXT_LIST
    /// is non-NULL.
    ER_GTID_NEXT_CANT_BE_AUTOMATIC_IF_GTID_NEXT_LIST_IS_NON_NULL = 1770,
    /// Skipping transaction %s because it has already been executed and logged.
    ER_SKIPPING_LOGGED_TRANSACTION = 1771,
    /// Malformed GTID set specification '%s'.
    ER_MALFORMED_GTID_SET_SPECIFICATION = 1772,
    /// Malformed GTID set encoding.
    ER_MALFORMED_GTID_SET_ENCODING = 1773,
    /// Malformed GTID specification '%s'.
    ER_MALFORMED_GTID_SPECIFICATION = 1774,
    /// Impossible to generate Global Transaction Identifier: the integer component reached the
    /// maximal value. Restart the server with a new server_uuid.
    ER_GNO_EXHAUSTED = 1775,
    /// Parameters MASTER_LOG_FILE, MASTER_LOG_POS, RELAY_LOG_FILE and RELAY_LOG_POS cannot be set
    /// when MASTER_AUTO_POSITION is active.
    ER_BAD_SLAVE_AUTO_POSITION = 1776,
    /// CHANGE MASTER TO MASTER_AUTO_POSITION = 1 can only be executed when @@GLOBAL.GTID_MODE =
    /// ON.
    ER_AUTO_POSITION_REQUIRES_GTID_MODE_ON = 1777,
    /// Cannot execute statements with implicit commit inside a transaction when
    /// @@SESSION.GTID_NEXT != AUTOMATIC.
    ER_CANT_DO_IMPLICIT_COMMIT_IN_TRX_WHEN_GTID_NEXT_IS_SET = 1778,
    /// GTID_MODE = ON or GTID_MODE = UPGRADE_STEP_2 requires DISABLE_GTID_UNSAFE_STATEMENTS = 1.
    ER_GTID_MODE_2_OR_3_REQUIRES_DISABLE_GTID_UNSAFE_STATEMENTS_ON = 1779,
    /// @@GLOBAL.GTID_MODE = ON or UPGRADE_STEP_1 or UPGRADE_STEP_2 requires --log-bin and
    /// --log-slave-updates.
    ER_GTID_MODE_REQUIRES_BINLOG = 1780,
    /// @@SESSION.GTID_NEXT cannot be set to UUID:NUMBER when @@GLOBAL.GTID_MODE = OFF.
    ER_CANT_SET_GTID_NEXT_TO_GTID_WHEN_GTID_MODE_IS_OFF = 1781,
    /// @@SESSION.GTID_NEXT cannot be set to ANONYMOUS when @@GLOBAL.GTID_MODE = ON.
    ER_CANT_SET_GTID_NEXT_TO_ANONYMOUS_WHEN_GTID_MODE_IS_ON = 1782,
    /// @@SESSION.GTID_NEXT_LIST cannot be set to a non-NULL value when @@GLOBAL.GTID_MODE = OFF.
    ER_CANT_SET_GTID_NEXT_LIST_TO_NON_NULL_WHEN_GTID_MODE_IS_OFF = 1783,
    /// Found a Gtid_log_event or Previous_gtids_log_event when @@GLOBAL.GTID_MODE = OFF.
    ER_FOUND_GTID_EVENT_WHEN_GTID_MODE_IS_OFF = 1784,
    /// When @@GLOBAL.ENFORCE_GTID_CONSISTENCY = 1, updates to non-transactional tables can only be
    /// done in either autocommitted statements or single-statement transactions, and never in the
    /// same statement as updates to transactional tables.
    ER_GTID_UNSAFE_NON_TRANSACTIONAL_TABLE = 1785,
    /// CREATE TABLE ... SELECT is forbidden when @@GLOBAL.ENFORCE_GTID_CONSISTENCY = 1.
    ER_GTID_UNSAFE_CREATE_SELECT = 1786,
    /// When @@GLOBAL.ENFORCE_GTID_CONSISTENCY = 1, the statements CREATE TEMPORARY TABLE and DROP
    /// TEMPORARY TABLE can be executed in a non-transactional context only, and require that
    /// AUTOCOMMIT = 1.
    ER_GTID_UNSAFE_CREATE_DROP_TEMPORARY_TABLE_IN_TRANSACTION = 1787,
    /// The value of @@GLOBAL.GTID_MODE can only change one step at a time: OFF <-> UPGRADE_STEP_1
    /// <-> UPGRADE_STEP_2 <-> ON. Also note that this value must be stepped up or down
    /// simultaneously on all servers; see the Manual for instructions.
    ER_GTID_MODE_CAN_ONLY_CHANGE_ONE_STEP_AT_A_TIME = 1788,
    /// The slave is connecting using CHANGE MASTER TO MASTER_AUTO_POSITION = 1, but the master has
    /// purged binary logs containing GTIDs that the slave requires.
    ER_MASTER_HAS_PURGED_REQUIRED_GTIDS = 1789,
    /// @@SESSION.GTID_NEXT cannot be changed by a client that owns a GTID. The client owns %s.
    /// Ownership is released on COMMIT or ROLLBACK.
    ER_CANT_SET_GTID_NEXT_WHEN_OWNING_GTID = 1790,
    /// Unknown EXPLAIN format name: '%s'
    ER_UNKNOWN_EXPLAIN_FORMAT = 1791,
    /// Cannot execute statement in a READ ONLY transaction.
    ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION = 1792,
    /// Comment for table partition '%s' is too long (max = %lu
    ER_TOO_LONG_TABLE_PARTITION_COMMENT = 1793,
    /// Slave is not configured or failed to initialize properly. You must at least set --server-id
    /// to enable either a master or a slave. Additional error messages can be found in the MySQL
    /// error log.
    ER_SLAVE_CONFIGURATION = 1794,
    /// InnoDB presently supports one FULLTEXT index creation at a time
    ER_INNODB_FT_LIMIT = 1795,
    /// Cannot create FULLTEXT index on temporary InnoDB table
    ER_INNODB_NO_FT_TEMP_TABLE = 1796,
    /// Column '%s' is of wrong type for an InnoDB FULLTEXT index
    ER_INNODB_FT_WRONG_DOCID_COLUMN = 1797,
    /// Index '%s' is of wrong type for an InnoDB FULLTEXT index
    ER_INNODB_FT_WRONG_DOCID_INDEX = 1798,
    /// Creating index '%s' required more than 'innodb_online_alter_log_max_size' bytes of
    /// modification log. Please try again.
    ER_INNODB_ONLINE_LOG_TOO_BIG = 1799,
    /// Unknown ALGORITHM '%s'
    ER_UNKNOWN_ALTER_ALGORITHM = 1800,
    /// Unknown LOCK type '%s'
    ER_UNKNOWN_ALTER_LOCK = 1801,
    /// CHANGE MASTER cannot be executed when the slave was stopped with an error or killed in MTS
    /// mode. Consider using RESET SLAVE or START SLAVE UNTIL.
    ER_MTS_CHANGE_MASTER_CANT_RUN_WITH_GAPS = 1802,
    /// Cannot recover after SLAVE errored out in parallel execution mode. Additional error
    /// messages can be found in the MySQL error log.
    ER_MTS_RECOVERY_FAILURE = 1803,
    /// Cannot clean up worker info tables. Additional error messages can be found in the MySQL
    /// error log.
    ER_MTS_RESET_WORKERS = 1804,
    /// Column count of %s.%s is wrong. Expected %d, found %d. The table is probably corrupted
    ER_COL_COUNT_DOESNT_MATCH_CORRUPTED_V2 = 1805,
    /// Slave must silently retry current transaction
    ER_SLAVE_SILENT_RETRY_TRANSACTION = 1806,
    /// There is a foreign key check running on table '%s'. Cannot discard the table.
    ER_DISCARD_FK_CHECKS_RUNNING = 1807,
    /// Schema mismatch (%s
    ER_TABLE_SCHEMA_MISMATCH = 1808,
    /// Table '%s' in system tablespace
    ER_TABLE_IN_SYSTEM_TABLESPACE = 1809,
    /// IO Read error: (%lu, %s) %s
    ER_IO_READ_ERROR = 1810,
    /// IO Write error: (%lu, %s) %s
    ER_IO_WRITE_ERROR = 1811,
    /// Tablespace is missing for table '%s'
    ER_TABLESPACE_MISSING = 1812,
    /// Tablespace for table '%s' exists. Please DISCARD the tablespace before IMPORT.
    ER_TABLESPACE_EXISTS = 1813,
    /// Tablespace has been discarded for table '%s'
    ER_TABLESPACE_DISCARDED = 1814,
    /// Internal error: %s
    ER_INTERNAL_ERROR = 1815,
    /// ALTER TABLE '%s' IMPORT TABLESPACE failed with error %lu : '%s'
    ER_INNODB_IMPORT_ERROR = 1816,
    /// Index corrupt: %s
    ER_INNODB_INDEX_CORRUPT = 1817,
    /// YEAR(%lu) column type is deprecated. Creating YEAR(4) column instead.
    ER_INVALID_YEAR_COLUMN_LENGTH = 1818,
    /// Your password does not satisfy the current policy requirements
    ER_NOT_VALID_PASSWORD = 1819,
    /// You must SET PASSWORD before executing this statement
    ER_MUST_CHANGE_PASSWORD = 1820,
    /// Failed to add the foreign key constaint. Missing index for constraint '%s' in the foreign
    /// table '%s'
    ER_FK_NO_INDEX_CHILD = 1821,
    /// Failed to add the foreign key constaint. Missing index for constraint '%s' in the
    /// referenced table '%s'
    ER_FK_NO_INDEX_PARENT = 1822,
    /// Failed to add the foreign key constraint '%s' to system tables
    ER_FK_FAIL_ADD_SYSTEM = 1823,
    /// Failed to open the referenced table '%s'
    ER_FK_CANNOT_OPEN_PARENT = 1824,
    /// Failed to add the foreign key constraint on table '%s'. Incorrect options in FOREIGN KEY
    /// constraint '%s'
    ER_FK_INCORRECT_OPTION = 1825,
    /// Duplicate foreign key constraint name '%s'
    ER_FK_DUP_NAME = 1826,
    /// The password hash doesn't have the expected format. Check if the correct password algorithm
    /// is being used with the PASSWORD() function.
    ER_PASSWORD_FORMAT = 1827,
    /// Cannot drop column '%s': needed in a foreign key constraint '%s'
    ER_FK_COLUMN_CANNOT_DROP = 1828,
    /// Cannot drop column '%s': needed in a foreign key constraint '%s' of table '%s'
    ER_FK_COLUMN_CANNOT_DROP_CHILD = 1829,
    /// Column '%s' cannot be NOT NULL: needed in a foreign key constraint '%s' SET NULL
    ER_FK_COLUMN_NOT_NULL = 1830,
    /// Duplicate index '%s' defined on the table '%s.%s'. This is deprecated and will be
    /// disallowed in a future release.
    ER_DUP_INDEX = 1831,
    /// Cannot change column '%s': used in a foreign key constraint '%s'
    ER_FK_COLUMN_CANNOT_CHANGE = 1832,
    /// Cannot change column '%s': used in a foreign key constraint '%s' of table '%s'
    ER_FK_COLUMN_CANNOT_CHANGE_CHILD = 1833,
    /// Cannot delete rows from table which is parent in a foreign key constraint '%s' of table
    /// '%s'
    ER_FK_CANNOT_DELETE_PARENT = 1834,
    /// Malformed communication packet.
    ER_MALFORMED_PACKET = 1835,
    /// Running in read-only mode
    ER_READ_ONLY_MODE = 1836,
    /// When @@SESSION.GTID_NEXT is set to a GTID, you must explicitly set it to a different value
    /// after a COMMIT or ROLLBACK. Please check GTID_NEXT variable manual page for detailed
    /// explanation. Current @@SESSION.GTID_NEXT is '%s'.
    ER_GTID_NEXT_TYPE_UNDEFINED_GROUP = 1837,
    /// The system variable %s cannot be set in stored procedures.
    ER_VARIABLE_NOT_SETTABLE_IN_SP = 1838,
    /// @@GLOBAL.GTID_PURGED can only be set when @@GLOBAL.GTID_MODE = ON.
    ER_CANT_SET_GTID_PURGED_WHEN_GTID_MODE_IS_OFF = 1839,
    /// @@GLOBAL.GTID_PURGED can only be set when @@GLOBAL.GTID_EXECUTED is empty.
    ER_CANT_SET_GTID_PURGED_WHEN_GTID_EXECUTED_IS_NOT_EMPTY = 1840,
    /// @@GLOBAL.GTID_PURGED can only be set when there are no ongoing transactions (not even in
    /// other clients).
    ER_CANT_SET_GTID_PURGED_WHEN_OWNED_GTIDS_IS_NOT_EMPTY = 1841,
    /// @@GLOBAL.GTID_PURGED was changed from '%s' to '%s'.
    ER_GTID_PURGED_WAS_CHANGED = 1842,
    /// @@GLOBAL.GTID_EXECUTED was changed from '%s' to '%s'.
    ER_GTID_EXECUTED_WAS_CHANGED = 1843,
    /// Cannot execute statement: impossible to write to binary log since BINLOG_FORMAT =
    /// STATEMENT, and both replicated and non replicated tables are written to.
    ER_BINLOG_STMT_MODE_AND_NO_REPL_TABLES = 1844,
    /// %s is not supported for this operation. Try %s.
    ER_ALTER_OPERATION_NOT_SUPPORTED = 1845,
    /// %s is not supported. Reason: %s. Try %s.
    ER_ALTER_OPERATION_NOT_SUPPORTED_REASON = 1846,
    /// COPY algorithm requires a lock
    ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_COPY = 1847,
    /// Partition specific operations do not yet support LOCK/ALGORITHM
    ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_PARTITION = 1848,
    /// Columns participating in a foreign key are renamed
    ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_FK_RENAME = 1849,
    /// Cannot change column type INPLACE
    ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_COLUMN_TYPE = 1850,
    /// Adding foreign keys needs foreign_key_checks=OFF
    ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_FK_CHECK = 1851,
    /// Creating unique indexes with IGNORE requires COPY algorithm to remove duplicate rows
    ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_IGNORE = 1852,
    /// Dropping a primary key is not allowed without also adding a new primary key
    ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_NOPK = 1853,
    /// Adding an auto-increment column requires a lock
    ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_AUTOINC = 1854,
    /// Cannot replace hidden FTS_DOC_ID with a user-visible one
    ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_HIDDEN_FTS = 1855,
    /// Cannot drop or rename FTS_DOC_ID
    ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_CHANGE_FTS = 1856,
    /// Fulltext index creation requires a lock
    ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_FTS = 1857,
    /// sql_slave_skip_counter can not be set when the server is running with @@GLOBAL.GTID_MODE =
    /// ON. Instead, for each transaction that you want to skip, generate an empty transaction with
    /// the same GTID as the transaction
    ER_SQL_SLAVE_SKIP_COUNTER_NOT_SETTABLE_IN_GTID_MODE = 1858,
    /// Duplicate entry for key '%s'
    ER_DUP_UNKNOWN_IN_INDEX = 1859,
    /// Long database name and identifier for object resulted in path length exceeding %d
    /// characters. Path: '%s'.
    ER_IDENT_CAUSES_TOO_LONG_PATH = 1860,
    /// cannot silently convert NULL values, as required in this SQL_MODE
    ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_NOT_NULL = 1861,
    /// Your password has expired. To log in you must change it using a client that supports
    /// expired passwords.
    ER_MUST_CHANGE_PASSWORD_LOGIN = 1862,
    /// Found a row in wrong partition %s
    ER_ROW_IN_WRONG_PARTITION = 1863,
    /// Cannot schedule event %s, relay-log name %s, position %s to Worker thread because its size
    /// %lu exceeds %lu of slave_pending_jobs_size_max.
    ER_MTS_EVENT_BIGGER_PENDING_JOBS_SIZE_MAX = 1864,
    /// Cannot CREATE FULLTEXT INDEX WITH PARSER on InnoDB table
    ER_INNODB_NO_FT_USES_PARSER = 1865,
    /// The binary log file '%s' is logically corrupted: %s
    ER_BINLOG_LOGICAL_CORRUPTION = 1866,
    /// file %s was not purged because it was being read by %d thread(s), purged only %d out of %d
    /// files.
    ER_WARN_PURGE_LOG_IN_USE = 1867,
    /// file %s was not purged because it is the active log file.
    ER_WARN_PURGE_LOG_IS_ACTIVE = 1868,
    /// Auto-increment value in UPDATE conflicts with internally generated values
    ER_AUTO_INCREMENT_CONFLICT = 1869,
    /// Row events are not logged for %s statements that modify BLACKHOLE tables in row format.
    /// Table(s): '%s'
    WARN_ON_BLOCKHOLE_IN_RBR = 1870,
    /// Slave failed to initialize master info structure from the repository
    ER_SLAVE_MI_INIT_REPOSITORY = 1871,
    /// Slave failed to initialize relay log info structure from the repository
    ER_SLAVE_RLI_INIT_REPOSITORY = 1872,
    /// Access denied trying to change to user '%s'@'%s' (using password: %s). Disconnecting.
    ER_ACCESS_DENIED_CHANGE_USER_ERROR = 1873,
    /// InnoDB is in read only mode.
    ER_INNODB_READ_ONLY = 1874,
    /// STOP SLAVE command execution is incomplete: Slave SQL thread got the stop signal, thread is
    /// busy, SQL thread will stop once the current task is complete.
    ER_STOP_SLAVE_SQL_THREAD_TIMEOUT = 1875,
    /// STOP SLAVE command execution is incomplete: Slave IO thread got the stop signal, thread is
    /// busy, IO thread will stop once the current task is complete.
    ER_STOP_SLAVE_IO_THREAD_TIMEOUT = 1876,
    /// Operation cannot be performed. The table '%s.%s' is missing, corrupt or contains bad data.
    ER_TABLE_CORRUPT = 1877,
    /// Temporary file write failure.
    ER_TEMP_FILE_WRITE_FAILURE = 1878,
    /// Upgrade index name failed, please use create index(alter table) algorithm copy to rebuild
    /// index.
    ER_INNODB_FT_AUX_NOT_HEX_ID = 1879,
    /// TIME/TIMESTAMP/DATETIME columns of old format have been upgraded to the new format.
    ER_OLD_TEMPORALS_UPGRADED = 1880,
    /// Operation not allowed when innodb_forced_recovery > 0.
    ER_INNODB_FORCED_RECOVERY = 1881,
    /// The initialization vector supplied to %s is too short. Must be at least %d bytes long
    ER_AES_INVALID_IV = 1882,
    /// Plugin '%s' cannot be uninstalled now. %s
    ER_PLUGIN_CANNOT_BE_UNINSTALLED = 1883,
    /// Cannot execute statement because it needs to be written to the binary log as multiple
    /// statements, and this is not allowed when @@SESSION.GTID_NEXT == 'UUID:NUMBER'.
    ER_GTID_UNSAFE_BINLOG_SPLITTABLE_STATEMENT_AND_GTID_GROUP = 1884,
    /// Slave has more GTIDs than the master has, using the master's SERVER_UUID. This may indicate
    /// that the end of the binary log was truncated or that the last binary log file was lost,
    /// e.g., after a power or disk failure when sync_binlog != 1. The master may or may not have
    /// rolled back transactions that were already replicated to the slave. Suggest to replicate
    /// any transactions that master has rolled back from slave to master, and/or commit empty
    /// transactions on master to account for transactions that have been committed on master but
    /// are not included in GTID_EXECUTED.
    ER_SLAVE_HAS_MORE_GTIDS_THAN_MASTER = 1885,
}

impl From<u16> for ErrorKind {
    fn from(x: u16) -> Self {
        match x {
            1000_u16 => ErrorKind::ER_HASHCHK,
            1001_u16 => ErrorKind::ER_NISAMCHK,
            1002_u16 => ErrorKind::ER_NO,
            1003_u16 => ErrorKind::ER_YES,
            1004_u16 => ErrorKind::ER_CANT_CREATE_FILE,
            1005_u16 => ErrorKind::ER_CANT_CREATE_TABLE,
            1006_u16 => ErrorKind::ER_CANT_CREATE_DB,
            1007_u16 => ErrorKind::ER_DB_CREATE_EXISTS,
            1008_u16 => ErrorKind::ER_DB_DROP_EXISTS,
            1009_u16 => ErrorKind::ER_DB_DROP_DELETE,
            1010_u16 => ErrorKind::ER_DB_DROP_RMDIR,
            1011_u16 => ErrorKind::ER_CANT_DELETE_FILE,
            1012_u16 => ErrorKind::ER_CANT_FIND_SYSTEM_REC,
            1013_u16 => ErrorKind::ER_CANT_GET_STAT,
            1014_u16 => ErrorKind::ER_CANT_GET_WD,
            1015_u16 => ErrorKind::ER_CANT_LOCK,
            1016_u16 => ErrorKind::ER_CANT_OPEN_FILE,
            1017_u16 => ErrorKind::ER_FILE_NOT_FOUND,
            1018_u16 => ErrorKind::ER_CANT_READ_DIR,
            1019_u16 => ErrorKind::ER_CANT_SET_WD,
            1020_u16 => ErrorKind::ER_CHECKREAD,
            1021_u16 => ErrorKind::ER_DISK_FULL,
            1022_u16 => ErrorKind::ER_DUP_KEY,
            1023_u16 => ErrorKind::ER_ERROR_ON_CLOSE,
            1024_u16 => ErrorKind::ER_ERROR_ON_READ,
            1025_u16 => ErrorKind::ER_ERROR_ON_RENAME,
            1026_u16 => ErrorKind::ER_ERROR_ON_WRITE,
            1027_u16 => ErrorKind::ER_FILE_USED,
            1028_u16 => ErrorKind::ER_FILSORT_ABORT,
            1029_u16 => ErrorKind::ER_FORM_NOT_FOUND,
            1030_u16 => ErrorKind::ER_GET_ERRN,
            1031_u16 => ErrorKind::ER_ILLEGAL_HA,
            1032_u16 => ErrorKind::ER_KEY_NOT_FOUND,
            1033_u16 => ErrorKind::ER_NOT_FORM_FILE,
            1034_u16 => ErrorKind::ER_NOT_KEYFILE,
            1035_u16 => ErrorKind::ER_OLD_KEYFILE,
            1036_u16 => ErrorKind::ER_OPEN_AS_READONLY,
            1037_u16 => ErrorKind::ER_OUTOFMEMORY,
            1038_u16 => ErrorKind::ER_OUT_OF_SORTMEMORY,
            1039_u16 => ErrorKind::ER_UNEXPECTED_EOF,
            1040_u16 => ErrorKind::ER_CON_COUNT_ERROR,
            1041_u16 => ErrorKind::ER_OUT_OF_RESOURCES,
            1042_u16 => ErrorKind::ER_BAD_HOST_ERROR,
            1043_u16 => ErrorKind::ER_HANDSHAKE_ERROR,
            1044_u16 => ErrorKind::ER_DBACCESS_DENIED_ERROR,
            1045_u16 => ErrorKind::ER_ACCESS_DENIED_ERROR,
            1046_u16 => ErrorKind::ER_NO_DB_ERROR,
            1047_u16 => ErrorKind::ER_UNKNOWN_COM_ERROR,
            1048_u16 => ErrorKind::ER_BAD_NULL_ERROR,
            1049_u16 => ErrorKind::ER_BAD_DB_ERROR,
            1050_u16 => ErrorKind::ER_TABLE_EXISTS_ERROR,
            1051_u16 => ErrorKind::ER_BAD_TABLE_ERROR,
            1052_u16 => ErrorKind::ER_NON_UNIQ_ERROR,
            1053_u16 => ErrorKind::ER_SERVER_SHUTDOWN,
            1054_u16 => ErrorKind::ER_BAD_FIELD_ERROR,
            1055_u16 => ErrorKind::ER_WRONG_FIELD_WITH_GROUP,
            1056_u16 => ErrorKind::ER_WRONG_GROUP_FIELD,
            1057_u16 => ErrorKind::ER_WRONG_SUM_SELECT,
            1058_u16 => ErrorKind::ER_WRONG_VALUE_COUNT,
            1059_u16 => ErrorKind::ER_TOO_LONG_IDENT,
            1060_u16 => ErrorKind::ER_DUP_FIELDNAME,
            1061_u16 => ErrorKind::ER_DUP_KEYNAME,
            1062_u16 => ErrorKind::ER_DUP_ENTRY,
            1063_u16 => ErrorKind::ER_WRONG_FIELD_SPEC,
            1064_u16 => ErrorKind::ER_PARSE_ERROR,
            1065_u16 => ErrorKind::ER_EMPTY_QUERY,
            1066_u16 => ErrorKind::ER_NONUNIQ_TABLE,
            1067_u16 => ErrorKind::ER_INVALID_DEFAULT,
            1068_u16 => ErrorKind::ER_MULTIPLE_PRI_KEY,
            1069_u16 => ErrorKind::ER_TOO_MANY_KEYS,
            1070_u16 => ErrorKind::ER_TOO_MANY_KEY_PARTS,
            1071_u16 => ErrorKind::ER_TOO_LONG_KEY,
            1072_u16 => ErrorKind::ER_KEY_COLUMN_DOES_NOT_EXITS,
            1073_u16 => ErrorKind::ER_BLOB_USED_AS_KEY,
            1074_u16 => ErrorKind::ER_TOO_BIG_FIELDLENGTH,
            1075_u16 => ErrorKind::ER_WRONG_AUTO_KEY,
            1076_u16 => ErrorKind::ER_READY,
            1077_u16 => ErrorKind::ER_NORMAL_SHUTDOWN,
            1078_u16 => ErrorKind::ER_GOT_SIGNAL,
            1079_u16 => ErrorKind::ER_SHUTDOWN_COMPLETE,
            1080_u16 => ErrorKind::ER_FORCING_CLOSE,
            1081_u16 => ErrorKind::ER_IPSOCK_ERROR,
            1082_u16 => ErrorKind::ER_NO_SUCH_INDEX,
            1083_u16 => ErrorKind::ER_WRONG_FIELD_TERMINATORS,
            1084_u16 => ErrorKind::ER_BLOBS_AND_NO_TERMINATED,
            1085_u16 => ErrorKind::ER_TEXTFILE_NOT_READABLE,
            1086_u16 => ErrorKind::ER_FILE_EXISTS_ERROR,
            1087_u16 => ErrorKind::ER_LOAD_INF,
            1088_u16 => ErrorKind::ER_ALTER_INF,
            1089_u16 => ErrorKind::ER_WRONG_SUB_KEY,
            1090_u16 => ErrorKind::ER_CANT_REMOVE_ALL_FIELDS,
            1091_u16 => ErrorKind::ER_CANT_DROP_FIELD_OR_KEY,
            1092_u16 => ErrorKind::ER_INSERT_INF,
            1093_u16 => ErrorKind::ER_UPDATE_TABLE_USED,
            1094_u16 => ErrorKind::ER_NO_SUCH_THREAD,
            1095_u16 => ErrorKind::ER_KILL_DENIED_ERROR,
            1096_u16 => ErrorKind::ER_NO_TABLES_USED,
            1097_u16 => ErrorKind::ER_TOO_BIG_SET,
            1098_u16 => ErrorKind::ER_NO_UNIQUE_LOGFILE,
            1099_u16 => ErrorKind::ER_TABLE_NOT_LOCKED_FOR_WRITE,
            1100_u16 => ErrorKind::ER_TABLE_NOT_LOCKED,
            1101_u16 => ErrorKind::ER_BLOB_CANT_HAVE_DEFAULT,
            1102_u16 => ErrorKind::ER_WRONG_DB_NAME,
            1103_u16 => ErrorKind::ER_WRONG_TABLE_NAME,
            1104_u16 => ErrorKind::ER_TOO_BIG_SELECT,
            1105_u16 => ErrorKind::ER_UNKNOWN_ERROR,
            1106_u16 => ErrorKind::ER_UNKNOWN_PROCEDURE,
            1107_u16 => ErrorKind::ER_WRONG_PARAMCOUNT_TO_PROCEDURE,
            1108_u16 => ErrorKind::ER_WRONG_PARAMETERS_TO_PROCEDURE,
            1109_u16 => ErrorKind::ER_UNKNOWN_TABLE,
            1110_u16 => ErrorKind::ER_FIELD_SPECIFIED_TWICE,
            1111_u16 => ErrorKind::ER_INVALID_GROUP_FUNC_USE,
            1112_u16 => ErrorKind::ER_UNSUPPORTED_EXTENSION,
            1113_u16 => ErrorKind::ER_TABLE_MUST_HAVE_COLUMNS,
            1114_u16 => ErrorKind::ER_RECORD_FILE_FULL,
            1115_u16 => ErrorKind::ER_UNKNOWN_CHARACTER_SET,
            1116_u16 => ErrorKind::ER_TOO_MANY_TABLES,
            1117_u16 => ErrorKind::ER_TOO_MANY_FIELDS,
            1118_u16 => ErrorKind::ER_TOO_BIG_ROWSIZE,
            1119_u16 => ErrorKind::ER_STACK_OVERRUN,
            1120_u16 => ErrorKind::ER_WRONG_OUTER_JOIN,
            1121_u16 => ErrorKind::ER_NULL_COLUMN_IN_INDEX,
            1122_u16 => ErrorKind::ER_CANT_FIND_UDF,
            1123_u16 => ErrorKind::ER_CANT_INITIALIZE_UDF,
            1124_u16 => ErrorKind::ER_UDF_NO_PATHS,
            1125_u16 => ErrorKind::ER_UDF_EXISTS,
            1126_u16 => ErrorKind::ER_CANT_OPEN_LIBRARY,
            1127_u16 => ErrorKind::ER_CANT_FIND_DL_ENTRY,
            1128_u16 => ErrorKind::ER_FUNCTION_NOT_DEFINED,
            1129_u16 => ErrorKind::ER_HOST_IS_BLOCKED,
            1130_u16 => ErrorKind::ER_HOST_NOT_PRIVILEGED,
            1131_u16 => ErrorKind::ER_PASSWORD_ANONYMOUS_USER,
            1132_u16 => ErrorKind::ER_PASSWORD_NOT_ALLOWED,
            1133_u16 => ErrorKind::ER_PASSWORD_NO_MATCH,
            1134_u16 => ErrorKind::ER_UPDATE_INF,
            1135_u16 => ErrorKind::ER_CANT_CREATE_THREAD,
            1136_u16 => ErrorKind::ER_WRONG_VALUE_COUNT_ON_ROW,
            1137_u16 => ErrorKind::ER_CANT_REOPEN_TABLE,
            1138_u16 => ErrorKind::ER_INVALID_USE_OF_NULL,
            1139_u16 => ErrorKind::ER_REGEXP_ERROR,
            1140_u16 => ErrorKind::ER_MIX_OF_GROUP_FUNC_AND_FIELDS,
            1141_u16 => ErrorKind::ER_NONEXISTING_GRANT,
            1142_u16 => ErrorKind::ER_TABLEACCESS_DENIED_ERROR,
            1143_u16 => ErrorKind::ER_COLUMNACCESS_DENIED_ERROR,
            1144_u16 => ErrorKind::ER_ILLEGAL_GRANT_FOR_TABLE,
            1145_u16 => ErrorKind::ER_GRANT_WRONG_HOST_OR_USER,
            1146_u16 => ErrorKind::ER_NO_SUCH_TABLE,
            1147_u16 => ErrorKind::ER_NONEXISTING_TABLE_GRANT,
            1148_u16 => ErrorKind::ER_NOT_ALLOWED_COMMAND,
            1149_u16 => ErrorKind::ER_SYNTAX_ERROR,
            1150_u16 => ErrorKind::ER_DELAYED_CANT_CHANGE_LOCK,
            1151_u16 => ErrorKind::ER_TOO_MANY_DELAYED_THREADS,
            1152_u16 => ErrorKind::ER_ABORTING_CONNECTION,
            1153_u16 => ErrorKind::ER_NET_PACKET_TOO_LARGE,
            1154_u16 => ErrorKind::ER_NET_READ_ERROR_FROM_PIPE,
            1155_u16 => ErrorKind::ER_NET_FCNTL_ERROR,
            1156_u16 => ErrorKind::ER_NET_PACKETS_OUT_OF_ORDER,
            1157_u16 => ErrorKind::ER_NET_UNCOMPRESS_ERROR,
            1158_u16 => ErrorKind::ER_NET_READ_ERROR,
            1159_u16 => ErrorKind::ER_NET_READ_INTERRUPTED,
            1160_u16 => ErrorKind::ER_NET_ERROR_ON_WRITE,
            1161_u16 => ErrorKind::ER_NET_WRITE_INTERRUPTED,
            1162_u16 => ErrorKind::ER_TOO_LONG_STRING,
            1163_u16 => ErrorKind::ER_TABLE_CANT_HANDLE_BLOB,
            1164_u16 => ErrorKind::ER_TABLE_CANT_HANDLE_AUTO_INCREMENT,
            1165_u16 => ErrorKind::ER_DELAYED_INSERT_TABLE_LOCKED,
            1166_u16 => ErrorKind::ER_WRONG_COLUMN_NAME,
            1167_u16 => ErrorKind::ER_WRONG_KEY_COLUMN,
            1168_u16 => ErrorKind::ER_WRONG_MRG_TABLE,
            1169_u16 => ErrorKind::ER_DUP_UNIQUE,
            1170_u16 => ErrorKind::ER_BLOB_KEY_WITHOUT_LENGTH,
            1171_u16 => ErrorKind::ER_PRIMARY_CANT_HAVE_NULL,
            1172_u16 => ErrorKind::ER_TOO_MANY_ROWS,
            1173_u16 => ErrorKind::ER_REQUIRES_PRIMARY_KEY,
            1174_u16 => ErrorKind::ER_NO_RAID_COMPILED,
            1175_u16 => ErrorKind::ER_UPDATE_WITHOUT_KEY_IN_SAFE_MODE,
            1176_u16 => ErrorKind::ER_KEY_DOES_NOT_EXITS,
            1177_u16 => ErrorKind::ER_CHECK_NO_SUCH_TABLE,
            1178_u16 => ErrorKind::ER_CHECK_NOT_IMPLEMENTED,
            1179_u16 => ErrorKind::ER_CANT_DO_THIS_DURING_AN_TRANSACTION,
            1180_u16 => ErrorKind::ER_ERROR_DURING_COMMIT,
            1181_u16 => ErrorKind::ER_ERROR_DURING_ROLLBACK,
            1182_u16 => ErrorKind::ER_ERROR_DURING_FLUSH_LOGS,
            1183_u16 => ErrorKind::ER_ERROR_DURING_CHECKPOINT,
            1184_u16 => ErrorKind::ER_NEW_ABORTING_CONNECTION,
            1185_u16 => ErrorKind::ER_DUMP_NOT_IMPLEMENTED,
            1186_u16 => ErrorKind::ER_FLUSH_MASTER_BINLOG_CLOSED,
            1187_u16 => ErrorKind::ER_INDEX_REBUILD,
            1188_u16 => ErrorKind::ER_MASTER,
            1189_u16 => ErrorKind::ER_MASTER_NET_READ,
            1190_u16 => ErrorKind::ER_MASTER_NET_WRITE,
            1191_u16 => ErrorKind::ER_FT_MATCHING_KEY_NOT_FOUND,
            1192_u16 => ErrorKind::ER_LOCK_OR_ACTIVE_TRANSACTION,
            1193_u16 => ErrorKind::ER_UNKNOWN_SYSTEM_VARIABLE,
            1194_u16 => ErrorKind::ER_CRASHED_ON_USAGE,
            1195_u16 => ErrorKind::ER_CRASHED_ON_REPAIR,
            1196_u16 => ErrorKind::ER_WARNING_NOT_COMPLETE_ROLLBACK,
            1197_u16 => ErrorKind::ER_TRANS_CACHE_FULL,
            1198_u16 => ErrorKind::ER_SLAVE_MUST_STOP,
            1199_u16 => ErrorKind::ER_SLAVE_NOT_RUNNING,
            1200_u16 => ErrorKind::ER_BAD_SLAVE,
            1201_u16 => ErrorKind::ER_MASTER_INF,
            1202_u16 => ErrorKind::ER_SLAVE_THREAD,
            1203_u16 => ErrorKind::ER_TOO_MANY_USER_CONNECTIONS,
            1204_u16 => ErrorKind::ER_SET_CONSTANTS_ONLY,
            1205_u16 => ErrorKind::ER_LOCK_WAIT_TIMEOUT,
            1206_u16 => ErrorKind::ER_LOCK_TABLE_FULL,
            1207_u16 => ErrorKind::ER_READ_ONLY_TRANSACTION,
            1208_u16 => ErrorKind::ER_DROP_DB_WITH_READ_LOCK,
            1209_u16 => ErrorKind::ER_CREATE_DB_WITH_READ_LOCK,
            1210_u16 => ErrorKind::ER_WRONG_ARGUMENTS,
            1211_u16 => ErrorKind::ER_NO_PERMISSION_TO_CREATE_USER,
            1212_u16 => ErrorKind::ER_UNION_TABLES_IN_DIFFERENT_DIR,
            1213_u16 => ErrorKind::ER_LOCK_DEADLOCK,
            1214_u16 => ErrorKind::ER_TABLE_CANT_HANDLE_FT,
            1215_u16 => ErrorKind::ER_CANNOT_ADD_FOREIGN,
            1216_u16 => ErrorKind::ER_NO_REFERENCED_ROW,
            1217_u16 => ErrorKind::ER_ROW_IS_REFERENCED,
            1218_u16 => ErrorKind::ER_CONNECT_TO_MASTER,
            1219_u16 => ErrorKind::ER_QUERY_ON_MASTER,
            1220_u16 => ErrorKind::ER_ERROR_WHEN_EXECUTING_COMMAND,
            1221_u16 => ErrorKind::ER_WRONG_USAGE,
            1222_u16 => ErrorKind::ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT,
            1223_u16 => ErrorKind::ER_CANT_UPDATE_WITH_READLOCK,
            1224_u16 => ErrorKind::ER_MIXING_NOT_ALLOWED,
            1225_u16 => ErrorKind::ER_DUP_ARGUMENT,
            1226_u16 => ErrorKind::ER_USER_LIMIT_REACHED,
            1227_u16 => ErrorKind::ER_SPECIFIC_ACCESS_DENIED_ERROR,
            1228_u16 => ErrorKind::ER_LOCAL_VARIABLE,
            1229_u16 => ErrorKind::ER_GLOBAL_VARIABLE,
            1230_u16 => ErrorKind::ER_NO_DEFAULT,
            1231_u16 => ErrorKind::ER_WRONG_VALUE_FOR_VAR,
            1232_u16 => ErrorKind::ER_WRONG_TYPE_FOR_VAR,
            1233_u16 => ErrorKind::ER_VAR_CANT_BE_READ,
            1234_u16 => ErrorKind::ER_CANT_USE_OPTION_HERE,
            1235_u16 => ErrorKind::ER_NOT_SUPPORTED_YET,
            1236_u16 => ErrorKind::ER_MASTER_FATAL_ERROR_READING_BINLOG,
            1237_u16 => ErrorKind::ER_SLAVE_IGNORED_TABLE,
            1238_u16 => ErrorKind::ER_INCORRECT_GLOBAL_LOCAL_VAR,
            1239_u16 => ErrorKind::ER_WRONG_FK_DEF,
            1240_u16 => ErrorKind::ER_KEY_REF_DO_NOT_MATCH_TABLE_REF,
            1241_u16 => ErrorKind::ER_OPERAND_COLUMNS,
            1242_u16 => ErrorKind::ER_SUBQUERY_NO_1_ROW,
            1243_u16 => ErrorKind::ER_UNKNOWN_STMT_HANDLER,
            1244_u16 => ErrorKind::ER_CORRUPT_HELP_DB,
            1245_u16 => ErrorKind::ER_CYCLIC_REFERENCE,
            1246_u16 => ErrorKind::ER_AUTO_CONVERT,
            1247_u16 => ErrorKind::ER_ILLEGAL_REFERENCE,
            1248_u16 => ErrorKind::ER_DERIVED_MUST_HAVE_ALIAS,
            1249_u16 => ErrorKind::ER_SELECT_REDUCED,
            1250_u16 => ErrorKind::ER_TABLENAME_NOT_ALLOWED_HERE,
            1251_u16 => ErrorKind::ER_NOT_SUPPORTED_AUTH_MODE,
            1252_u16 => ErrorKind::ER_SPATIAL_CANT_HAVE_NULL,
            1253_u16 => ErrorKind::ER_COLLATION_CHARSET_MISMATCH,
            1254_u16 => ErrorKind::ER_SLAVE_WAS_RUNNING,
            1255_u16 => ErrorKind::ER_SLAVE_WAS_NOT_RUNNING,
            1256_u16 => ErrorKind::ER_TOO_BIG_FOR_UNCOMPRESS,
            1257_u16 => ErrorKind::ER_ZLIB_Z_MEM_ERROR,
            1258_u16 => ErrorKind::ER_ZLIB_Z_BUF_ERROR,
            1259_u16 => ErrorKind::ER_ZLIB_Z_DATA_ERROR,
            1260_u16 => ErrorKind::ER_CUT_VALUE_GROUP_CONCAT,
            1261_u16 => ErrorKind::ER_WARN_TOO_FEW_RECORDS,
            1262_u16 => ErrorKind::ER_WARN_TOO_MANY_RECORDS,
            1263_u16 => ErrorKind::ER_WARN_NULL_TO_NOTNULL,
            1264_u16 => ErrorKind::ER_WARN_DATA_OUT_OF_RANGE,
            1265_u16 => ErrorKind::WARN_DATA_TRUNCATED,
            1266_u16 => ErrorKind::ER_WARN_USING_OTHER_HANDLER,
            1267_u16 => ErrorKind::ER_CANT_AGGREGATE_2COLLATIONS,
            1268_u16 => ErrorKind::ER_DROP_USER,
            1269_u16 => ErrorKind::ER_REVOKE_GRANTS,
            1270_u16 => ErrorKind::ER_CANT_AGGREGATE_3COLLATIONS,
            1271_u16 => ErrorKind::ER_CANT_AGGREGATE_NCOLLATIONS,
            1272_u16 => ErrorKind::ER_VARIABLE_IS_NOT_STRUCT,
            1273_u16 => ErrorKind::ER_UNKNOWN_COLLATION,
            1274_u16 => ErrorKind::ER_SLAVE_IGNORED_SSL_PARAMS,
            1275_u16 => ErrorKind::ER_SERVER_IS_IN_SECURE_AUTH_MODE,
            1276_u16 => ErrorKind::ER_WARN_FIELD_RESOLVED,
            1277_u16 => ErrorKind::ER_BAD_SLAVE_UNTIL_COND,
            1278_u16 => ErrorKind::ER_MISSING_SKIP_SLAVE,
            1279_u16 => ErrorKind::ER_UNTIL_COND_IGNORED,
            1280_u16 => ErrorKind::ER_WRONG_NAME_FOR_INDEX,
            1281_u16 => ErrorKind::ER_WRONG_NAME_FOR_CATALOG,
            1282_u16 => ErrorKind::ER_WARN_QC_RESIZE,
            1283_u16 => ErrorKind::ER_BAD_FT_COLUMN,
            1284_u16 => ErrorKind::ER_UNKNOWN_KEY_CACHE,
            1285_u16 => ErrorKind::ER_WARN_HOSTNAME_WONT_WORK,
            1286_u16 => ErrorKind::ER_UNKNOWN_STORAGE_ENGINE,
            1287_u16 => ErrorKind::ER_WARN_DEPRECATED_SYNTAX,
            1288_u16 => ErrorKind::ER_NON_UPDATABLE_TABLE,
            1289_u16 => ErrorKind::ER_FEATURE_DISABLED,
            1290_u16 => ErrorKind::ER_OPTION_PREVENTS_STATEMENT,
            1291_u16 => ErrorKind::ER_DUPLICATED_VALUE_IN_TYPE,
            1292_u16 => ErrorKind::ER_TRUNCATED_WRONG_VALUE,
            1293_u16 => ErrorKind::ER_TOO_MUCH_AUTO_TIMESTAMP_COLS,
            1294_u16 => ErrorKind::ER_INVALID_ON_UPDATE,
            1295_u16 => ErrorKind::ER_UNSUPPORTED_PS,
            1296_u16 => ErrorKind::ER_GET_ERRMSG,
            1297_u16 => ErrorKind::ER_GET_TEMPORARY_ERRMSG,
            1298_u16 => ErrorKind::ER_UNKNOWN_TIME_ZONE,
            1299_u16 => ErrorKind::ER_WARN_INVALID_TIMESTAMP,
            1300_u16 => ErrorKind::ER_INVALID_CHARACTER_STRING,
            1301_u16 => ErrorKind::ER_WARN_ALLOWED_PACKET_OVERFLOWED,
            1302_u16 => ErrorKind::ER_CONFLICTING_DECLARATIONS,
            1303_u16 => ErrorKind::ER_SP_NO_RECURSIVE_CREATE,
            1304_u16 => ErrorKind::ER_SP_ALREADY_EXISTS,
            1305_u16 => ErrorKind::ER_SP_DOES_NOT_EXIST,
            1306_u16 => ErrorKind::ER_SP_DROP_FAILED,
            1307_u16 => ErrorKind::ER_SP_STORE_FAILED,
            1308_u16 => ErrorKind::ER_SP_LILABEL_MISMATCH,
            1309_u16 => ErrorKind::ER_SP_LABEL_REDEFINE,
            1310_u16 => ErrorKind::ER_SP_LABEL_MISMATCH,
            1311_u16 => ErrorKind::ER_SP_UNINIT_VAR,
            1312_u16 => ErrorKind::ER_SP_BADSELECT,
            1313_u16 => ErrorKind::ER_SP_BADRETURN,
            1314_u16 => ErrorKind::ER_SP_BADSTATEMENT,
            1315_u16 => ErrorKind::ER_UPDATE_LOG_DEPRECATED_IGNORED,
            1316_u16 => ErrorKind::ER_UPDATE_LOG_DEPRECATED_TRANSLATED,
            1317_u16 => ErrorKind::ER_QUERY_INTERRUPTED,
            1318_u16 => ErrorKind::ER_SP_WRONG_NO_OF_ARGS,
            1319_u16 => ErrorKind::ER_SP_COND_MISMATCH,
            1320_u16 => ErrorKind::ER_SP_NORETURN,
            1321_u16 => ErrorKind::ER_SP_NORETURNEND,
            1322_u16 => ErrorKind::ER_SP_BAD_CURSOR_QUERY,
            1323_u16 => ErrorKind::ER_SP_BAD_CURSOR_SELECT,
            1324_u16 => ErrorKind::ER_SP_CURSOR_MISMATCH,
            1325_u16 => ErrorKind::ER_SP_CURSOR_ALREADY_OPEN,
            1326_u16 => ErrorKind::ER_SP_CURSOR_NOT_OPEN,
            1327_u16 => ErrorKind::ER_SP_UNDECLARED_VAR,
            1328_u16 => ErrorKind::ER_SP_WRONG_NO_OF_FETCH_ARGS,
            1329_u16 => ErrorKind::ER_SP_FETCH_NO_DATA,
            1330_u16 => ErrorKind::ER_SP_DUP_PARAM,
            1331_u16 => ErrorKind::ER_SP_DUP_VAR,
            1332_u16 => ErrorKind::ER_SP_DUP_COND,
            1333_u16 => ErrorKind::ER_SP_DUP_CURS,
            1334_u16 => ErrorKind::ER_SP_CANT_ALTER,
            1335_u16 => ErrorKind::ER_SP_SUBSELECT_NYI,
            1336_u16 => ErrorKind::ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG,
            1337_u16 => ErrorKind::ER_SP_VARCOND_AFTER_CURSHNDLR,
            1338_u16 => ErrorKind::ER_SP_CURSOR_AFTER_HANDLER,
            1339_u16 => ErrorKind::ER_SP_CASE_NOT_FOUND,
            1340_u16 => ErrorKind::ER_FPARSER_TOO_BIG_FILE,
            1341_u16 => ErrorKind::ER_FPARSER_BAD_HEADER,
            1342_u16 => ErrorKind::ER_FPARSER_EOF_IN_COMMENT,
            1343_u16 => ErrorKind::ER_FPARSER_ERROR_IN_PARAMETER,
            1344_u16 => ErrorKind::ER_FPARSER_EOF_IN_UNKNOWN_PARAMETER,
            1345_u16 => ErrorKind::ER_VIEW_NO_EXPLAIN,
            1346_u16 => ErrorKind::ER_FRM_UNKNOWN_TYPE,
            1347_u16 => ErrorKind::ER_WRONG_OBJECT,
            1348_u16 => ErrorKind::ER_NONUPDATEABLE_COLUMN,
            1349_u16 => ErrorKind::ER_VIEW_SELECT_DERIVED,
            1350_u16 => ErrorKind::ER_VIEW_SELECT_CLAUSE,
            1351_u16 => ErrorKind::ER_VIEW_SELECT_VARIABLE,
            1352_u16 => ErrorKind::ER_VIEW_SELECT_TMPTABLE,
            1353_u16 => ErrorKind::ER_VIEW_WRONG_LIST,
            1354_u16 => ErrorKind::ER_WARN_VIEW_MERGE,
            1355_u16 => ErrorKind::ER_WARN_VIEW_WITHOUT_KEY,
            1356_u16 => ErrorKind::ER_VIEW_INVALID,
            1357_u16 => ErrorKind::ER_SP_NO_DROP_SP,
            1358_u16 => ErrorKind::ER_SP_GOTO_IN_HNDLR,
            1359_u16 => ErrorKind::ER_TRG_ALREADY_EXISTS,
            1360_u16 => ErrorKind::ER_TRG_DOES_NOT_EXIST,
            1361_u16 => ErrorKind::ER_TRG_ON_VIEW_OR_TEMP_TABLE,
            1362_u16 => ErrorKind::ER_TRG_CANT_CHANGE_ROW,
            1363_u16 => ErrorKind::ER_TRG_NO_SUCH_ROW_IN_TRG,
            1364_u16 => ErrorKind::ER_NO_DEFAULT_FOR_FIELD,
            1365_u16 => ErrorKind::ER_DIVISION_BY_ZER,
            1366_u16 => ErrorKind::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
            1367_u16 => ErrorKind::ER_ILLEGAL_VALUE_FOR_TYPE,
            1368_u16 => ErrorKind::ER_VIEW_NONUPD_CHECK,
            1369_u16 => ErrorKind::ER_VIEW_CHECK_FAILED,
            1370_u16 => ErrorKind::ER_PROCACCESS_DENIED_ERROR,
            1371_u16 => ErrorKind::ER_RELAY_LOG_FAIL,
            1372_u16 => ErrorKind::ER_PASSWD_LENGTH,
            1373_u16 => ErrorKind::ER_UNKNOWN_TARGET_BINLOG,
            1374_u16 => ErrorKind::ER_IO_ERR_LOG_INDEX_READ,
            1375_u16 => ErrorKind::ER_BINLOG_PURGE_PROHIBITED,
            1376_u16 => ErrorKind::ER_FSEEK_FAIL,
            1377_u16 => ErrorKind::ER_BINLOG_PURGE_FATAL_ERR,
            1378_u16 => ErrorKind::ER_LOG_IN_USE,
            1379_u16 => ErrorKind::ER_LOG_PURGE_UNKNOWN_ERR,
            1380_u16 => ErrorKind::ER_RELAY_LOG_INIT,
            1381_u16 => ErrorKind::ER_NO_BINARY_LOGGING,
            1382_u16 => ErrorKind::ER_RESERVED_SYNTAX,
            1383_u16 => ErrorKind::ER_WSAS_FAILED,
            1384_u16 => ErrorKind::ER_DIFF_GROUPS_PROC,
            1385_u16 => ErrorKind::ER_NO_GROUP_FOR_PROC,
            1386_u16 => ErrorKind::ER_ORDER_WITH_PROC,
            1387_u16 => ErrorKind::ER_LOGGING_PROHIBIT_CHANGING_OF,
            1388_u16 => ErrorKind::ER_NO_FILE_MAPPING,
            1389_u16 => ErrorKind::ER_WRONG_MAGIC,
            1390_u16 => ErrorKind::ER_PS_MANY_PARAM,
            1391_u16 => ErrorKind::ER_KEY_PART_0,
            1392_u16 => ErrorKind::ER_VIEW_CHECKSUM,
            1393_u16 => ErrorKind::ER_VIEW_MULTIUPDATE,
            1394_u16 => ErrorKind::ER_VIEW_NO_INSERT_FIELD_LIST,
            1395_u16 => ErrorKind::ER_VIEW_DELETE_MERGE_VIEW,
            1396_u16 => ErrorKind::ER_CANNOT_USER,
            1397_u16 => ErrorKind::ER_XAER_NOTA,
            1398_u16 => ErrorKind::ER_XAER_INVAL,
            1399_u16 => ErrorKind::ER_XAER_RMFAIL,
            1400_u16 => ErrorKind::ER_XAER_OUTSIDE,
            1401_u16 => ErrorKind::ER_XAER_RMERR,
            1402_u16 => ErrorKind::ER_XA_RBROLLBACK,
            1403_u16 => ErrorKind::ER_NONEXISTING_PROC_GRANT,
            1404_u16 => ErrorKind::ER_PROC_AUTO_GRANT_FAIL,
            1405_u16 => ErrorKind::ER_PROC_AUTO_REVOKE_FAIL,
            1406_u16 => ErrorKind::ER_DATA_TOO_LONG,
            1407_u16 => ErrorKind::ER_SP_BAD_SQLSTATE,
            1408_u16 => ErrorKind::ER_STARTUP,
            1409_u16 => ErrorKind::ER_LOAD_FROM_FIXED_SIZE_ROWS_TO_VAR,
            1410_u16 => ErrorKind::ER_CANT_CREATE_USER_WITH_GRANT,
            1411_u16 => ErrorKind::ER_WRONG_VALUE_FOR_TYPE,
            1412_u16 => ErrorKind::ER_TABLE_DEF_CHANGED,
            1413_u16 => ErrorKind::ER_SP_DUP_HANDLER,
            1414_u16 => ErrorKind::ER_SP_NOT_VAR_ARG,
            1415_u16 => ErrorKind::ER_SP_NO_RETSET,
            1416_u16 => ErrorKind::ER_CANT_CREATE_GEOMETRY_OBJECT,
            1417_u16 => ErrorKind::ER_FAILED_ROUTINE_BREAK_BINLOG,
            1418_u16 => ErrorKind::ER_BINLOG_UNSAFE_ROUTINE,
            1419_u16 => ErrorKind::ER_BINLOG_CREATE_ROUTINE_NEED_SUPER,
            1420_u16 => ErrorKind::ER_EXEC_STMT_WITH_OPEN_CURSOR,
            1421_u16 => ErrorKind::ER_STMT_HAS_NO_OPEN_CURSOR,
            1422_u16 => ErrorKind::ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG,
            1423_u16 => ErrorKind::ER_NO_DEFAULT_FOR_VIEW_FIELD,
            1424_u16 => ErrorKind::ER_SP_NO_RECURSION,
            1425_u16 => ErrorKind::ER_TOO_BIG_SCALE,
            1426_u16 => ErrorKind::ER_TOO_BIG_PRECISION,
            1427_u16 => ErrorKind::ER_M_BIGGER_THAN_D,
            1428_u16 => ErrorKind::ER_WRONG_LOCK_OF_SYSTEM_TABLE,
            1429_u16 => ErrorKind::ER_CONNECT_TO_FOREIGN_DATA_SOURCE,
            1430_u16 => ErrorKind::ER_QUERY_ON_FOREIGN_DATA_SOURCE,
            1431_u16 => ErrorKind::ER_FOREIGN_DATA_SOURCE_DOESNT_EXIST,
            1432_u16 => ErrorKind::ER_FOREIGN_DATA_STRING_INVALID_CANT_CREATE,
            1433_u16 => ErrorKind::ER_FOREIGN_DATA_STRING_INVALID,
            1434_u16 => ErrorKind::ER_CANT_CREATE_FEDERATED_TABLE,
            1435_u16 => ErrorKind::ER_TRG_IN_WRONG_SCHEMA,
            1436_u16 => ErrorKind::ER_STACK_OVERRUN_NEED_MORE,
            1437_u16 => ErrorKind::ER_TOO_LONG_BODY,
            1438_u16 => ErrorKind::ER_WARN_CANT_DROP_DEFAULT_KEYCACHE,
            1439_u16 => ErrorKind::ER_TOO_BIG_DISPLAYWIDTH,
            1440_u16 => ErrorKind::ER_XAER_DUPID,
            1441_u16 => ErrorKind::ER_DATETIME_FUNCTION_OVERFLOW,
            1442_u16 => ErrorKind::ER_CANT_UPDATE_USED_TABLE_IN_SF_OR_TRG,
            1443_u16 => ErrorKind::ER_VIEW_PREVENT_UPDATE,
            1444_u16 => ErrorKind::ER_PS_NO_RECURSION,
            1445_u16 => ErrorKind::ER_SP_CANT_SET_AUTOCOMMIT,
            1446_u16 => ErrorKind::ER_MALFORMED_DEFINER,
            1447_u16 => ErrorKind::ER_VIEW_FRM_NO_USER,
            1448_u16 => ErrorKind::ER_VIEW_OTHER_USER,
            1449_u16 => ErrorKind::ER_NO_SUCH_USER,
            1450_u16 => ErrorKind::ER_FORBID_SCHEMA_CHANGE,
            1451_u16 => ErrorKind::ER_ROW_IS_REFERENCED_2,
            1452_u16 => ErrorKind::ER_NO_REFERENCED_ROW_2,
            1453_u16 => ErrorKind::ER_SP_BAD_VAR_SHADOW,
            1454_u16 => ErrorKind::ER_TRG_NO_DEFINER,
            1455_u16 => ErrorKind::ER_OLD_FILE_FORMAT,
            1456_u16 => ErrorKind::ER_SP_RECURSION_LIMIT,
            1457_u16 => ErrorKind::ER_SP_PROC_TABLE_CORRUPT,
            1458_u16 => ErrorKind::ER_SP_WRONG_NAME,
            1459_u16 => ErrorKind::ER_TABLE_NEEDS_UPGRADE,
            1460_u16 => ErrorKind::ER_SP_NO_AGGREGATE,
            1461_u16 => ErrorKind::ER_MAX_PREPARED_STMT_COUNT_REACHED,
            1462_u16 => ErrorKind::ER_VIEW_RECURSIVE,
            1463_u16 => ErrorKind::ER_NON_GROUPING_FIELD_USED,
            1464_u16 => ErrorKind::ER_TABLE_CANT_HANDLE_SPKEYS,
            1465_u16 => ErrorKind::ER_NO_TRIGGERS_ON_SYSTEM_SCHEMA,
            1466_u16 => ErrorKind::ER_REMOVED_SPACES,
            1467_u16 => ErrorKind::ER_AUTOINC_READ_FAILED,
            1468_u16 => ErrorKind::ER_USERNAME,
            1469_u16 => ErrorKind::ER_HOSTNAME,
            1470_u16 => ErrorKind::ER_WRONG_STRING_LENGTH,
            1471_u16 => ErrorKind::ER_NON_INSERTABLE_TABLE,
            1472_u16 => ErrorKind::ER_ADMIN_WRONG_MRG_TABLE,
            1473_u16 => ErrorKind::ER_TOO_HIGH_LEVEL_OF_NESTING_FOR_SELECT,
            1474_u16 => ErrorKind::ER_NAME_BECOMES_EMPTY,
            1475_u16 => ErrorKind::ER_AMBIGUOUS_FIELD_TERM,
            1476_u16 => ErrorKind::ER_FOREIGN_SERVER_EXISTS,
            1477_u16 => ErrorKind::ER_FOREIGN_SERVER_DOESNT_EXIST,
            1478_u16 => ErrorKind::ER_ILLEGAL_HA_CREATE_OPTION,
            1479_u16 => ErrorKind::ER_PARTITION_REQUIRES_VALUES_ERROR,
            1480_u16 => ErrorKind::ER_PARTITION_WRONG_VALUES_ERROR,
            1481_u16 => ErrorKind::ER_PARTITION_MAXVALUE_ERROR,
            1482_u16 => ErrorKind::ER_PARTITION_SUBPARTITION_ERROR,
            1483_u16 => ErrorKind::ER_PARTITION_SUBPART_MIX_ERROR,
            1484_u16 => ErrorKind::ER_PARTITION_WRONG_NO_PART_ERROR,
            1485_u16 => ErrorKind::ER_PARTITION_WRONG_NO_SUBPART_ERROR,
            1486_u16 => ErrorKind::ER_WRONG_EXPR_IN_PARTITION_FUNC_ERROR,
            1487_u16 => ErrorKind::ER_NO_CONST_EXPR_IN_RANGE_OR_LIST_ERROR,
            1488_u16 => ErrorKind::ER_FIELD_NOT_FOUND_PART_ERROR,
            1489_u16 => ErrorKind::ER_LIST_OF_FIELDS_ONLY_IN_HASH_ERROR,
            1490_u16 => ErrorKind::ER_INCONSISTENT_PARTITION_INFO_ERROR,
            1491_u16 => ErrorKind::ER_PARTITION_FUNC_NOT_ALLOWED_ERROR,
            1492_u16 => ErrorKind::ER_PARTITIONS_MUST_BE_DEFINED_ERROR,
            1493_u16 => ErrorKind::ER_RANGE_NOT_INCREASING_ERROR,
            1494_u16 => ErrorKind::ER_INCONSISTENT_TYPE_OF_FUNCTIONS_ERROR,
            1495_u16 => ErrorKind::ER_MULTIPLE_DEF_CONST_IN_LIST_PART_ERROR,
            1496_u16 => ErrorKind::ER_PARTITION_ENTRY_ERROR,
            1497_u16 => ErrorKind::ER_MIX_HANDLER_ERROR,
            1498_u16 => ErrorKind::ER_PARTITION_NOT_DEFINED_ERROR,
            1499_u16 => ErrorKind::ER_TOO_MANY_PARTITIONS_ERROR,
            1500_u16 => ErrorKind::ER_SUBPARTITION_ERROR,
            1501_u16 => ErrorKind::ER_CANT_CREATE_HANDLER_FILE,
            1502_u16 => ErrorKind::ER_BLOB_FIELD_IN_PART_FUNC_ERROR,
            1503_u16 => ErrorKind::ER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF,
            1504_u16 => ErrorKind::ER_NO_PARTS_ERROR,
            1505_u16 => ErrorKind::ER_PARTITION_MGMT_ON_NONPARTITIONED,
            1506_u16 => ErrorKind::ER_FOREIGN_KEY_ON_PARTITIONED,
            1507_u16 => ErrorKind::ER_DROP_PARTITION_NON_EXISTENT,
            1508_u16 => ErrorKind::ER_DROP_LAST_PARTITION,
            1509_u16 => ErrorKind::ER_COALESCE_ONLY_ON_HASH_PARTITION,
            1510_u16 => ErrorKind::ER_REORG_HASH_ONLY_ON_SAME_N,
            1511_u16 => ErrorKind::ER_REORG_NO_PARAM_ERROR,
            1512_u16 => ErrorKind::ER_ONLY_ON_RANGE_LIST_PARTITION,
            1513_u16 => ErrorKind::ER_ADD_PARTITION_SUBPART_ERROR,
            1514_u16 => ErrorKind::ER_ADD_PARTITION_NO_NEW_PARTITION,
            1515_u16 => ErrorKind::ER_COALESCE_PARTITION_NO_PARTITION,
            1516_u16 => ErrorKind::ER_REORG_PARTITION_NOT_EXIST,
            1517_u16 => ErrorKind::ER_SAME_NAME_PARTITION,
            1518_u16 => ErrorKind::ER_NO_BINLOG_ERROR,
            1519_u16 => ErrorKind::ER_CONSECUTIVE_REORG_PARTITIONS,
            1520_u16 => ErrorKind::ER_REORG_OUTSIDE_RANGE,
            1521_u16 => ErrorKind::ER_PARTITION_FUNCTION_FAILURE,
            1522_u16 => ErrorKind::ER_PART_STATE_ERROR,
            1523_u16 => ErrorKind::ER_LIMITED_PART_RANGE,
            1524_u16 => ErrorKind::ER_PLUGIN_IS_NOT_LOADED,
            1525_u16 => ErrorKind::ER_WRONG_VALUE,
            1526_u16 => ErrorKind::ER_NO_PARTITION_FOR_GIVEN_VALUE,
            1527_u16 => ErrorKind::ER_FILEGROUP_OPTION_ONLY_ONCE,
            1528_u16 => ErrorKind::ER_CREATE_FILEGROUP_FAILED,
            1529_u16 => ErrorKind::ER_DROP_FILEGROUP_FAILED,
            1530_u16 => ErrorKind::ER_TABLESPACE_AUTO_EXTEND_ERROR,
            1531_u16 => ErrorKind::ER_WRONG_SIZE_NUMBER,
            1532_u16 => ErrorKind::ER_SIZE_OVERFLOW_ERROR,
            1533_u16 => ErrorKind::ER_ALTER_FILEGROUP_FAILED,
            1534_u16 => ErrorKind::ER_BINLOG_ROW_LOGGING_FAILED,
            1535_u16 => ErrorKind::ER_BINLOG_ROW_WRONG_TABLE_DEF,
            1536_u16 => ErrorKind::ER_BINLOG_ROW_RBR_TO_SBR,
            1537_u16 => ErrorKind::ER_EVENT_ALREADY_EXISTS,
            1538_u16 => ErrorKind::ER_EVENT_STORE_FAILED,
            1539_u16 => ErrorKind::ER_EVENT_DOES_NOT_EXIST,
            1540_u16 => ErrorKind::ER_EVENT_CANT_ALTER,
            1541_u16 => ErrorKind::ER_EVENT_DROP_FAILED,
            1542_u16 => ErrorKind::ER_EVENT_INTERVAL_NOT_POSITIVE_OR_TOO_BIG,
            1543_u16 => ErrorKind::ER_EVENT_ENDS_BEFORE_STARTS,
            1544_u16 => ErrorKind::ER_EVENT_EXEC_TIME_IN_THE_PAST,
            1545_u16 => ErrorKind::ER_EVENT_OPEN_TABLE_FAILED,
            1546_u16 => ErrorKind::ER_EVENT_NEITHER_M_EXPR_NOR_M_AT,
            1547_u16 => ErrorKind::ER_COL_COUNT_DOESNT_MATCH_CORRUPTED,
            1548_u16 => ErrorKind::ER_CANNOT_LOAD_FROM_TABLE,
            1549_u16 => ErrorKind::ER_EVENT_CANNOT_DELETE,
            1550_u16 => ErrorKind::ER_EVENT_COMPILE_ERROR,
            1551_u16 => ErrorKind::ER_EVENT_SAME_NAME,
            1552_u16 => ErrorKind::ER_EVENT_DATA_TOO_LONG,
            1553_u16 => ErrorKind::ER_DROP_INDEX_FK,
            1554_u16 => ErrorKind::ER_WARN_DEPRECATED_SYNTAX_WITH_VER,
            1555_u16 => ErrorKind::ER_CANT_WRITE_LOCK_LOG_TABLE,
            1556_u16 => ErrorKind::ER_CANT_LOCK_LOG_TABLE,
            1557_u16 => ErrorKind::ER_FOREIGN_DUPLICATE_KEY,
            1558_u16 => ErrorKind::ER_COL_COUNT_DOESNT_MATCH_PLEASE_UPDATE,
            1559_u16 => ErrorKind::ER_TEMP_TABLE_PREVENTS_SWITCH_OUT_OF_RBR,
            1560_u16 => ErrorKind::ER_STORED_FUNCTION_PREVENTS_SWITCH_BINLOG_FORMAT,
            1561_u16 => ErrorKind::ER_NDB_CANT_SWITCH_BINLOG_FORMAT,
            1562_u16 => ErrorKind::ER_PARTITION_NO_TEMPORARY,
            1563_u16 => ErrorKind::ER_PARTITION_CONST_DOMAIN_ERROR,
            1564_u16 => ErrorKind::ER_PARTITION_FUNCTION_IS_NOT_ALLOWED,
            1565_u16 => ErrorKind::ER_DDL_LOG_ERROR,
            1566_u16 => ErrorKind::ER_NULL_IN_VALUES_LESS_THAN,
            1567_u16 => ErrorKind::ER_WRONG_PARTITION_NAME,
            1568_u16 => ErrorKind::ER_CANT_CHANGE_TX_ISOLATION,
            1569_u16 => ErrorKind::ER_DUP_ENTRY_AUTOINCREMENT_CASE,
            1570_u16 => ErrorKind::ER_EVENT_MODIFY_QUEUE_ERROR,
            1571_u16 => ErrorKind::ER_EVENT_SET_VAR_ERROR,
            1572_u16 => ErrorKind::ER_PARTITION_MERGE_ERROR,
            1573_u16 => ErrorKind::ER_CANT_ACTIVATE_LOG,
            1574_u16 => ErrorKind::ER_RBR_NOT_AVAILABLE,
            1575_u16 => ErrorKind::ER_BASE64_DECODE_ERROR,
            1576_u16 => ErrorKind::ER_EVENT_RECURSION_FORBIDDEN,
            1577_u16 => ErrorKind::ER_EVENTS_DB_ERROR,
            1578_u16 => ErrorKind::ER_ONLY_INTEGERS_ALLOWED,
            1579_u16 => ErrorKind::ER_UNSUPORTED_LOG_ENGINE,
            1580_u16 => ErrorKind::ER_BAD_LOG_STATEMENT,
            1581_u16 => ErrorKind::ER_CANT_RENAME_LOG_TABLE,
            1582_u16 => ErrorKind::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT,
            1583_u16 => ErrorKind::ER_WRONG_PARAMETERS_TO_NATIVE_FCT,
            1584_u16 => ErrorKind::ER_WRONG_PARAMETERS_TO_STORED_FCT,
            1585_u16 => ErrorKind::ER_NATIVE_FCT_NAME_COLLISION,
            1586_u16 => ErrorKind::ER_DUP_ENTRY_WITH_KEY_NAME,
            1587_u16 => ErrorKind::ER_BINLOG_PURGE_EMFILE,
            1588_u16 => ErrorKind::ER_EVENT_CANNOT_CREATE_IN_THE_PAST,
            1589_u16 => ErrorKind::ER_EVENT_CANNOT_ALTER_IN_THE_PAST,
            1590_u16 => ErrorKind::ER_SLAVE_INCIDENT,
            1591_u16 => ErrorKind::ER_NO_PARTITION_FOR_GIVEN_VALUE_SILENT,
            1592_u16 => ErrorKind::ER_BINLOG_UNSAFE_STATEMENT,
            1593_u16 => ErrorKind::ER_SLAVE_FATAL_ERROR,
            1594_u16 => ErrorKind::ER_SLAVE_RELAY_LOG_READ_FAILURE,
            1595_u16 => ErrorKind::ER_SLAVE_RELAY_LOG_WRITE_FAILURE,
            1596_u16 => ErrorKind::ER_SLAVE_CREATE_EVENT_FAILURE,
            1597_u16 => ErrorKind::ER_SLAVE_MASTER_COM_FAILURE,
            1598_u16 => ErrorKind::ER_BINLOG_LOGGING_IMPOSSIBLE,
            1599_u16 => ErrorKind::ER_VIEW_NO_CREATION_CTX,
            1600_u16 => ErrorKind::ER_VIEW_INVALID_CREATION_CTX,
            1601_u16 => ErrorKind::ER_SR_INVALID_CREATION_CTX,
            1602_u16 => ErrorKind::ER_TRG_CORRUPTED_FILE,
            1603_u16 => ErrorKind::ER_TRG_NO_CREATION_CTX,
            1604_u16 => ErrorKind::ER_TRG_INVALID_CREATION_CTX,
            1605_u16 => ErrorKind::ER_EVENT_INVALID_CREATION_CTX,
            1606_u16 => ErrorKind::ER_TRG_CANT_OPEN_TABLE,
            1607_u16 => ErrorKind::ER_CANT_CREATE_SROUTINE,
            1608_u16 => ErrorKind::ER_UNUSED_11,
            1609_u16 => ErrorKind::ER_NO_FORMAT_DESCRIPTION_EVENT_BEFORE_BINLOG_STATEMENT,
            1610_u16 => ErrorKind::ER_SLAVE_CORRUPT_EVENT,
            1611_u16 => ErrorKind::ER_LOAD_DATA_INVALID_COLUMN,
            1612_u16 => ErrorKind::ER_LOG_PURGE_NO_FILE,
            1613_u16 => ErrorKind::ER_XA_RBTIMEOUT,
            1614_u16 => ErrorKind::ER_XA_RBDEADLOCK,
            1615_u16 => ErrorKind::ER_NEED_REPREPARE,
            1616_u16 => ErrorKind::ER_DELAYED_NOT_SUPPORTED,
            1617_u16 => ErrorKind::WARN_NO_MASTER_INF,
            1618_u16 => ErrorKind::WARN_OPTION_IGNORED,
            1619_u16 => ErrorKind::WARN_PLUGIN_DELETE_BUILTIN,
            1620_u16 => ErrorKind::WARN_PLUGIN_BUSY,
            1621_u16 => ErrorKind::ER_VARIABLE_IS_READONLY,
            1622_u16 => ErrorKind::ER_WARN_ENGINE_TRANSACTION_ROLLBACK,
            1623_u16 => ErrorKind::ER_SLAVE_HEARTBEAT_FAILURE,
            1624_u16 => ErrorKind::ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE,
            1625_u16 => ErrorKind::ER_NDB_REPLICATION_SCHEMA_ERROR,
            1626_u16 => ErrorKind::ER_CONFLICT_FN_PARSE_ERROR,
            1627_u16 => ErrorKind::ER_EXCEPTIONS_WRITE_ERROR,
            1628_u16 => ErrorKind::ER_TOO_LONG_TABLE_COMMENT,
            1629_u16 => ErrorKind::ER_TOO_LONG_FIELD_COMMENT,
            1630_u16 => ErrorKind::ER_FUNC_INEXISTENT_NAME_COLLISION,
            1631_u16 => ErrorKind::ER_DATABASE_NAME,
            1632_u16 => ErrorKind::ER_TABLE_NAME,
            1633_u16 => ErrorKind::ER_PARTITION_NAME,
            1634_u16 => ErrorKind::ER_SUBPARTITION_NAME,
            1635_u16 => ErrorKind::ER_TEMPORARY_NAME,
            1636_u16 => ErrorKind::ER_RENAMED_NAME,
            1637_u16 => ErrorKind::ER_TOO_MANY_CONCURRENT_TRXS,
            1638_u16 => ErrorKind::WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED,
            1639_u16 => ErrorKind::ER_DEBUG_SYNC_TIMEOUT,
            1640_u16 => ErrorKind::ER_DEBUG_SYNC_HIT_LIMIT,
            1641_u16 => ErrorKind::ER_DUP_SIGNAL_SET,
            1642_u16 => ErrorKind::ER_SIGNAL_WARN,
            1643_u16 => ErrorKind::ER_SIGNAL_NOT_FOUND,
            1644_u16 => ErrorKind::ER_SIGNAL_EXCEPTION,
            1645_u16 => ErrorKind::ER_RESIGNAL_WITHOUT_ACTIVE_HANDLER,
            1646_u16 => ErrorKind::ER_SIGNAL_BAD_CONDITION_TYPE,
            1647_u16 => ErrorKind::WARN_COND_ITEM_TRUNCATED,
            1648_u16 => ErrorKind::ER_COND_ITEM_TOO_LONG,
            1649_u16 => ErrorKind::ER_UNKNOWN_LOCALE,
            1650_u16 => ErrorKind::ER_SLAVE_IGNORE_SERVER_IDS,
            1651_u16 => ErrorKind::ER_QUERY_CACHE_DISABLED,
            1652_u16 => ErrorKind::ER_SAME_NAME_PARTITION_FIELD,
            1653_u16 => ErrorKind::ER_PARTITION_COLUMN_LIST_ERROR,
            1654_u16 => ErrorKind::ER_WRONG_TYPE_COLUMN_VALUE_ERROR,
            1655_u16 => ErrorKind::ER_TOO_MANY_PARTITION_FUNC_FIELDS_ERROR,
            1656_u16 => ErrorKind::ER_MAXVALUE_IN_VALUES_IN,
            1657_u16 => ErrorKind::ER_TOO_MANY_VALUES_ERROR,
            1658_u16 => ErrorKind::ER_ROW_SINGLE_PARTITION_FIELD_ERROR,
            1659_u16 => ErrorKind::ER_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD,
            1660_u16 => ErrorKind::ER_PARTITION_FIELDS_TOO_LONG,
            1661_u16 => ErrorKind::ER_BINLOG_ROW_ENGINE_AND_STMT_ENGINE,
            1662_u16 => ErrorKind::ER_BINLOG_ROW_MODE_AND_STMT_ENGINE,
            1663_u16 => ErrorKind::ER_BINLOG_UNSAFE_AND_STMT_ENGINE,
            1664_u16 => ErrorKind::ER_BINLOG_ROW_INJECTION_AND_STMT_ENGINE,
            1665_u16 => ErrorKind::ER_BINLOG_STMT_MODE_AND_ROW_ENGINE,
            1666_u16 => ErrorKind::ER_BINLOG_ROW_INJECTION_AND_STMT_MODE,
            1667_u16 => ErrorKind::ER_BINLOG_MULTIPLE_ENGINES_AND_SELF_LOGGING_ENGINE,
            1668_u16 => ErrorKind::ER_BINLOG_UNSAFE_LIMIT,
            1669_u16 => ErrorKind::ER_BINLOG_UNSAFE_INSERT_DELAYED,
            1670_u16 => ErrorKind::ER_BINLOG_UNSAFE_SYSTEM_TABLE,
            1671_u16 => ErrorKind::ER_BINLOG_UNSAFE_AUTOINC_COLUMNS,
            1672_u16 => ErrorKind::ER_BINLOG_UNSAFE_UDF,
            1673_u16 => ErrorKind::ER_BINLOG_UNSAFE_SYSTEM_VARIABLE,
            1674_u16 => ErrorKind::ER_BINLOG_UNSAFE_SYSTEM_FUNCTION,
            1675_u16 => ErrorKind::ER_BINLOG_UNSAFE_NONTRANS_AFTER_TRANS,
            1676_u16 => ErrorKind::ER_MESSAGE_AND_STATEMENT,
            1677_u16 => ErrorKind::ER_SLAVE_CONVERSION_FAILED,
            1678_u16 => ErrorKind::ER_SLAVE_CANT_CREATE_CONVERSION,
            1679_u16 => ErrorKind::ER_INSIDE_TRANSACTION_PREVENTS_SWITCH_BINLOG_FORMAT,
            1680_u16 => ErrorKind::ER_PATH_LENGTH,
            1681_u16 => ErrorKind::ER_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT,
            1682_u16 => ErrorKind::ER_WRONG_NATIVE_TABLE_STRUCTURE,
            1683_u16 => ErrorKind::ER_WRONG_PERFSCHEMA_USAGE,
            1684_u16 => ErrorKind::ER_WARN_I_S_SKIPPED_TABLE,
            1685_u16 => ErrorKind::ER_INSIDE_TRANSACTION_PREVENTS_SWITCH_BINLOG_DIRECT,
            1686_u16 => ErrorKind::ER_STORED_FUNCTION_PREVENTS_SWITCH_BINLOG_DIRECT,
            1687_u16 => ErrorKind::ER_SPATIAL_MUST_HAVE_GEOM_COL,
            1688_u16 => ErrorKind::ER_TOO_LONG_INDEX_COMMENT,
            1689_u16 => ErrorKind::ER_LOCK_ABORTED,
            1690_u16 => ErrorKind::ER_DATA_OUT_OF_RANGE,
            1691_u16 => ErrorKind::ER_WRONG_SPVAR_TYPE_IN_LIMIT,
            1692_u16 => ErrorKind::ER_BINLOG_UNSAFE_MULTIPLE_ENGINES_AND_SELF_LOGGING_ENGINE,
            1693_u16 => ErrorKind::ER_BINLOG_UNSAFE_MIXED_STATEMENT,
            1694_u16 => ErrorKind::ER_INSIDE_TRANSACTION_PREVENTS_SWITCH_SQL_LOG_BIN,
            1695_u16 => ErrorKind::ER_STORED_FUNCTION_PREVENTS_SWITCH_SQL_LOG_BIN,
            1696_u16 => ErrorKind::ER_FAILED_READ_FROM_PAR_FILE,
            1697_u16 => ErrorKind::ER_VALUES_IS_NOT_INT_TYPE_ERROR,
            1698_u16 => ErrorKind::ER_ACCESS_DENIED_NO_PASSWORD_ERROR,
            1699_u16 => ErrorKind::ER_SET_PASSWORD_AUTH_PLUGIN,
            1700_u16 => ErrorKind::ER_GRANT_PLUGIN_USER_EXISTS,
            1701_u16 => ErrorKind::ER_TRUNCATE_ILLEGAL_FK,
            1702_u16 => ErrorKind::ER_PLUGIN_IS_PERMANENT,
            1703_u16 => ErrorKind::ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE_MIN,
            1704_u16 => ErrorKind::ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE_MAX,
            1705_u16 => ErrorKind::ER_STMT_CACHE_FULL,
            1706_u16 => ErrorKind::ER_MULTI_UPDATE_KEY_CONFLICT,
            1707_u16 => ErrorKind::ER_TABLE_NEEDS_REBUILD,
            1708_u16 => ErrorKind::WARN_OPTION_BELOW_LIMIT,
            1709_u16 => ErrorKind::ER_INDEX_COLUMN_TOO_LONG,
            1710_u16 => ErrorKind::ER_ERROR_IN_TRIGGER_BODY,
            1711_u16 => ErrorKind::ER_ERROR_IN_UNKNOWN_TRIGGER_BODY,
            1712_u16 => ErrorKind::ER_INDEX_CORRUPT,
            1713_u16 => ErrorKind::ER_UNDO_RECORD_TOO_BIG,
            1714_u16 => ErrorKind::ER_BINLOG_UNSAFE_INSERT_IGNORE_SELECT,
            1715_u16 => ErrorKind::ER_BINLOG_UNSAFE_INSERT_SELECT_UPDATE,
            1716_u16 => ErrorKind::ER_BINLOG_UNSAFE_REPLACE_SELECT,
            1717_u16 => ErrorKind::ER_BINLOG_UNSAFE_CREATE_IGNORE_SELECT,
            1718_u16 => ErrorKind::ER_BINLOG_UNSAFE_CREATE_REPLACE_SELECT,
            1719_u16 => ErrorKind::ER_BINLOG_UNSAFE_UPDATE_IGNORE,
            1720_u16 => ErrorKind::ER_PLUGIN_NO_UNINSTALL,
            1721_u16 => ErrorKind::ER_PLUGIN_NO_INSTALL,
            1722_u16 => ErrorKind::ER_BINLOG_UNSAFE_WRITE_AUTOINC_SELECT,
            1723_u16 => ErrorKind::ER_BINLOG_UNSAFE_CREATE_SELECT_AUTOINC,
            1724_u16 => ErrorKind::ER_BINLOG_UNSAFE_INSERT_TWO_KEYS,
            1725_u16 => ErrorKind::ER_TABLE_IN_FK_CHECK,
            1726_u16 => ErrorKind::ER_UNSUPPORTED_ENGINE,
            1727_u16 => ErrorKind::ER_BINLOG_UNSAFE_AUTOINC_NOT_FIRST,
            1728_u16 => ErrorKind::ER_CANNOT_LOAD_FROM_TABLE_V2,
            1729_u16 => ErrorKind::ER_MASTER_DELAY_VALUE_OUT_OF_RANGE,
            1730_u16 => ErrorKind::ER_ONLY_FD_AND_RBR_EVENTS_ALLOWED_IN_BINLOG_STATEMENT,
            1731_u16 => ErrorKind::ER_PARTITION_EXCHANGE_DIFFERENT_OPTION,
            1732_u16 => ErrorKind::ER_PARTITION_EXCHANGE_PART_TABLE,
            1733_u16 => ErrorKind::ER_PARTITION_EXCHANGE_TEMP_TABLE,
            1734_u16 => ErrorKind::ER_PARTITION_INSTEAD_OF_SUBPARTITION,
            1735_u16 => ErrorKind::ER_UNKNOWN_PARTITION,
            1736_u16 => ErrorKind::ER_TABLES_DIFFERENT_METADATA,
            1737_u16 => ErrorKind::ER_ROW_DOES_NOT_MATCH_PARTITION,
            1738_u16 => ErrorKind::ER_BINLOG_CACHE_SIZE_GREATER_THAN_MAX,
            1739_u16 => ErrorKind::ER_WARN_INDEX_NOT_APPLICABLE,
            1740_u16 => ErrorKind::ER_PARTITION_EXCHANGE_FOREIGN_KEY,
            1741_u16 => ErrorKind::ER_NO_SUCH_KEY_VALUE,
            1742_u16 => ErrorKind::ER_RPL_INFO_DATA_TOO_LONG,
            1743_u16 => ErrorKind::ER_NETWORK_READ_EVENT_CHECKSUM_FAILURE,
            1744_u16 => ErrorKind::ER_BINLOG_READ_EVENT_CHECKSUM_FAILURE,
            1745_u16 => ErrorKind::ER_BINLOG_STMT_CACHE_SIZE_GREATER_THAN_MAX,
            1746_u16 => ErrorKind::ER_CANT_UPDATE_TABLE_IN_CREATE_TABLE_SELECT,
            1747_u16 => ErrorKind::ER_PARTITION_CLAUSE_ON_NONPARTITIONED,
            1748_u16 => ErrorKind::ER_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET,
            1749_u16 => ErrorKind::ER_NO_SUCH_PARTITION_UNUSED,
            1750_u16 => ErrorKind::ER_CHANGE_RPL_INFO_REPOSITORY_FAILURE,
            1751_u16 => ErrorKind::ER_WARNING_NOT_COMPLETE_ROLLBACK_WITH_CREATED_TEMP_TABLE,
            1752_u16 => ErrorKind::ER_WARNING_NOT_COMPLETE_ROLLBACK_WITH_DROPPED_TEMP_TABLE,
            1753_u16 => ErrorKind::ER_MTS_FEATURE_IS_NOT_SUPPORTED,
            1754_u16 => ErrorKind::ER_MTS_UPDATED_DBS_GREATER_MAX,
            1755_u16 => ErrorKind::ER_MTS_CANT_PARALLEL,
            1756_u16 => ErrorKind::ER_MTS_INCONSISTENT_DATA,
            1757_u16 => ErrorKind::ER_FULLTEXT_NOT_SUPPORTED_WITH_PARTITIONING,
            1758_u16 => ErrorKind::ER_DA_INVALID_CONDITION_NUMBER,
            1759_u16 => ErrorKind::ER_INSECURE_PLAIN_TEXT,
            1760_u16 => ErrorKind::ER_INSECURE_CHANGE_MASTER,
            1761_u16 => ErrorKind::ER_FOREIGN_DUPLICATE_KEY_WITH_CHILD_INFO,
            1762_u16 => ErrorKind::ER_FOREIGN_DUPLICATE_KEY_WITHOUT_CHILD_INFO,
            1763_u16 => ErrorKind::ER_SQLTHREAD_WITH_SECURE_SLAVE,
            1764_u16 => ErrorKind::ER_TABLE_HAS_NO_FT,
            1765_u16 => ErrorKind::ER_VARIABLE_NOT_SETTABLE_IN_SF_OR_TRIGGER,
            1766_u16 => ErrorKind::ER_VARIABLE_NOT_SETTABLE_IN_TRANSACTION,
            1767_u16 => ErrorKind::ER_GTID_NEXT_IS_NOT_IN_GTID_NEXT_LIST,
            1768_u16 => {
                ErrorKind::ER_CANT_CHANGE_GTID_NEXT_IN_TRANSACTION_WHEN_GTID_NEXT_LIST_IS_NULL
            }
            1769_u16 => ErrorKind::ER_SET_STATEMENT_CANNOT_INVOKE_FUNCTION,
            1770_u16 => ErrorKind::ER_GTID_NEXT_CANT_BE_AUTOMATIC_IF_GTID_NEXT_LIST_IS_NON_NULL,
            1771_u16 => ErrorKind::ER_SKIPPING_LOGGED_TRANSACTION,
            1772_u16 => ErrorKind::ER_MALFORMED_GTID_SET_SPECIFICATION,
            1773_u16 => ErrorKind::ER_MALFORMED_GTID_SET_ENCODING,
            1774_u16 => ErrorKind::ER_MALFORMED_GTID_SPECIFICATION,
            1775_u16 => ErrorKind::ER_GNO_EXHAUSTED,
            1776_u16 => ErrorKind::ER_BAD_SLAVE_AUTO_POSITION,
            1777_u16 => ErrorKind::ER_AUTO_POSITION_REQUIRES_GTID_MODE_ON,
            1778_u16 => ErrorKind::ER_CANT_DO_IMPLICIT_COMMIT_IN_TRX_WHEN_GTID_NEXT_IS_SET,
            1779_u16 => ErrorKind::ER_GTID_MODE_2_OR_3_REQUIRES_DISABLE_GTID_UNSAFE_STATEMENTS_ON,
            1780_u16 => ErrorKind::ER_GTID_MODE_REQUIRES_BINLOG,
            1781_u16 => ErrorKind::ER_CANT_SET_GTID_NEXT_TO_GTID_WHEN_GTID_MODE_IS_OFF,
            1782_u16 => ErrorKind::ER_CANT_SET_GTID_NEXT_TO_ANONYMOUS_WHEN_GTID_MODE_IS_ON,
            1783_u16 => ErrorKind::ER_CANT_SET_GTID_NEXT_LIST_TO_NON_NULL_WHEN_GTID_MODE_IS_OFF,
            1784_u16 => ErrorKind::ER_FOUND_GTID_EVENT_WHEN_GTID_MODE_IS_OFF,
            1785_u16 => ErrorKind::ER_GTID_UNSAFE_NON_TRANSACTIONAL_TABLE,
            1786_u16 => ErrorKind::ER_GTID_UNSAFE_CREATE_SELECT,
            1787_u16 => ErrorKind::ER_GTID_UNSAFE_CREATE_DROP_TEMPORARY_TABLE_IN_TRANSACTION,
            1788_u16 => ErrorKind::ER_GTID_MODE_CAN_ONLY_CHANGE_ONE_STEP_AT_A_TIME,
            1789_u16 => ErrorKind::ER_MASTER_HAS_PURGED_REQUIRED_GTIDS,
            1790_u16 => ErrorKind::ER_CANT_SET_GTID_NEXT_WHEN_OWNING_GTID,
            1791_u16 => ErrorKind::ER_UNKNOWN_EXPLAIN_FORMAT,
            1792_u16 => ErrorKind::ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION,
            1793_u16 => ErrorKind::ER_TOO_LONG_TABLE_PARTITION_COMMENT,
            1794_u16 => ErrorKind::ER_SLAVE_CONFIGURATION,
            1795_u16 => ErrorKind::ER_INNODB_FT_LIMIT,
            1796_u16 => ErrorKind::ER_INNODB_NO_FT_TEMP_TABLE,
            1797_u16 => ErrorKind::ER_INNODB_FT_WRONG_DOCID_COLUMN,
            1798_u16 => ErrorKind::ER_INNODB_FT_WRONG_DOCID_INDEX,
            1799_u16 => ErrorKind::ER_INNODB_ONLINE_LOG_TOO_BIG,
            1800_u16 => ErrorKind::ER_UNKNOWN_ALTER_ALGORITHM,
            1801_u16 => ErrorKind::ER_UNKNOWN_ALTER_LOCK,
            1802_u16 => ErrorKind::ER_MTS_CHANGE_MASTER_CANT_RUN_WITH_GAPS,
            1803_u16 => ErrorKind::ER_MTS_RECOVERY_FAILURE,
            1804_u16 => ErrorKind::ER_MTS_RESET_WORKERS,
            1805_u16 => ErrorKind::ER_COL_COUNT_DOESNT_MATCH_CORRUPTED_V2,
            1806_u16 => ErrorKind::ER_SLAVE_SILENT_RETRY_TRANSACTION,
            1807_u16 => ErrorKind::ER_DISCARD_FK_CHECKS_RUNNING,
            1808_u16 => ErrorKind::ER_TABLE_SCHEMA_MISMATCH,
            1809_u16 => ErrorKind::ER_TABLE_IN_SYSTEM_TABLESPACE,
            1810_u16 => ErrorKind::ER_IO_READ_ERROR,
            1811_u16 => ErrorKind::ER_IO_WRITE_ERROR,
            1812_u16 => ErrorKind::ER_TABLESPACE_MISSING,
            1813_u16 => ErrorKind::ER_TABLESPACE_EXISTS,
            1814_u16 => ErrorKind::ER_TABLESPACE_DISCARDED,
            1815_u16 => ErrorKind::ER_INTERNAL_ERROR,
            1816_u16 => ErrorKind::ER_INNODB_IMPORT_ERROR,
            1817_u16 => ErrorKind::ER_INNODB_INDEX_CORRUPT,
            1818_u16 => ErrorKind::ER_INVALID_YEAR_COLUMN_LENGTH,
            1819_u16 => ErrorKind::ER_NOT_VALID_PASSWORD,
            1820_u16 => ErrorKind::ER_MUST_CHANGE_PASSWORD,
            1821_u16 => ErrorKind::ER_FK_NO_INDEX_CHILD,
            1822_u16 => ErrorKind::ER_FK_NO_INDEX_PARENT,
            1823_u16 => ErrorKind::ER_FK_FAIL_ADD_SYSTEM,
            1824_u16 => ErrorKind::ER_FK_CANNOT_OPEN_PARENT,
            1825_u16 => ErrorKind::ER_FK_INCORRECT_OPTION,
            1826_u16 => ErrorKind::ER_FK_DUP_NAME,
            1827_u16 => ErrorKind::ER_PASSWORD_FORMAT,
            1828_u16 => ErrorKind::ER_FK_COLUMN_CANNOT_DROP,
            1829_u16 => ErrorKind::ER_FK_COLUMN_CANNOT_DROP_CHILD,
            1830_u16 => ErrorKind::ER_FK_COLUMN_NOT_NULL,
            1831_u16 => ErrorKind::ER_DUP_INDEX,
            1832_u16 => ErrorKind::ER_FK_COLUMN_CANNOT_CHANGE,
            1833_u16 => ErrorKind::ER_FK_COLUMN_CANNOT_CHANGE_CHILD,
            1834_u16 => ErrorKind::ER_FK_CANNOT_DELETE_PARENT,
            1835_u16 => ErrorKind::ER_MALFORMED_PACKET,
            1836_u16 => ErrorKind::ER_READ_ONLY_MODE,
            1837_u16 => ErrorKind::ER_GTID_NEXT_TYPE_UNDEFINED_GROUP,
            1838_u16 => ErrorKind::ER_VARIABLE_NOT_SETTABLE_IN_SP,
            1839_u16 => ErrorKind::ER_CANT_SET_GTID_PURGED_WHEN_GTID_MODE_IS_OFF,
            1840_u16 => ErrorKind::ER_CANT_SET_GTID_PURGED_WHEN_GTID_EXECUTED_IS_NOT_EMPTY,
            1841_u16 => ErrorKind::ER_CANT_SET_GTID_PURGED_WHEN_OWNED_GTIDS_IS_NOT_EMPTY,
            1842_u16 => ErrorKind::ER_GTID_PURGED_WAS_CHANGED,
            1843_u16 => ErrorKind::ER_GTID_EXECUTED_WAS_CHANGED,
            1844_u16 => ErrorKind::ER_BINLOG_STMT_MODE_AND_NO_REPL_TABLES,
            1845_u16 => ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED,
            1846_u16 => ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON,
            1847_u16 => ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_COPY,
            1848_u16 => ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_PARTITION,
            1849_u16 => ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_FK_RENAME,
            1850_u16 => ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_COLUMN_TYPE,
            1851_u16 => ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_FK_CHECK,
            1852_u16 => ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_IGNORE,
            1853_u16 => ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_NOPK,
            1854_u16 => ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_AUTOINC,
            1855_u16 => ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_HIDDEN_FTS,
            1856_u16 => ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_CHANGE_FTS,
            1857_u16 => ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_FTS,
            1858_u16 => ErrorKind::ER_SQL_SLAVE_SKIP_COUNTER_NOT_SETTABLE_IN_GTID_MODE,
            1859_u16 => ErrorKind::ER_DUP_UNKNOWN_IN_INDEX,
            1860_u16 => ErrorKind::ER_IDENT_CAUSES_TOO_LONG_PATH,
            1861_u16 => ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_NOT_NULL,
            1862_u16 => ErrorKind::ER_MUST_CHANGE_PASSWORD_LOGIN,
            1863_u16 => ErrorKind::ER_ROW_IN_WRONG_PARTITION,
            1864_u16 => ErrorKind::ER_MTS_EVENT_BIGGER_PENDING_JOBS_SIZE_MAX,
            1865_u16 => ErrorKind::ER_INNODB_NO_FT_USES_PARSER,
            1866_u16 => ErrorKind::ER_BINLOG_LOGICAL_CORRUPTION,
            1867_u16 => ErrorKind::ER_WARN_PURGE_LOG_IN_USE,
            1868_u16 => ErrorKind::ER_WARN_PURGE_LOG_IS_ACTIVE,
            1869_u16 => ErrorKind::ER_AUTO_INCREMENT_CONFLICT,
            1870_u16 => ErrorKind::WARN_ON_BLOCKHOLE_IN_RBR,
            1871_u16 => ErrorKind::ER_SLAVE_MI_INIT_REPOSITORY,
            1872_u16 => ErrorKind::ER_SLAVE_RLI_INIT_REPOSITORY,
            1873_u16 => ErrorKind::ER_ACCESS_DENIED_CHANGE_USER_ERROR,
            1874_u16 => ErrorKind::ER_INNODB_READ_ONLY,
            1875_u16 => ErrorKind::ER_STOP_SLAVE_SQL_THREAD_TIMEOUT,
            1876_u16 => ErrorKind::ER_STOP_SLAVE_IO_THREAD_TIMEOUT,
            1877_u16 => ErrorKind::ER_TABLE_CORRUPT,
            1878_u16 => ErrorKind::ER_TEMP_FILE_WRITE_FAILURE,
            1879_u16 => ErrorKind::ER_INNODB_FT_AUX_NOT_HEX_ID,
            1880_u16 => ErrorKind::ER_OLD_TEMPORALS_UPGRADED,
            1881_u16 => ErrorKind::ER_INNODB_FORCED_RECOVERY,
            1882_u16 => ErrorKind::ER_AES_INVALID_IV,
            1883_u16 => ErrorKind::ER_PLUGIN_CANNOT_BE_UNINSTALLED,
            1884_u16 => ErrorKind::ER_GTID_UNSAFE_BINLOG_SPLITTABLE_STATEMENT_AND_GTID_GROUP,
            1885_u16 => ErrorKind::ER_SLAVE_HAS_MORE_GTIDS_THAN_MASTER,
            _ => ErrorKind::ER_UNKNOWN_ERROR,
        }
    }
}

impl From<ErrorKind> for u16 {
    fn from(x: ErrorKind) -> u16 {
        match x {
            ErrorKind::ER_HASHCHK => 1000_u16,
            ErrorKind::ER_NISAMCHK => 1001_u16,
            ErrorKind::ER_NO => 1002_u16,
            ErrorKind::ER_YES => 1003_u16,
            ErrorKind::ER_CANT_CREATE_FILE => 1004_u16,
            ErrorKind::ER_CANT_CREATE_TABLE => 1005_u16,
            ErrorKind::ER_CANT_CREATE_DB => 1006_u16,
            ErrorKind::ER_DB_CREATE_EXISTS => 1007_u16,
            ErrorKind::ER_DB_DROP_EXISTS => 1008_u16,
            ErrorKind::ER_DB_DROP_DELETE => 1009_u16,
            ErrorKind::ER_DB_DROP_RMDIR => 1010_u16,
            ErrorKind::ER_CANT_DELETE_FILE => 1011_u16,
            ErrorKind::ER_CANT_FIND_SYSTEM_REC => 1012_u16,
            ErrorKind::ER_CANT_GET_STAT => 1013_u16,
            ErrorKind::ER_CANT_GET_WD => 1014_u16,
            ErrorKind::ER_CANT_LOCK => 1015_u16,
            ErrorKind::ER_CANT_OPEN_FILE => 1016_u16,
            ErrorKind::ER_FILE_NOT_FOUND => 1017_u16,
            ErrorKind::ER_CANT_READ_DIR => 1018_u16,
            ErrorKind::ER_CANT_SET_WD => 1019_u16,
            ErrorKind::ER_CHECKREAD => 1020_u16,
            ErrorKind::ER_DISK_FULL => 1021_u16,
            ErrorKind::ER_DUP_KEY => 1022_u16,
            ErrorKind::ER_ERROR_ON_CLOSE => 1023_u16,
            ErrorKind::ER_ERROR_ON_READ => 1024_u16,
            ErrorKind::ER_ERROR_ON_RENAME => 1025_u16,
            ErrorKind::ER_ERROR_ON_WRITE => 1026_u16,
            ErrorKind::ER_FILE_USED => 1027_u16,
            ErrorKind::ER_FILSORT_ABORT => 1028_u16,
            ErrorKind::ER_FORM_NOT_FOUND => 1029_u16,
            ErrorKind::ER_GET_ERRN => 1030_u16,
            ErrorKind::ER_ILLEGAL_HA => 1031_u16,
            ErrorKind::ER_KEY_NOT_FOUND => 1032_u16,
            ErrorKind::ER_NOT_FORM_FILE => 1033_u16,
            ErrorKind::ER_NOT_KEYFILE => 1034_u16,
            ErrorKind::ER_OLD_KEYFILE => 1035_u16,
            ErrorKind::ER_OPEN_AS_READONLY => 1036_u16,
            ErrorKind::ER_OUTOFMEMORY => 1037_u16,
            ErrorKind::ER_OUT_OF_SORTMEMORY => 1038_u16,
            ErrorKind::ER_UNEXPECTED_EOF => 1039_u16,
            ErrorKind::ER_CON_COUNT_ERROR => 1040_u16,
            ErrorKind::ER_OUT_OF_RESOURCES => 1041_u16,
            ErrorKind::ER_BAD_HOST_ERROR => 1042_u16,
            ErrorKind::ER_HANDSHAKE_ERROR => 1043_u16,
            ErrorKind::ER_DBACCESS_DENIED_ERROR => 1044_u16,
            ErrorKind::ER_ACCESS_DENIED_ERROR => 1045_u16,
            ErrorKind::ER_NO_DB_ERROR => 1046_u16,
            ErrorKind::ER_UNKNOWN_COM_ERROR => 1047_u16,
            ErrorKind::ER_BAD_NULL_ERROR => 1048_u16,
            ErrorKind::ER_BAD_DB_ERROR => 1049_u16,
            ErrorKind::ER_TABLE_EXISTS_ERROR => 1050_u16,
            ErrorKind::ER_BAD_TABLE_ERROR => 1051_u16,
            ErrorKind::ER_NON_UNIQ_ERROR => 1052_u16,
            ErrorKind::ER_SERVER_SHUTDOWN => 1053_u16,
            ErrorKind::ER_BAD_FIELD_ERROR => 1054_u16,
            ErrorKind::ER_WRONG_FIELD_WITH_GROUP => 1055_u16,
            ErrorKind::ER_WRONG_GROUP_FIELD => 1056_u16,
            ErrorKind::ER_WRONG_SUM_SELECT => 1057_u16,
            ErrorKind::ER_WRONG_VALUE_COUNT => 1058_u16,
            ErrorKind::ER_TOO_LONG_IDENT => 1059_u16,
            ErrorKind::ER_DUP_FIELDNAME => 1060_u16,
            ErrorKind::ER_DUP_KEYNAME => 1061_u16,
            ErrorKind::ER_DUP_ENTRY => 1062_u16,
            ErrorKind::ER_WRONG_FIELD_SPEC => 1063_u16,
            ErrorKind::ER_PARSE_ERROR => 1064_u16,
            ErrorKind::ER_EMPTY_QUERY => 1065_u16,
            ErrorKind::ER_NONUNIQ_TABLE => 1066_u16,
            ErrorKind::ER_INVALID_DEFAULT => 1067_u16,
            ErrorKind::ER_MULTIPLE_PRI_KEY => 1068_u16,
            ErrorKind::ER_TOO_MANY_KEYS => 1069_u16,
            ErrorKind::ER_TOO_MANY_KEY_PARTS => 1070_u16,
            ErrorKind::ER_TOO_LONG_KEY => 1071_u16,
            ErrorKind::ER_KEY_COLUMN_DOES_NOT_EXITS => 1072_u16,
            ErrorKind::ER_BLOB_USED_AS_KEY => 1073_u16,
            ErrorKind::ER_TOO_BIG_FIELDLENGTH => 1074_u16,
            ErrorKind::ER_WRONG_AUTO_KEY => 1075_u16,
            ErrorKind::ER_READY => 1076_u16,
            ErrorKind::ER_NORMAL_SHUTDOWN => 1077_u16,
            ErrorKind::ER_GOT_SIGNAL => 1078_u16,
            ErrorKind::ER_SHUTDOWN_COMPLETE => 1079_u16,
            ErrorKind::ER_FORCING_CLOSE => 1080_u16,
            ErrorKind::ER_IPSOCK_ERROR => 1081_u16,
            ErrorKind::ER_NO_SUCH_INDEX => 1082_u16,
            ErrorKind::ER_WRONG_FIELD_TERMINATORS => 1083_u16,
            ErrorKind::ER_BLOBS_AND_NO_TERMINATED => 1084_u16,
            ErrorKind::ER_TEXTFILE_NOT_READABLE => 1085_u16,
            ErrorKind::ER_FILE_EXISTS_ERROR => 1086_u16,
            ErrorKind::ER_LOAD_INF => 1087_u16,
            ErrorKind::ER_ALTER_INF => 1088_u16,
            ErrorKind::ER_WRONG_SUB_KEY => 1089_u16,
            ErrorKind::ER_CANT_REMOVE_ALL_FIELDS => 1090_u16,
            ErrorKind::ER_CANT_DROP_FIELD_OR_KEY => 1091_u16,
            ErrorKind::ER_INSERT_INF => 1092_u16,
            ErrorKind::ER_UPDATE_TABLE_USED => 1093_u16,
            ErrorKind::ER_NO_SUCH_THREAD => 1094_u16,
            ErrorKind::ER_KILL_DENIED_ERROR => 1095_u16,
            ErrorKind::ER_NO_TABLES_USED => 1096_u16,
            ErrorKind::ER_TOO_BIG_SET => 1097_u16,
            ErrorKind::ER_NO_UNIQUE_LOGFILE => 1098_u16,
            ErrorKind::ER_TABLE_NOT_LOCKED_FOR_WRITE => 1099_u16,
            ErrorKind::ER_TABLE_NOT_LOCKED => 1100_u16,
            ErrorKind::ER_BLOB_CANT_HAVE_DEFAULT => 1101_u16,
            ErrorKind::ER_WRONG_DB_NAME => 1102_u16,
            ErrorKind::ER_WRONG_TABLE_NAME => 1103_u16,
            ErrorKind::ER_TOO_BIG_SELECT => 1104_u16,
            ErrorKind::ER_UNKNOWN_ERROR => 1105_u16,
            ErrorKind::ER_UNKNOWN_PROCEDURE => 1106_u16,
            ErrorKind::ER_WRONG_PARAMCOUNT_TO_PROCEDURE => 1107_u16,
            ErrorKind::ER_WRONG_PARAMETERS_TO_PROCEDURE => 1108_u16,
            ErrorKind::ER_UNKNOWN_TABLE => 1109_u16,
            ErrorKind::ER_FIELD_SPECIFIED_TWICE => 1110_u16,
            ErrorKind::ER_INVALID_GROUP_FUNC_USE => 1111_u16,
            ErrorKind::ER_UNSUPPORTED_EXTENSION => 1112_u16,
            ErrorKind::ER_TABLE_MUST_HAVE_COLUMNS => 1113_u16,
            ErrorKind::ER_RECORD_FILE_FULL => 1114_u16,
            ErrorKind::ER_UNKNOWN_CHARACTER_SET => 1115_u16,
            ErrorKind::ER_TOO_MANY_TABLES => 1116_u16,
            ErrorKind::ER_TOO_MANY_FIELDS => 1117_u16,
            ErrorKind::ER_TOO_BIG_ROWSIZE => 1118_u16,
            ErrorKind::ER_STACK_OVERRUN => 1119_u16,
            ErrorKind::ER_WRONG_OUTER_JOIN => 1120_u16,
            ErrorKind::ER_NULL_COLUMN_IN_INDEX => 1121_u16,
            ErrorKind::ER_CANT_FIND_UDF => 1122_u16,
            ErrorKind::ER_CANT_INITIALIZE_UDF => 1123_u16,
            ErrorKind::ER_UDF_NO_PATHS => 1124_u16,
            ErrorKind::ER_UDF_EXISTS => 1125_u16,
            ErrorKind::ER_CANT_OPEN_LIBRARY => 1126_u16,
            ErrorKind::ER_CANT_FIND_DL_ENTRY => 1127_u16,
            ErrorKind::ER_FUNCTION_NOT_DEFINED => 1128_u16,
            ErrorKind::ER_HOST_IS_BLOCKED => 1129_u16,
            ErrorKind::ER_HOST_NOT_PRIVILEGED => 1130_u16,
            ErrorKind::ER_PASSWORD_ANONYMOUS_USER => 1131_u16,
            ErrorKind::ER_PASSWORD_NOT_ALLOWED => 1132_u16,
            ErrorKind::ER_PASSWORD_NO_MATCH => 1133_u16,
            ErrorKind::ER_UPDATE_INF => 1134_u16,
            ErrorKind::ER_CANT_CREATE_THREAD => 1135_u16,
            ErrorKind::ER_WRONG_VALUE_COUNT_ON_ROW => 1136_u16,
            ErrorKind::ER_CANT_REOPEN_TABLE => 1137_u16,
            ErrorKind::ER_INVALID_USE_OF_NULL => 1138_u16,
            ErrorKind::ER_REGEXP_ERROR => 1139_u16,
            ErrorKind::ER_MIX_OF_GROUP_FUNC_AND_FIELDS => 1140_u16,
            ErrorKind::ER_NONEXISTING_GRANT => 1141_u16,
            ErrorKind::ER_TABLEACCESS_DENIED_ERROR => 1142_u16,
            ErrorKind::ER_COLUMNACCESS_DENIED_ERROR => 1143_u16,
            ErrorKind::ER_ILLEGAL_GRANT_FOR_TABLE => 1144_u16,
            ErrorKind::ER_GRANT_WRONG_HOST_OR_USER => 1145_u16,
            ErrorKind::ER_NO_SUCH_TABLE => 1146_u16,
            ErrorKind::ER_NONEXISTING_TABLE_GRANT => 1147_u16,
            ErrorKind::ER_NOT_ALLOWED_COMMAND => 1148_u16,
            ErrorKind::ER_SYNTAX_ERROR => 1149_u16,
            ErrorKind::ER_DELAYED_CANT_CHANGE_LOCK => 1150_u16,
            ErrorKind::ER_TOO_MANY_DELAYED_THREADS => 1151_u16,
            ErrorKind::ER_ABORTING_CONNECTION => 1152_u16,
            ErrorKind::ER_NET_PACKET_TOO_LARGE => 1153_u16,
            ErrorKind::ER_NET_READ_ERROR_FROM_PIPE => 1154_u16,
            ErrorKind::ER_NET_FCNTL_ERROR => 1155_u16,
            ErrorKind::ER_NET_PACKETS_OUT_OF_ORDER => 1156_u16,
            ErrorKind::ER_NET_UNCOMPRESS_ERROR => 1157_u16,
            ErrorKind::ER_NET_READ_ERROR => 1158_u16,
            ErrorKind::ER_NET_READ_INTERRUPTED => 1159_u16,
            ErrorKind::ER_NET_ERROR_ON_WRITE => 1160_u16,
            ErrorKind::ER_NET_WRITE_INTERRUPTED => 1161_u16,
            ErrorKind::ER_TOO_LONG_STRING => 1162_u16,
            ErrorKind::ER_TABLE_CANT_HANDLE_BLOB => 1163_u16,
            ErrorKind::ER_TABLE_CANT_HANDLE_AUTO_INCREMENT => 1164_u16,
            ErrorKind::ER_DELAYED_INSERT_TABLE_LOCKED => 1165_u16,
            ErrorKind::ER_WRONG_COLUMN_NAME => 1166_u16,
            ErrorKind::ER_WRONG_KEY_COLUMN => 1167_u16,
            ErrorKind::ER_WRONG_MRG_TABLE => 1168_u16,
            ErrorKind::ER_DUP_UNIQUE => 1169_u16,
            ErrorKind::ER_BLOB_KEY_WITHOUT_LENGTH => 1170_u16,
            ErrorKind::ER_PRIMARY_CANT_HAVE_NULL => 1171_u16,
            ErrorKind::ER_TOO_MANY_ROWS => 1172_u16,
            ErrorKind::ER_REQUIRES_PRIMARY_KEY => 1173_u16,
            ErrorKind::ER_NO_RAID_COMPILED => 1174_u16,
            ErrorKind::ER_UPDATE_WITHOUT_KEY_IN_SAFE_MODE => 1175_u16,
            ErrorKind::ER_KEY_DOES_NOT_EXITS => 1176_u16,
            ErrorKind::ER_CHECK_NO_SUCH_TABLE => 1177_u16,
            ErrorKind::ER_CHECK_NOT_IMPLEMENTED => 1178_u16,
            ErrorKind::ER_CANT_DO_THIS_DURING_AN_TRANSACTION => 1179_u16,
            ErrorKind::ER_ERROR_DURING_COMMIT => 1180_u16,
            ErrorKind::ER_ERROR_DURING_ROLLBACK => 1181_u16,
            ErrorKind::ER_ERROR_DURING_FLUSH_LOGS => 1182_u16,
            ErrorKind::ER_ERROR_DURING_CHECKPOINT => 1183_u16,
            ErrorKind::ER_NEW_ABORTING_CONNECTION => 1184_u16,
            ErrorKind::ER_DUMP_NOT_IMPLEMENTED => 1185_u16,
            ErrorKind::ER_FLUSH_MASTER_BINLOG_CLOSED => 1186_u16,
            ErrorKind::ER_INDEX_REBUILD => 1187_u16,
            ErrorKind::ER_MASTER => 1188_u16,
            ErrorKind::ER_MASTER_NET_READ => 1189_u16,
            ErrorKind::ER_MASTER_NET_WRITE => 1190_u16,
            ErrorKind::ER_FT_MATCHING_KEY_NOT_FOUND => 1191_u16,
            ErrorKind::ER_LOCK_OR_ACTIVE_TRANSACTION => 1192_u16,
            ErrorKind::ER_UNKNOWN_SYSTEM_VARIABLE => 1193_u16,
            ErrorKind::ER_CRASHED_ON_USAGE => 1194_u16,
            ErrorKind::ER_CRASHED_ON_REPAIR => 1195_u16,
            ErrorKind::ER_WARNING_NOT_COMPLETE_ROLLBACK => 1196_u16,
            ErrorKind::ER_TRANS_CACHE_FULL => 1197_u16,
            ErrorKind::ER_SLAVE_MUST_STOP => 1198_u16,
            ErrorKind::ER_SLAVE_NOT_RUNNING => 1199_u16,
            ErrorKind::ER_BAD_SLAVE => 1200_u16,
            ErrorKind::ER_MASTER_INF => 1201_u16,
            ErrorKind::ER_SLAVE_THREAD => 1202_u16,
            ErrorKind::ER_TOO_MANY_USER_CONNECTIONS => 1203_u16,
            ErrorKind::ER_SET_CONSTANTS_ONLY => 1204_u16,
            ErrorKind::ER_LOCK_WAIT_TIMEOUT => 1205_u16,
            ErrorKind::ER_LOCK_TABLE_FULL => 1206_u16,
            ErrorKind::ER_READ_ONLY_TRANSACTION => 1207_u16,
            ErrorKind::ER_DROP_DB_WITH_READ_LOCK => 1208_u16,
            ErrorKind::ER_CREATE_DB_WITH_READ_LOCK => 1209_u16,
            ErrorKind::ER_WRONG_ARGUMENTS => 1210_u16,
            ErrorKind::ER_NO_PERMISSION_TO_CREATE_USER => 1211_u16,
            ErrorKind::ER_UNION_TABLES_IN_DIFFERENT_DIR => 1212_u16,
            ErrorKind::ER_LOCK_DEADLOCK => 1213_u16,
            ErrorKind::ER_TABLE_CANT_HANDLE_FT => 1214_u16,
            ErrorKind::ER_CANNOT_ADD_FOREIGN => 1215_u16,
            ErrorKind::ER_NO_REFERENCED_ROW => 1216_u16,
            ErrorKind::ER_ROW_IS_REFERENCED => 1217_u16,
            ErrorKind::ER_CONNECT_TO_MASTER => 1218_u16,
            ErrorKind::ER_QUERY_ON_MASTER => 1219_u16,
            ErrorKind::ER_ERROR_WHEN_EXECUTING_COMMAND => 1220_u16,
            ErrorKind::ER_WRONG_USAGE => 1221_u16,
            ErrorKind::ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT => 1222_u16,
            ErrorKind::ER_CANT_UPDATE_WITH_READLOCK => 1223_u16,
            ErrorKind::ER_MIXING_NOT_ALLOWED => 1224_u16,
            ErrorKind::ER_DUP_ARGUMENT => 1225_u16,
            ErrorKind::ER_USER_LIMIT_REACHED => 1226_u16,
            ErrorKind::ER_SPECIFIC_ACCESS_DENIED_ERROR => 1227_u16,
            ErrorKind::ER_LOCAL_VARIABLE => 1228_u16,
            ErrorKind::ER_GLOBAL_VARIABLE => 1229_u16,
            ErrorKind::ER_NO_DEFAULT => 1230_u16,
            ErrorKind::ER_WRONG_VALUE_FOR_VAR => 1231_u16,
            ErrorKind::ER_WRONG_TYPE_FOR_VAR => 1232_u16,
            ErrorKind::ER_VAR_CANT_BE_READ => 1233_u16,
            ErrorKind::ER_CANT_USE_OPTION_HERE => 1234_u16,
            ErrorKind::ER_NOT_SUPPORTED_YET => 1235_u16,
            ErrorKind::ER_MASTER_FATAL_ERROR_READING_BINLOG => 1236_u16,
            ErrorKind::ER_SLAVE_IGNORED_TABLE => 1237_u16,
            ErrorKind::ER_INCORRECT_GLOBAL_LOCAL_VAR => 1238_u16,
            ErrorKind::ER_WRONG_FK_DEF => 1239_u16,
            ErrorKind::ER_KEY_REF_DO_NOT_MATCH_TABLE_REF => 1240_u16,
            ErrorKind::ER_OPERAND_COLUMNS => 1241_u16,
            ErrorKind::ER_SUBQUERY_NO_1_ROW => 1242_u16,
            ErrorKind::ER_UNKNOWN_STMT_HANDLER => 1243_u16,
            ErrorKind::ER_CORRUPT_HELP_DB => 1244_u16,
            ErrorKind::ER_CYCLIC_REFERENCE => 1245_u16,
            ErrorKind::ER_AUTO_CONVERT => 1246_u16,
            ErrorKind::ER_ILLEGAL_REFERENCE => 1247_u16,
            ErrorKind::ER_DERIVED_MUST_HAVE_ALIAS => 1248_u16,
            ErrorKind::ER_SELECT_REDUCED => 1249_u16,
            ErrorKind::ER_TABLENAME_NOT_ALLOWED_HERE => 1250_u16,
            ErrorKind::ER_NOT_SUPPORTED_AUTH_MODE => 1251_u16,
            ErrorKind::ER_SPATIAL_CANT_HAVE_NULL => 1252_u16,
            ErrorKind::ER_COLLATION_CHARSET_MISMATCH => 1253_u16,
            ErrorKind::ER_SLAVE_WAS_RUNNING => 1254_u16,
            ErrorKind::ER_SLAVE_WAS_NOT_RUNNING => 1255_u16,
            ErrorKind::ER_TOO_BIG_FOR_UNCOMPRESS => 1256_u16,
            ErrorKind::ER_ZLIB_Z_MEM_ERROR => 1257_u16,
            ErrorKind::ER_ZLIB_Z_BUF_ERROR => 1258_u16,
            ErrorKind::ER_ZLIB_Z_DATA_ERROR => 1259_u16,
            ErrorKind::ER_CUT_VALUE_GROUP_CONCAT => 1260_u16,
            ErrorKind::ER_WARN_TOO_FEW_RECORDS => 1261_u16,
            ErrorKind::ER_WARN_TOO_MANY_RECORDS => 1262_u16,
            ErrorKind::ER_WARN_NULL_TO_NOTNULL => 1263_u16,
            ErrorKind::ER_WARN_DATA_OUT_OF_RANGE => 1264_u16,
            ErrorKind::WARN_DATA_TRUNCATED => 1265_u16,
            ErrorKind::ER_WARN_USING_OTHER_HANDLER => 1266_u16,
            ErrorKind::ER_CANT_AGGREGATE_2COLLATIONS => 1267_u16,
            ErrorKind::ER_DROP_USER => 1268_u16,
            ErrorKind::ER_REVOKE_GRANTS => 1269_u16,
            ErrorKind::ER_CANT_AGGREGATE_3COLLATIONS => 1270_u16,
            ErrorKind::ER_CANT_AGGREGATE_NCOLLATIONS => 1271_u16,
            ErrorKind::ER_VARIABLE_IS_NOT_STRUCT => 1272_u16,
            ErrorKind::ER_UNKNOWN_COLLATION => 1273_u16,
            ErrorKind::ER_SLAVE_IGNORED_SSL_PARAMS => 1274_u16,
            ErrorKind::ER_SERVER_IS_IN_SECURE_AUTH_MODE => 1275_u16,
            ErrorKind::ER_WARN_FIELD_RESOLVED => 1276_u16,
            ErrorKind::ER_BAD_SLAVE_UNTIL_COND => 1277_u16,
            ErrorKind::ER_MISSING_SKIP_SLAVE => 1278_u16,
            ErrorKind::ER_UNTIL_COND_IGNORED => 1279_u16,
            ErrorKind::ER_WRONG_NAME_FOR_INDEX => 1280_u16,
            ErrorKind::ER_WRONG_NAME_FOR_CATALOG => 1281_u16,
            ErrorKind::ER_WARN_QC_RESIZE => 1282_u16,
            ErrorKind::ER_BAD_FT_COLUMN => 1283_u16,
            ErrorKind::ER_UNKNOWN_KEY_CACHE => 1284_u16,
            ErrorKind::ER_WARN_HOSTNAME_WONT_WORK => 1285_u16,
            ErrorKind::ER_UNKNOWN_STORAGE_ENGINE => 1286_u16,
            ErrorKind::ER_WARN_DEPRECATED_SYNTAX => 1287_u16,
            ErrorKind::ER_NON_UPDATABLE_TABLE => 1288_u16,
            ErrorKind::ER_FEATURE_DISABLED => 1289_u16,
            ErrorKind::ER_OPTION_PREVENTS_STATEMENT => 1290_u16,
            ErrorKind::ER_DUPLICATED_VALUE_IN_TYPE => 1291_u16,
            ErrorKind::ER_TRUNCATED_WRONG_VALUE => 1292_u16,
            ErrorKind::ER_TOO_MUCH_AUTO_TIMESTAMP_COLS => 1293_u16,
            ErrorKind::ER_INVALID_ON_UPDATE => 1294_u16,
            ErrorKind::ER_UNSUPPORTED_PS => 1295_u16,
            ErrorKind::ER_GET_ERRMSG => 1296_u16,
            ErrorKind::ER_GET_TEMPORARY_ERRMSG => 1297_u16,
            ErrorKind::ER_UNKNOWN_TIME_ZONE => 1298_u16,
            ErrorKind::ER_WARN_INVALID_TIMESTAMP => 1299_u16,
            ErrorKind::ER_INVALID_CHARACTER_STRING => 1300_u16,
            ErrorKind::ER_WARN_ALLOWED_PACKET_OVERFLOWED => 1301_u16,
            ErrorKind::ER_CONFLICTING_DECLARATIONS => 1302_u16,
            ErrorKind::ER_SP_NO_RECURSIVE_CREATE => 1303_u16,
            ErrorKind::ER_SP_ALREADY_EXISTS => 1304_u16,
            ErrorKind::ER_SP_DOES_NOT_EXIST => 1305_u16,
            ErrorKind::ER_SP_DROP_FAILED => 1306_u16,
            ErrorKind::ER_SP_STORE_FAILED => 1307_u16,
            ErrorKind::ER_SP_LILABEL_MISMATCH => 1308_u16,
            ErrorKind::ER_SP_LABEL_REDEFINE => 1309_u16,
            ErrorKind::ER_SP_LABEL_MISMATCH => 1310_u16,
            ErrorKind::ER_SP_UNINIT_VAR => 1311_u16,
            ErrorKind::ER_SP_BADSELECT => 1312_u16,
            ErrorKind::ER_SP_BADRETURN => 1313_u16,
            ErrorKind::ER_SP_BADSTATEMENT => 1314_u16,
            ErrorKind::ER_UPDATE_LOG_DEPRECATED_IGNORED => 1315_u16,
            ErrorKind::ER_UPDATE_LOG_DEPRECATED_TRANSLATED => 1316_u16,
            ErrorKind::ER_QUERY_INTERRUPTED => 1317_u16,
            ErrorKind::ER_SP_WRONG_NO_OF_ARGS => 1318_u16,
            ErrorKind::ER_SP_COND_MISMATCH => 1319_u16,
            ErrorKind::ER_SP_NORETURN => 1320_u16,
            ErrorKind::ER_SP_NORETURNEND => 1321_u16,
            ErrorKind::ER_SP_BAD_CURSOR_QUERY => 1322_u16,
            ErrorKind::ER_SP_BAD_CURSOR_SELECT => 1323_u16,
            ErrorKind::ER_SP_CURSOR_MISMATCH => 1324_u16,
            ErrorKind::ER_SP_CURSOR_ALREADY_OPEN => 1325_u16,
            ErrorKind::ER_SP_CURSOR_NOT_OPEN => 1326_u16,
            ErrorKind::ER_SP_UNDECLARED_VAR => 1327_u16,
            ErrorKind::ER_SP_WRONG_NO_OF_FETCH_ARGS => 1328_u16,
            ErrorKind::ER_SP_FETCH_NO_DATA => 1329_u16,
            ErrorKind::ER_SP_DUP_PARAM => 1330_u16,
            ErrorKind::ER_SP_DUP_VAR => 1331_u16,
            ErrorKind::ER_SP_DUP_COND => 1332_u16,
            ErrorKind::ER_SP_DUP_CURS => 1333_u16,
            ErrorKind::ER_SP_CANT_ALTER => 1334_u16,
            ErrorKind::ER_SP_SUBSELECT_NYI => 1335_u16,
            ErrorKind::ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG => 1336_u16,
            ErrorKind::ER_SP_VARCOND_AFTER_CURSHNDLR => 1337_u16,
            ErrorKind::ER_SP_CURSOR_AFTER_HANDLER => 1338_u16,
            ErrorKind::ER_SP_CASE_NOT_FOUND => 1339_u16,
            ErrorKind::ER_FPARSER_TOO_BIG_FILE => 1340_u16,
            ErrorKind::ER_FPARSER_BAD_HEADER => 1341_u16,
            ErrorKind::ER_FPARSER_EOF_IN_COMMENT => 1342_u16,
            ErrorKind::ER_FPARSER_ERROR_IN_PARAMETER => 1343_u16,
            ErrorKind::ER_FPARSER_EOF_IN_UNKNOWN_PARAMETER => 1344_u16,
            ErrorKind::ER_VIEW_NO_EXPLAIN => 1345_u16,
            ErrorKind::ER_FRM_UNKNOWN_TYPE => 1346_u16,
            ErrorKind::ER_WRONG_OBJECT => 1347_u16,
            ErrorKind::ER_NONUPDATEABLE_COLUMN => 1348_u16,
            ErrorKind::ER_VIEW_SELECT_DERIVED => 1349_u16,
            ErrorKind::ER_VIEW_SELECT_CLAUSE => 1350_u16,
            ErrorKind::ER_VIEW_SELECT_VARIABLE => 1351_u16,
            ErrorKind::ER_VIEW_SELECT_TMPTABLE => 1352_u16,
            ErrorKind::ER_VIEW_WRONG_LIST => 1353_u16,
            ErrorKind::ER_WARN_VIEW_MERGE => 1354_u16,
            ErrorKind::ER_WARN_VIEW_WITHOUT_KEY => 1355_u16,
            ErrorKind::ER_VIEW_INVALID => 1356_u16,
            ErrorKind::ER_SP_NO_DROP_SP => 1357_u16,
            ErrorKind::ER_SP_GOTO_IN_HNDLR => 1358_u16,
            ErrorKind::ER_TRG_ALREADY_EXISTS => 1359_u16,
            ErrorKind::ER_TRG_DOES_NOT_EXIST => 1360_u16,
            ErrorKind::ER_TRG_ON_VIEW_OR_TEMP_TABLE => 1361_u16,
            ErrorKind::ER_TRG_CANT_CHANGE_ROW => 1362_u16,
            ErrorKind::ER_TRG_NO_SUCH_ROW_IN_TRG => 1363_u16,
            ErrorKind::ER_NO_DEFAULT_FOR_FIELD => 1364_u16,
            ErrorKind::ER_DIVISION_BY_ZER => 1365_u16,
            ErrorKind::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD => 1366_u16,
            ErrorKind::ER_ILLEGAL_VALUE_FOR_TYPE => 1367_u16,
            ErrorKind::ER_VIEW_NONUPD_CHECK => 1368_u16,
            ErrorKind::ER_VIEW_CHECK_FAILED => 1369_u16,
            ErrorKind::ER_PROCACCESS_DENIED_ERROR => 1370_u16,
            ErrorKind::ER_RELAY_LOG_FAIL => 1371_u16,
            ErrorKind::ER_PASSWD_LENGTH => 1372_u16,
            ErrorKind::ER_UNKNOWN_TARGET_BINLOG => 1373_u16,
            ErrorKind::ER_IO_ERR_LOG_INDEX_READ => 1374_u16,
            ErrorKind::ER_BINLOG_PURGE_PROHIBITED => 1375_u16,
            ErrorKind::ER_FSEEK_FAIL => 1376_u16,
            ErrorKind::ER_BINLOG_PURGE_FATAL_ERR => 1377_u16,
            ErrorKind::ER_LOG_IN_USE => 1378_u16,
            ErrorKind::ER_LOG_PURGE_UNKNOWN_ERR => 1379_u16,
            ErrorKind::ER_RELAY_LOG_INIT => 1380_u16,
            ErrorKind::ER_NO_BINARY_LOGGING => 1381_u16,
            ErrorKind::ER_RESERVED_SYNTAX => 1382_u16,
            ErrorKind::ER_WSAS_FAILED => 1383_u16,
            ErrorKind::ER_DIFF_GROUPS_PROC => 1384_u16,
            ErrorKind::ER_NO_GROUP_FOR_PROC => 1385_u16,
            ErrorKind::ER_ORDER_WITH_PROC => 1386_u16,
            ErrorKind::ER_LOGGING_PROHIBIT_CHANGING_OF => 1387_u16,
            ErrorKind::ER_NO_FILE_MAPPING => 1388_u16,
            ErrorKind::ER_WRONG_MAGIC => 1389_u16,
            ErrorKind::ER_PS_MANY_PARAM => 1390_u16,
            ErrorKind::ER_KEY_PART_0 => 1391_u16,
            ErrorKind::ER_VIEW_CHECKSUM => 1392_u16,
            ErrorKind::ER_VIEW_MULTIUPDATE => 1393_u16,
            ErrorKind::ER_VIEW_NO_INSERT_FIELD_LIST => 1394_u16,
            ErrorKind::ER_VIEW_DELETE_MERGE_VIEW => 1395_u16,
            ErrorKind::ER_CANNOT_USER => 1396_u16,
            ErrorKind::ER_XAER_NOTA => 1397_u16,
            ErrorKind::ER_XAER_INVAL => 1398_u16,
            ErrorKind::ER_XAER_RMFAIL => 1399_u16,
            ErrorKind::ER_XAER_OUTSIDE => 1400_u16,
            ErrorKind::ER_XAER_RMERR => 1401_u16,
            ErrorKind::ER_XA_RBROLLBACK => 1402_u16,
            ErrorKind::ER_NONEXISTING_PROC_GRANT => 1403_u16,
            ErrorKind::ER_PROC_AUTO_GRANT_FAIL => 1404_u16,
            ErrorKind::ER_PROC_AUTO_REVOKE_FAIL => 1405_u16,
            ErrorKind::ER_DATA_TOO_LONG => 1406_u16,
            ErrorKind::ER_SP_BAD_SQLSTATE => 1407_u16,
            ErrorKind::ER_STARTUP => 1408_u16,
            ErrorKind::ER_LOAD_FROM_FIXED_SIZE_ROWS_TO_VAR => 1409_u16,
            ErrorKind::ER_CANT_CREATE_USER_WITH_GRANT => 1410_u16,
            ErrorKind::ER_WRONG_VALUE_FOR_TYPE => 1411_u16,
            ErrorKind::ER_TABLE_DEF_CHANGED => 1412_u16,
            ErrorKind::ER_SP_DUP_HANDLER => 1413_u16,
            ErrorKind::ER_SP_NOT_VAR_ARG => 1414_u16,
            ErrorKind::ER_SP_NO_RETSET => 1415_u16,
            ErrorKind::ER_CANT_CREATE_GEOMETRY_OBJECT => 1416_u16,
            ErrorKind::ER_FAILED_ROUTINE_BREAK_BINLOG => 1417_u16,
            ErrorKind::ER_BINLOG_UNSAFE_ROUTINE => 1418_u16,
            ErrorKind::ER_BINLOG_CREATE_ROUTINE_NEED_SUPER => 1419_u16,
            ErrorKind::ER_EXEC_STMT_WITH_OPEN_CURSOR => 1420_u16,
            ErrorKind::ER_STMT_HAS_NO_OPEN_CURSOR => 1421_u16,
            ErrorKind::ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG => 1422_u16,
            ErrorKind::ER_NO_DEFAULT_FOR_VIEW_FIELD => 1423_u16,
            ErrorKind::ER_SP_NO_RECURSION => 1424_u16,
            ErrorKind::ER_TOO_BIG_SCALE => 1425_u16,
            ErrorKind::ER_TOO_BIG_PRECISION => 1426_u16,
            ErrorKind::ER_M_BIGGER_THAN_D => 1427_u16,
            ErrorKind::ER_WRONG_LOCK_OF_SYSTEM_TABLE => 1428_u16,
            ErrorKind::ER_CONNECT_TO_FOREIGN_DATA_SOURCE => 1429_u16,
            ErrorKind::ER_QUERY_ON_FOREIGN_DATA_SOURCE => 1430_u16,
            ErrorKind::ER_FOREIGN_DATA_SOURCE_DOESNT_EXIST => 1431_u16,
            ErrorKind::ER_FOREIGN_DATA_STRING_INVALID_CANT_CREATE => 1432_u16,
            ErrorKind::ER_FOREIGN_DATA_STRING_INVALID => 1433_u16,
            ErrorKind::ER_CANT_CREATE_FEDERATED_TABLE => 1434_u16,
            ErrorKind::ER_TRG_IN_WRONG_SCHEMA => 1435_u16,
            ErrorKind::ER_STACK_OVERRUN_NEED_MORE => 1436_u16,
            ErrorKind::ER_TOO_LONG_BODY => 1437_u16,
            ErrorKind::ER_WARN_CANT_DROP_DEFAULT_KEYCACHE => 1438_u16,
            ErrorKind::ER_TOO_BIG_DISPLAYWIDTH => 1439_u16,
            ErrorKind::ER_XAER_DUPID => 1440_u16,
            ErrorKind::ER_DATETIME_FUNCTION_OVERFLOW => 1441_u16,
            ErrorKind::ER_CANT_UPDATE_USED_TABLE_IN_SF_OR_TRG => 1442_u16,
            ErrorKind::ER_VIEW_PREVENT_UPDATE => 1443_u16,
            ErrorKind::ER_PS_NO_RECURSION => 1444_u16,
            ErrorKind::ER_SP_CANT_SET_AUTOCOMMIT => 1445_u16,
            ErrorKind::ER_MALFORMED_DEFINER => 1446_u16,
            ErrorKind::ER_VIEW_FRM_NO_USER => 1447_u16,
            ErrorKind::ER_VIEW_OTHER_USER => 1448_u16,
            ErrorKind::ER_NO_SUCH_USER => 1449_u16,
            ErrorKind::ER_FORBID_SCHEMA_CHANGE => 1450_u16,
            ErrorKind::ER_ROW_IS_REFERENCED_2 => 1451_u16,
            ErrorKind::ER_NO_REFERENCED_ROW_2 => 1452_u16,
            ErrorKind::ER_SP_BAD_VAR_SHADOW => 1453_u16,
            ErrorKind::ER_TRG_NO_DEFINER => 1454_u16,
            ErrorKind::ER_OLD_FILE_FORMAT => 1455_u16,
            ErrorKind::ER_SP_RECURSION_LIMIT => 1456_u16,
            ErrorKind::ER_SP_PROC_TABLE_CORRUPT => 1457_u16,
            ErrorKind::ER_SP_WRONG_NAME => 1458_u16,
            ErrorKind::ER_TABLE_NEEDS_UPGRADE => 1459_u16,
            ErrorKind::ER_SP_NO_AGGREGATE => 1460_u16,
            ErrorKind::ER_MAX_PREPARED_STMT_COUNT_REACHED => 1461_u16,
            ErrorKind::ER_VIEW_RECURSIVE => 1462_u16,
            ErrorKind::ER_NON_GROUPING_FIELD_USED => 1463_u16,
            ErrorKind::ER_TABLE_CANT_HANDLE_SPKEYS => 1464_u16,
            ErrorKind::ER_NO_TRIGGERS_ON_SYSTEM_SCHEMA => 1465_u16,
            ErrorKind::ER_REMOVED_SPACES => 1466_u16,
            ErrorKind::ER_AUTOINC_READ_FAILED => 1467_u16,
            ErrorKind::ER_USERNAME => 1468_u16,
            ErrorKind::ER_HOSTNAME => 1469_u16,
            ErrorKind::ER_WRONG_STRING_LENGTH => 1470_u16,
            ErrorKind::ER_NON_INSERTABLE_TABLE => 1471_u16,
            ErrorKind::ER_ADMIN_WRONG_MRG_TABLE => 1472_u16,
            ErrorKind::ER_TOO_HIGH_LEVEL_OF_NESTING_FOR_SELECT => 1473_u16,
            ErrorKind::ER_NAME_BECOMES_EMPTY => 1474_u16,
            ErrorKind::ER_AMBIGUOUS_FIELD_TERM => 1475_u16,
            ErrorKind::ER_FOREIGN_SERVER_EXISTS => 1476_u16,
            ErrorKind::ER_FOREIGN_SERVER_DOESNT_EXIST => 1477_u16,
            ErrorKind::ER_ILLEGAL_HA_CREATE_OPTION => 1478_u16,
            ErrorKind::ER_PARTITION_REQUIRES_VALUES_ERROR => 1479_u16,
            ErrorKind::ER_PARTITION_WRONG_VALUES_ERROR => 1480_u16,
            ErrorKind::ER_PARTITION_MAXVALUE_ERROR => 1481_u16,
            ErrorKind::ER_PARTITION_SUBPARTITION_ERROR => 1482_u16,
            ErrorKind::ER_PARTITION_SUBPART_MIX_ERROR => 1483_u16,
            ErrorKind::ER_PARTITION_WRONG_NO_PART_ERROR => 1484_u16,
            ErrorKind::ER_PARTITION_WRONG_NO_SUBPART_ERROR => 1485_u16,
            ErrorKind::ER_WRONG_EXPR_IN_PARTITION_FUNC_ERROR => 1486_u16,
            ErrorKind::ER_NO_CONST_EXPR_IN_RANGE_OR_LIST_ERROR => 1487_u16,
            ErrorKind::ER_FIELD_NOT_FOUND_PART_ERROR => 1488_u16,
            ErrorKind::ER_LIST_OF_FIELDS_ONLY_IN_HASH_ERROR => 1489_u16,
            ErrorKind::ER_INCONSISTENT_PARTITION_INFO_ERROR => 1490_u16,
            ErrorKind::ER_PARTITION_FUNC_NOT_ALLOWED_ERROR => 1491_u16,
            ErrorKind::ER_PARTITIONS_MUST_BE_DEFINED_ERROR => 1492_u16,
            ErrorKind::ER_RANGE_NOT_INCREASING_ERROR => 1493_u16,
            ErrorKind::ER_INCONSISTENT_TYPE_OF_FUNCTIONS_ERROR => 1494_u16,
            ErrorKind::ER_MULTIPLE_DEF_CONST_IN_LIST_PART_ERROR => 1495_u16,
            ErrorKind::ER_PARTITION_ENTRY_ERROR => 1496_u16,
            ErrorKind::ER_MIX_HANDLER_ERROR => 1497_u16,
            ErrorKind::ER_PARTITION_NOT_DEFINED_ERROR => 1498_u16,
            ErrorKind::ER_TOO_MANY_PARTITIONS_ERROR => 1499_u16,
            ErrorKind::ER_SUBPARTITION_ERROR => 1500_u16,
            ErrorKind::ER_CANT_CREATE_HANDLER_FILE => 1501_u16,
            ErrorKind::ER_BLOB_FIELD_IN_PART_FUNC_ERROR => 1502_u16,
            ErrorKind::ER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF => 1503_u16,
            ErrorKind::ER_NO_PARTS_ERROR => 1504_u16,
            ErrorKind::ER_PARTITION_MGMT_ON_NONPARTITIONED => 1505_u16,
            ErrorKind::ER_FOREIGN_KEY_ON_PARTITIONED => 1506_u16,
            ErrorKind::ER_DROP_PARTITION_NON_EXISTENT => 1507_u16,
            ErrorKind::ER_DROP_LAST_PARTITION => 1508_u16,
            ErrorKind::ER_COALESCE_ONLY_ON_HASH_PARTITION => 1509_u16,
            ErrorKind::ER_REORG_HASH_ONLY_ON_SAME_N => 1510_u16,
            ErrorKind::ER_REORG_NO_PARAM_ERROR => 1511_u16,
            ErrorKind::ER_ONLY_ON_RANGE_LIST_PARTITION => 1512_u16,
            ErrorKind::ER_ADD_PARTITION_SUBPART_ERROR => 1513_u16,
            ErrorKind::ER_ADD_PARTITION_NO_NEW_PARTITION => 1514_u16,
            ErrorKind::ER_COALESCE_PARTITION_NO_PARTITION => 1515_u16,
            ErrorKind::ER_REORG_PARTITION_NOT_EXIST => 1516_u16,
            ErrorKind::ER_SAME_NAME_PARTITION => 1517_u16,
            ErrorKind::ER_NO_BINLOG_ERROR => 1518_u16,
            ErrorKind::ER_CONSECUTIVE_REORG_PARTITIONS => 1519_u16,
            ErrorKind::ER_REORG_OUTSIDE_RANGE => 1520_u16,
            ErrorKind::ER_PARTITION_FUNCTION_FAILURE => 1521_u16,
            ErrorKind::ER_PART_STATE_ERROR => 1522_u16,
            ErrorKind::ER_LIMITED_PART_RANGE => 1523_u16,
            ErrorKind::ER_PLUGIN_IS_NOT_LOADED => 1524_u16,
            ErrorKind::ER_WRONG_VALUE => 1525_u16,
            ErrorKind::ER_NO_PARTITION_FOR_GIVEN_VALUE => 1526_u16,
            ErrorKind::ER_FILEGROUP_OPTION_ONLY_ONCE => 1527_u16,
            ErrorKind::ER_CREATE_FILEGROUP_FAILED => 1528_u16,
            ErrorKind::ER_DROP_FILEGROUP_FAILED => 1529_u16,
            ErrorKind::ER_TABLESPACE_AUTO_EXTEND_ERROR => 1530_u16,
            ErrorKind::ER_WRONG_SIZE_NUMBER => 1531_u16,
            ErrorKind::ER_SIZE_OVERFLOW_ERROR => 1532_u16,
            ErrorKind::ER_ALTER_FILEGROUP_FAILED => 1533_u16,
            ErrorKind::ER_BINLOG_ROW_LOGGING_FAILED => 1534_u16,
            ErrorKind::ER_BINLOG_ROW_WRONG_TABLE_DEF => 1535_u16,
            ErrorKind::ER_BINLOG_ROW_RBR_TO_SBR => 1536_u16,
            ErrorKind::ER_EVENT_ALREADY_EXISTS => 1537_u16,
            ErrorKind::ER_EVENT_STORE_FAILED => 1538_u16,
            ErrorKind::ER_EVENT_DOES_NOT_EXIST => 1539_u16,
            ErrorKind::ER_EVENT_CANT_ALTER => 1540_u16,
            ErrorKind::ER_EVENT_DROP_FAILED => 1541_u16,
            ErrorKind::ER_EVENT_INTERVAL_NOT_POSITIVE_OR_TOO_BIG => 1542_u16,
            ErrorKind::ER_EVENT_ENDS_BEFORE_STARTS => 1543_u16,
            ErrorKind::ER_EVENT_EXEC_TIME_IN_THE_PAST => 1544_u16,
            ErrorKind::ER_EVENT_OPEN_TABLE_FAILED => 1545_u16,
            ErrorKind::ER_EVENT_NEITHER_M_EXPR_NOR_M_AT => 1546_u16,
            ErrorKind::ER_COL_COUNT_DOESNT_MATCH_CORRUPTED => 1547_u16,
            ErrorKind::ER_CANNOT_LOAD_FROM_TABLE => 1548_u16,
            ErrorKind::ER_EVENT_CANNOT_DELETE => 1549_u16,
            ErrorKind::ER_EVENT_COMPILE_ERROR => 1550_u16,
            ErrorKind::ER_EVENT_SAME_NAME => 1551_u16,
            ErrorKind::ER_EVENT_DATA_TOO_LONG => 1552_u16,
            ErrorKind::ER_DROP_INDEX_FK => 1553_u16,
            ErrorKind::ER_WARN_DEPRECATED_SYNTAX_WITH_VER => 1554_u16,
            ErrorKind::ER_CANT_WRITE_LOCK_LOG_TABLE => 1555_u16,
            ErrorKind::ER_CANT_LOCK_LOG_TABLE => 1556_u16,
            ErrorKind::ER_FOREIGN_DUPLICATE_KEY => 1557_u16,
            ErrorKind::ER_COL_COUNT_DOESNT_MATCH_PLEASE_UPDATE => 1558_u16,
            ErrorKind::ER_TEMP_TABLE_PREVENTS_SWITCH_OUT_OF_RBR => 1559_u16,
            ErrorKind::ER_STORED_FUNCTION_PREVENTS_SWITCH_BINLOG_FORMAT => 1560_u16,
            ErrorKind::ER_NDB_CANT_SWITCH_BINLOG_FORMAT => 1561_u16,
            ErrorKind::ER_PARTITION_NO_TEMPORARY => 1562_u16,
            ErrorKind::ER_PARTITION_CONST_DOMAIN_ERROR => 1563_u16,
            ErrorKind::ER_PARTITION_FUNCTION_IS_NOT_ALLOWED => 1564_u16,
            ErrorKind::ER_DDL_LOG_ERROR => 1565_u16,
            ErrorKind::ER_NULL_IN_VALUES_LESS_THAN => 1566_u16,
            ErrorKind::ER_WRONG_PARTITION_NAME => 1567_u16,
            ErrorKind::ER_CANT_CHANGE_TX_ISOLATION => 1568_u16,
            ErrorKind::ER_DUP_ENTRY_AUTOINCREMENT_CASE => 1569_u16,
            ErrorKind::ER_EVENT_MODIFY_QUEUE_ERROR => 1570_u16,
            ErrorKind::ER_EVENT_SET_VAR_ERROR => 1571_u16,
            ErrorKind::ER_PARTITION_MERGE_ERROR => 1572_u16,
            ErrorKind::ER_CANT_ACTIVATE_LOG => 1573_u16,
            ErrorKind::ER_RBR_NOT_AVAILABLE => 1574_u16,
            ErrorKind::ER_BASE64_DECODE_ERROR => 1575_u16,
            ErrorKind::ER_EVENT_RECURSION_FORBIDDEN => 1576_u16,
            ErrorKind::ER_EVENTS_DB_ERROR => 1577_u16,
            ErrorKind::ER_ONLY_INTEGERS_ALLOWED => 1578_u16,
            ErrorKind::ER_UNSUPORTED_LOG_ENGINE => 1579_u16,
            ErrorKind::ER_BAD_LOG_STATEMENT => 1580_u16,
            ErrorKind::ER_CANT_RENAME_LOG_TABLE => 1581_u16,
            ErrorKind::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT => 1582_u16,
            ErrorKind::ER_WRONG_PARAMETERS_TO_NATIVE_FCT => 1583_u16,
            ErrorKind::ER_WRONG_PARAMETERS_TO_STORED_FCT => 1584_u16,
            ErrorKind::ER_NATIVE_FCT_NAME_COLLISION => 1585_u16,
            ErrorKind::ER_DUP_ENTRY_WITH_KEY_NAME => 1586_u16,
            ErrorKind::ER_BINLOG_PURGE_EMFILE => 1587_u16,
            ErrorKind::ER_EVENT_CANNOT_CREATE_IN_THE_PAST => 1588_u16,
            ErrorKind::ER_EVENT_CANNOT_ALTER_IN_THE_PAST => 1589_u16,
            ErrorKind::ER_SLAVE_INCIDENT => 1590_u16,
            ErrorKind::ER_NO_PARTITION_FOR_GIVEN_VALUE_SILENT => 1591_u16,
            ErrorKind::ER_BINLOG_UNSAFE_STATEMENT => 1592_u16,
            ErrorKind::ER_SLAVE_FATAL_ERROR => 1593_u16,
            ErrorKind::ER_SLAVE_RELAY_LOG_READ_FAILURE => 1594_u16,
            ErrorKind::ER_SLAVE_RELAY_LOG_WRITE_FAILURE => 1595_u16,
            ErrorKind::ER_SLAVE_CREATE_EVENT_FAILURE => 1596_u16,
            ErrorKind::ER_SLAVE_MASTER_COM_FAILURE => 1597_u16,
            ErrorKind::ER_BINLOG_LOGGING_IMPOSSIBLE => 1598_u16,
            ErrorKind::ER_VIEW_NO_CREATION_CTX => 1599_u16,
            ErrorKind::ER_VIEW_INVALID_CREATION_CTX => 1600_u16,
            ErrorKind::ER_SR_INVALID_CREATION_CTX => 1601_u16,
            ErrorKind::ER_TRG_CORRUPTED_FILE => 1602_u16,
            ErrorKind::ER_TRG_NO_CREATION_CTX => 1603_u16,
            ErrorKind::ER_TRG_INVALID_CREATION_CTX => 1604_u16,
            ErrorKind::ER_EVENT_INVALID_CREATION_CTX => 1605_u16,
            ErrorKind::ER_TRG_CANT_OPEN_TABLE => 1606_u16,
            ErrorKind::ER_CANT_CREATE_SROUTINE => 1607_u16,
            ErrorKind::ER_UNUSED_11 => 1608_u16,
            ErrorKind::ER_NO_FORMAT_DESCRIPTION_EVENT_BEFORE_BINLOG_STATEMENT => 1609_u16,
            ErrorKind::ER_SLAVE_CORRUPT_EVENT => 1610_u16,
            ErrorKind::ER_LOAD_DATA_INVALID_COLUMN => 1611_u16,
            ErrorKind::ER_LOG_PURGE_NO_FILE => 1612_u16,
            ErrorKind::ER_XA_RBTIMEOUT => 1613_u16,
            ErrorKind::ER_XA_RBDEADLOCK => 1614_u16,
            ErrorKind::ER_NEED_REPREPARE => 1615_u16,
            ErrorKind::ER_DELAYED_NOT_SUPPORTED => 1616_u16,
            ErrorKind::WARN_NO_MASTER_INF => 1617_u16,
            ErrorKind::WARN_OPTION_IGNORED => 1618_u16,
            ErrorKind::WARN_PLUGIN_DELETE_BUILTIN => 1619_u16,
            ErrorKind::WARN_PLUGIN_BUSY => 1620_u16,
            ErrorKind::ER_VARIABLE_IS_READONLY => 1621_u16,
            ErrorKind::ER_WARN_ENGINE_TRANSACTION_ROLLBACK => 1622_u16,
            ErrorKind::ER_SLAVE_HEARTBEAT_FAILURE => 1623_u16,
            ErrorKind::ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE => 1624_u16,
            ErrorKind::ER_NDB_REPLICATION_SCHEMA_ERROR => 1625_u16,
            ErrorKind::ER_CONFLICT_FN_PARSE_ERROR => 1626_u16,
            ErrorKind::ER_EXCEPTIONS_WRITE_ERROR => 1627_u16,
            ErrorKind::ER_TOO_LONG_TABLE_COMMENT => 1628_u16,
            ErrorKind::ER_TOO_LONG_FIELD_COMMENT => 1629_u16,
            ErrorKind::ER_FUNC_INEXISTENT_NAME_COLLISION => 1630_u16,
            ErrorKind::ER_DATABASE_NAME => 1631_u16,
            ErrorKind::ER_TABLE_NAME => 1632_u16,
            ErrorKind::ER_PARTITION_NAME => 1633_u16,
            ErrorKind::ER_SUBPARTITION_NAME => 1634_u16,
            ErrorKind::ER_TEMPORARY_NAME => 1635_u16,
            ErrorKind::ER_RENAMED_NAME => 1636_u16,
            ErrorKind::ER_TOO_MANY_CONCURRENT_TRXS => 1637_u16,
            ErrorKind::WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED => 1638_u16,
            ErrorKind::ER_DEBUG_SYNC_TIMEOUT => 1639_u16,
            ErrorKind::ER_DEBUG_SYNC_HIT_LIMIT => 1640_u16,
            ErrorKind::ER_DUP_SIGNAL_SET => 1641_u16,
            ErrorKind::ER_SIGNAL_WARN => 1642_u16,
            ErrorKind::ER_SIGNAL_NOT_FOUND => 1643_u16,
            ErrorKind::ER_SIGNAL_EXCEPTION => 1644_u16,
            ErrorKind::ER_RESIGNAL_WITHOUT_ACTIVE_HANDLER => 1645_u16,
            ErrorKind::ER_SIGNAL_BAD_CONDITION_TYPE => 1646_u16,
            ErrorKind::WARN_COND_ITEM_TRUNCATED => 1647_u16,
            ErrorKind::ER_COND_ITEM_TOO_LONG => 1648_u16,
            ErrorKind::ER_UNKNOWN_LOCALE => 1649_u16,
            ErrorKind::ER_SLAVE_IGNORE_SERVER_IDS => 1650_u16,
            ErrorKind::ER_QUERY_CACHE_DISABLED => 1651_u16,
            ErrorKind::ER_SAME_NAME_PARTITION_FIELD => 1652_u16,
            ErrorKind::ER_PARTITION_COLUMN_LIST_ERROR => 1653_u16,
            ErrorKind::ER_WRONG_TYPE_COLUMN_VALUE_ERROR => 1654_u16,
            ErrorKind::ER_TOO_MANY_PARTITION_FUNC_FIELDS_ERROR => 1655_u16,
            ErrorKind::ER_MAXVALUE_IN_VALUES_IN => 1656_u16,
            ErrorKind::ER_TOO_MANY_VALUES_ERROR => 1657_u16,
            ErrorKind::ER_ROW_SINGLE_PARTITION_FIELD_ERROR => 1658_u16,
            ErrorKind::ER_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD => 1659_u16,
            ErrorKind::ER_PARTITION_FIELDS_TOO_LONG => 1660_u16,
            ErrorKind::ER_BINLOG_ROW_ENGINE_AND_STMT_ENGINE => 1661_u16,
            ErrorKind::ER_BINLOG_ROW_MODE_AND_STMT_ENGINE => 1662_u16,
            ErrorKind::ER_BINLOG_UNSAFE_AND_STMT_ENGINE => 1663_u16,
            ErrorKind::ER_BINLOG_ROW_INJECTION_AND_STMT_ENGINE => 1664_u16,
            ErrorKind::ER_BINLOG_STMT_MODE_AND_ROW_ENGINE => 1665_u16,
            ErrorKind::ER_BINLOG_ROW_INJECTION_AND_STMT_MODE => 1666_u16,
            ErrorKind::ER_BINLOG_MULTIPLE_ENGINES_AND_SELF_LOGGING_ENGINE => 1667_u16,
            ErrorKind::ER_BINLOG_UNSAFE_LIMIT => 1668_u16,
            ErrorKind::ER_BINLOG_UNSAFE_INSERT_DELAYED => 1669_u16,
            ErrorKind::ER_BINLOG_UNSAFE_SYSTEM_TABLE => 1670_u16,
            ErrorKind::ER_BINLOG_UNSAFE_AUTOINC_COLUMNS => 1671_u16,
            ErrorKind::ER_BINLOG_UNSAFE_UDF => 1672_u16,
            ErrorKind::ER_BINLOG_UNSAFE_SYSTEM_VARIABLE => 1673_u16,
            ErrorKind::ER_BINLOG_UNSAFE_SYSTEM_FUNCTION => 1674_u16,
            ErrorKind::ER_BINLOG_UNSAFE_NONTRANS_AFTER_TRANS => 1675_u16,
            ErrorKind::ER_MESSAGE_AND_STATEMENT => 1676_u16,
            ErrorKind::ER_SLAVE_CONVERSION_FAILED => 1677_u16,
            ErrorKind::ER_SLAVE_CANT_CREATE_CONVERSION => 1678_u16,
            ErrorKind::ER_INSIDE_TRANSACTION_PREVENTS_SWITCH_BINLOG_FORMAT => 1679_u16,
            ErrorKind::ER_PATH_LENGTH => 1680_u16,
            ErrorKind::ER_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT => 1681_u16,
            ErrorKind::ER_WRONG_NATIVE_TABLE_STRUCTURE => 1682_u16,
            ErrorKind::ER_WRONG_PERFSCHEMA_USAGE => 1683_u16,
            ErrorKind::ER_WARN_I_S_SKIPPED_TABLE => 1684_u16,
            ErrorKind::ER_INSIDE_TRANSACTION_PREVENTS_SWITCH_BINLOG_DIRECT => 1685_u16,
            ErrorKind::ER_STORED_FUNCTION_PREVENTS_SWITCH_BINLOG_DIRECT => 1686_u16,
            ErrorKind::ER_SPATIAL_MUST_HAVE_GEOM_COL => 1687_u16,
            ErrorKind::ER_TOO_LONG_INDEX_COMMENT => 1688_u16,
            ErrorKind::ER_LOCK_ABORTED => 1689_u16,
            ErrorKind::ER_DATA_OUT_OF_RANGE => 1690_u16,
            ErrorKind::ER_WRONG_SPVAR_TYPE_IN_LIMIT => 1691_u16,
            ErrorKind::ER_BINLOG_UNSAFE_MULTIPLE_ENGINES_AND_SELF_LOGGING_ENGINE => 1692_u16,
            ErrorKind::ER_BINLOG_UNSAFE_MIXED_STATEMENT => 1693_u16,
            ErrorKind::ER_INSIDE_TRANSACTION_PREVENTS_SWITCH_SQL_LOG_BIN => 1694_u16,
            ErrorKind::ER_STORED_FUNCTION_PREVENTS_SWITCH_SQL_LOG_BIN => 1695_u16,
            ErrorKind::ER_FAILED_READ_FROM_PAR_FILE => 1696_u16,
            ErrorKind::ER_VALUES_IS_NOT_INT_TYPE_ERROR => 1697_u16,
            ErrorKind::ER_ACCESS_DENIED_NO_PASSWORD_ERROR => 1698_u16,
            ErrorKind::ER_SET_PASSWORD_AUTH_PLUGIN => 1699_u16,
            ErrorKind::ER_GRANT_PLUGIN_USER_EXISTS => 1700_u16,
            ErrorKind::ER_TRUNCATE_ILLEGAL_FK => 1701_u16,
            ErrorKind::ER_PLUGIN_IS_PERMANENT => 1702_u16,
            ErrorKind::ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE_MIN => 1703_u16,
            ErrorKind::ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE_MAX => 1704_u16,
            ErrorKind::ER_STMT_CACHE_FULL => 1705_u16,
            ErrorKind::ER_MULTI_UPDATE_KEY_CONFLICT => 1706_u16,
            ErrorKind::ER_TABLE_NEEDS_REBUILD => 1707_u16,
            ErrorKind::WARN_OPTION_BELOW_LIMIT => 1708_u16,
            ErrorKind::ER_INDEX_COLUMN_TOO_LONG => 1709_u16,
            ErrorKind::ER_ERROR_IN_TRIGGER_BODY => 1710_u16,
            ErrorKind::ER_ERROR_IN_UNKNOWN_TRIGGER_BODY => 1711_u16,
            ErrorKind::ER_INDEX_CORRUPT => 1712_u16,
            ErrorKind::ER_UNDO_RECORD_TOO_BIG => 1713_u16,
            ErrorKind::ER_BINLOG_UNSAFE_INSERT_IGNORE_SELECT => 1714_u16,
            ErrorKind::ER_BINLOG_UNSAFE_INSERT_SELECT_UPDATE => 1715_u16,
            ErrorKind::ER_BINLOG_UNSAFE_REPLACE_SELECT => 1716_u16,
            ErrorKind::ER_BINLOG_UNSAFE_CREATE_IGNORE_SELECT => 1717_u16,
            ErrorKind::ER_BINLOG_UNSAFE_CREATE_REPLACE_SELECT => 1718_u16,
            ErrorKind::ER_BINLOG_UNSAFE_UPDATE_IGNORE => 1719_u16,
            ErrorKind::ER_PLUGIN_NO_UNINSTALL => 1720_u16,
            ErrorKind::ER_PLUGIN_NO_INSTALL => 1721_u16,
            ErrorKind::ER_BINLOG_UNSAFE_WRITE_AUTOINC_SELECT => 1722_u16,
            ErrorKind::ER_BINLOG_UNSAFE_CREATE_SELECT_AUTOINC => 1723_u16,
            ErrorKind::ER_BINLOG_UNSAFE_INSERT_TWO_KEYS => 1724_u16,
            ErrorKind::ER_TABLE_IN_FK_CHECK => 1725_u16,
            ErrorKind::ER_UNSUPPORTED_ENGINE => 1726_u16,
            ErrorKind::ER_BINLOG_UNSAFE_AUTOINC_NOT_FIRST => 1727_u16,
            ErrorKind::ER_CANNOT_LOAD_FROM_TABLE_V2 => 1728_u16,
            ErrorKind::ER_MASTER_DELAY_VALUE_OUT_OF_RANGE => 1729_u16,
            ErrorKind::ER_ONLY_FD_AND_RBR_EVENTS_ALLOWED_IN_BINLOG_STATEMENT => 1730_u16,
            ErrorKind::ER_PARTITION_EXCHANGE_DIFFERENT_OPTION => 1731_u16,
            ErrorKind::ER_PARTITION_EXCHANGE_PART_TABLE => 1732_u16,
            ErrorKind::ER_PARTITION_EXCHANGE_TEMP_TABLE => 1733_u16,
            ErrorKind::ER_PARTITION_INSTEAD_OF_SUBPARTITION => 1734_u16,
            ErrorKind::ER_UNKNOWN_PARTITION => 1735_u16,
            ErrorKind::ER_TABLES_DIFFERENT_METADATA => 1736_u16,
            ErrorKind::ER_ROW_DOES_NOT_MATCH_PARTITION => 1737_u16,
            ErrorKind::ER_BINLOG_CACHE_SIZE_GREATER_THAN_MAX => 1738_u16,
            ErrorKind::ER_WARN_INDEX_NOT_APPLICABLE => 1739_u16,
            ErrorKind::ER_PARTITION_EXCHANGE_FOREIGN_KEY => 1740_u16,
            ErrorKind::ER_NO_SUCH_KEY_VALUE => 1741_u16,
            ErrorKind::ER_RPL_INFO_DATA_TOO_LONG => 1742_u16,
            ErrorKind::ER_NETWORK_READ_EVENT_CHECKSUM_FAILURE => 1743_u16,
            ErrorKind::ER_BINLOG_READ_EVENT_CHECKSUM_FAILURE => 1744_u16,
            ErrorKind::ER_BINLOG_STMT_CACHE_SIZE_GREATER_THAN_MAX => 1745_u16,
            ErrorKind::ER_CANT_UPDATE_TABLE_IN_CREATE_TABLE_SELECT => 1746_u16,
            ErrorKind::ER_PARTITION_CLAUSE_ON_NONPARTITIONED => 1747_u16,
            ErrorKind::ER_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET => 1748_u16,
            ErrorKind::ER_NO_SUCH_PARTITION_UNUSED => 1749_u16,
            ErrorKind::ER_CHANGE_RPL_INFO_REPOSITORY_FAILURE => 1750_u16,
            ErrorKind::ER_WARNING_NOT_COMPLETE_ROLLBACK_WITH_CREATED_TEMP_TABLE => 1751_u16,
            ErrorKind::ER_WARNING_NOT_COMPLETE_ROLLBACK_WITH_DROPPED_TEMP_TABLE => 1752_u16,
            ErrorKind::ER_MTS_FEATURE_IS_NOT_SUPPORTED => 1753_u16,
            ErrorKind::ER_MTS_UPDATED_DBS_GREATER_MAX => 1754_u16,
            ErrorKind::ER_MTS_CANT_PARALLEL => 1755_u16,
            ErrorKind::ER_MTS_INCONSISTENT_DATA => 1756_u16,
            ErrorKind::ER_FULLTEXT_NOT_SUPPORTED_WITH_PARTITIONING => 1757_u16,
            ErrorKind::ER_DA_INVALID_CONDITION_NUMBER => 1758_u16,
            ErrorKind::ER_INSECURE_PLAIN_TEXT => 1759_u16,
            ErrorKind::ER_INSECURE_CHANGE_MASTER => 1760_u16,
            ErrorKind::ER_FOREIGN_DUPLICATE_KEY_WITH_CHILD_INFO => 1761_u16,
            ErrorKind::ER_FOREIGN_DUPLICATE_KEY_WITHOUT_CHILD_INFO => 1762_u16,
            ErrorKind::ER_SQLTHREAD_WITH_SECURE_SLAVE => 1763_u16,
            ErrorKind::ER_TABLE_HAS_NO_FT => 1764_u16,
            ErrorKind::ER_VARIABLE_NOT_SETTABLE_IN_SF_OR_TRIGGER => 1765_u16,
            ErrorKind::ER_VARIABLE_NOT_SETTABLE_IN_TRANSACTION => 1766_u16,
            ErrorKind::ER_GTID_NEXT_IS_NOT_IN_GTID_NEXT_LIST => 1767_u16,
            ErrorKind::ER_CANT_CHANGE_GTID_NEXT_IN_TRANSACTION_WHEN_GTID_NEXT_LIST_IS_NULL => {
                1768_u16
            }
            ErrorKind::ER_SET_STATEMENT_CANNOT_INVOKE_FUNCTION => 1769_u16,
            ErrorKind::ER_GTID_NEXT_CANT_BE_AUTOMATIC_IF_GTID_NEXT_LIST_IS_NON_NULL => 1770_u16,
            ErrorKind::ER_SKIPPING_LOGGED_TRANSACTION => 1771_u16,
            ErrorKind::ER_MALFORMED_GTID_SET_SPECIFICATION => 1772_u16,
            ErrorKind::ER_MALFORMED_GTID_SET_ENCODING => 1773_u16,
            ErrorKind::ER_MALFORMED_GTID_SPECIFICATION => 1774_u16,
            ErrorKind::ER_GNO_EXHAUSTED => 1775_u16,
            ErrorKind::ER_BAD_SLAVE_AUTO_POSITION => 1776_u16,
            ErrorKind::ER_AUTO_POSITION_REQUIRES_GTID_MODE_ON => 1777_u16,
            ErrorKind::ER_CANT_DO_IMPLICIT_COMMIT_IN_TRX_WHEN_GTID_NEXT_IS_SET => 1778_u16,
            ErrorKind::ER_GTID_MODE_2_OR_3_REQUIRES_DISABLE_GTID_UNSAFE_STATEMENTS_ON => 1779_u16,
            ErrorKind::ER_GTID_MODE_REQUIRES_BINLOG => 1780_u16,
            ErrorKind::ER_CANT_SET_GTID_NEXT_TO_GTID_WHEN_GTID_MODE_IS_OFF => 1781_u16,
            ErrorKind::ER_CANT_SET_GTID_NEXT_TO_ANONYMOUS_WHEN_GTID_MODE_IS_ON => 1782_u16,
            ErrorKind::ER_CANT_SET_GTID_NEXT_LIST_TO_NON_NULL_WHEN_GTID_MODE_IS_OFF => 1783_u16,
            ErrorKind::ER_FOUND_GTID_EVENT_WHEN_GTID_MODE_IS_OFF => 1784_u16,
            ErrorKind::ER_GTID_UNSAFE_NON_TRANSACTIONAL_TABLE => 1785_u16,
            ErrorKind::ER_GTID_UNSAFE_CREATE_SELECT => 1786_u16,
            ErrorKind::ER_GTID_UNSAFE_CREATE_DROP_TEMPORARY_TABLE_IN_TRANSACTION => 1787_u16,
            ErrorKind::ER_GTID_MODE_CAN_ONLY_CHANGE_ONE_STEP_AT_A_TIME => 1788_u16,
            ErrorKind::ER_MASTER_HAS_PURGED_REQUIRED_GTIDS => 1789_u16,
            ErrorKind::ER_CANT_SET_GTID_NEXT_WHEN_OWNING_GTID => 1790_u16,
            ErrorKind::ER_UNKNOWN_EXPLAIN_FORMAT => 1791_u16,
            ErrorKind::ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION => 1792_u16,
            ErrorKind::ER_TOO_LONG_TABLE_PARTITION_COMMENT => 1793_u16,
            ErrorKind::ER_SLAVE_CONFIGURATION => 1794_u16,
            ErrorKind::ER_INNODB_FT_LIMIT => 1795_u16,
            ErrorKind::ER_INNODB_NO_FT_TEMP_TABLE => 1796_u16,
            ErrorKind::ER_INNODB_FT_WRONG_DOCID_COLUMN => 1797_u16,
            ErrorKind::ER_INNODB_FT_WRONG_DOCID_INDEX => 1798_u16,
            ErrorKind::ER_INNODB_ONLINE_LOG_TOO_BIG => 1799_u16,
            ErrorKind::ER_UNKNOWN_ALTER_ALGORITHM => 1800_u16,
            ErrorKind::ER_UNKNOWN_ALTER_LOCK => 1801_u16,
            ErrorKind::ER_MTS_CHANGE_MASTER_CANT_RUN_WITH_GAPS => 1802_u16,
            ErrorKind::ER_MTS_RECOVERY_FAILURE => 1803_u16,
            ErrorKind::ER_MTS_RESET_WORKERS => 1804_u16,
            ErrorKind::ER_COL_COUNT_DOESNT_MATCH_CORRUPTED_V2 => 1805_u16,
            ErrorKind::ER_SLAVE_SILENT_RETRY_TRANSACTION => 1806_u16,
            ErrorKind::ER_DISCARD_FK_CHECKS_RUNNING => 1807_u16,
            ErrorKind::ER_TABLE_SCHEMA_MISMATCH => 1808_u16,
            ErrorKind::ER_TABLE_IN_SYSTEM_TABLESPACE => 1809_u16,
            ErrorKind::ER_IO_READ_ERROR => 1810_u16,
            ErrorKind::ER_IO_WRITE_ERROR => 1811_u16,
            ErrorKind::ER_TABLESPACE_MISSING => 1812_u16,
            ErrorKind::ER_TABLESPACE_EXISTS => 1813_u16,
            ErrorKind::ER_TABLESPACE_DISCARDED => 1814_u16,
            ErrorKind::ER_INTERNAL_ERROR => 1815_u16,
            ErrorKind::ER_INNODB_IMPORT_ERROR => 1816_u16,
            ErrorKind::ER_INNODB_INDEX_CORRUPT => 1817_u16,
            ErrorKind::ER_INVALID_YEAR_COLUMN_LENGTH => 1818_u16,
            ErrorKind::ER_NOT_VALID_PASSWORD => 1819_u16,
            ErrorKind::ER_MUST_CHANGE_PASSWORD => 1820_u16,
            ErrorKind::ER_FK_NO_INDEX_CHILD => 1821_u16,
            ErrorKind::ER_FK_NO_INDEX_PARENT => 1822_u16,
            ErrorKind::ER_FK_FAIL_ADD_SYSTEM => 1823_u16,
            ErrorKind::ER_FK_CANNOT_OPEN_PARENT => 1824_u16,
            ErrorKind::ER_FK_INCORRECT_OPTION => 1825_u16,
            ErrorKind::ER_FK_DUP_NAME => 1826_u16,
            ErrorKind::ER_PASSWORD_FORMAT => 1827_u16,
            ErrorKind::ER_FK_COLUMN_CANNOT_DROP => 1828_u16,
            ErrorKind::ER_FK_COLUMN_CANNOT_DROP_CHILD => 1829_u16,
            ErrorKind::ER_FK_COLUMN_NOT_NULL => 1830_u16,
            ErrorKind::ER_DUP_INDEX => 1831_u16,
            ErrorKind::ER_FK_COLUMN_CANNOT_CHANGE => 1832_u16,
            ErrorKind::ER_FK_COLUMN_CANNOT_CHANGE_CHILD => 1833_u16,
            ErrorKind::ER_FK_CANNOT_DELETE_PARENT => 1834_u16,
            ErrorKind::ER_MALFORMED_PACKET => 1835_u16,
            ErrorKind::ER_READ_ONLY_MODE => 1836_u16,
            ErrorKind::ER_GTID_NEXT_TYPE_UNDEFINED_GROUP => 1837_u16,
            ErrorKind::ER_VARIABLE_NOT_SETTABLE_IN_SP => 1838_u16,
            ErrorKind::ER_CANT_SET_GTID_PURGED_WHEN_GTID_MODE_IS_OFF => 1839_u16,
            ErrorKind::ER_CANT_SET_GTID_PURGED_WHEN_GTID_EXECUTED_IS_NOT_EMPTY => 1840_u16,
            ErrorKind::ER_CANT_SET_GTID_PURGED_WHEN_OWNED_GTIDS_IS_NOT_EMPTY => 1841_u16,
            ErrorKind::ER_GTID_PURGED_WAS_CHANGED => 1842_u16,
            ErrorKind::ER_GTID_EXECUTED_WAS_CHANGED => 1843_u16,
            ErrorKind::ER_BINLOG_STMT_MODE_AND_NO_REPL_TABLES => 1844_u16,
            ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED => 1845_u16,
            ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON => 1846_u16,
            ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_COPY => 1847_u16,
            ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_PARTITION => 1848_u16,
            ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_FK_RENAME => 1849_u16,
            ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_COLUMN_TYPE => 1850_u16,
            ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_FK_CHECK => 1851_u16,
            ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_IGNORE => 1852_u16,
            ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_NOPK => 1853_u16,
            ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_AUTOINC => 1854_u16,
            ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_HIDDEN_FTS => 1855_u16,
            ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_CHANGE_FTS => 1856_u16,
            ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_FTS => 1857_u16,
            ErrorKind::ER_SQL_SLAVE_SKIP_COUNTER_NOT_SETTABLE_IN_GTID_MODE => 1858_u16,
            ErrorKind::ER_DUP_UNKNOWN_IN_INDEX => 1859_u16,
            ErrorKind::ER_IDENT_CAUSES_TOO_LONG_PATH => 1860_u16,
            ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_NOT_NULL => 1861_u16,
            ErrorKind::ER_MUST_CHANGE_PASSWORD_LOGIN => 1862_u16,
            ErrorKind::ER_ROW_IN_WRONG_PARTITION => 1863_u16,
            ErrorKind::ER_MTS_EVENT_BIGGER_PENDING_JOBS_SIZE_MAX => 1864_u16,
            ErrorKind::ER_INNODB_NO_FT_USES_PARSER => 1865_u16,
            ErrorKind::ER_BINLOG_LOGICAL_CORRUPTION => 1866_u16,
            ErrorKind::ER_WARN_PURGE_LOG_IN_USE => 1867_u16,
            ErrorKind::ER_WARN_PURGE_LOG_IS_ACTIVE => 1868_u16,
            ErrorKind::ER_AUTO_INCREMENT_CONFLICT => 1869_u16,
            ErrorKind::WARN_ON_BLOCKHOLE_IN_RBR => 1870_u16,
            ErrorKind::ER_SLAVE_MI_INIT_REPOSITORY => 1871_u16,
            ErrorKind::ER_SLAVE_RLI_INIT_REPOSITORY => 1872_u16,
            ErrorKind::ER_ACCESS_DENIED_CHANGE_USER_ERROR => 1873_u16,
            ErrorKind::ER_INNODB_READ_ONLY => 1874_u16,
            ErrorKind::ER_STOP_SLAVE_SQL_THREAD_TIMEOUT => 1875_u16,
            ErrorKind::ER_STOP_SLAVE_IO_THREAD_TIMEOUT => 1876_u16,
            ErrorKind::ER_TABLE_CORRUPT => 1877_u16,
            ErrorKind::ER_TEMP_FILE_WRITE_FAILURE => 1878_u16,
            ErrorKind::ER_INNODB_FT_AUX_NOT_HEX_ID => 1879_u16,
            ErrorKind::ER_OLD_TEMPORALS_UPGRADED => 1880_u16,
            ErrorKind::ER_INNODB_FORCED_RECOVERY => 1881_u16,
            ErrorKind::ER_AES_INVALID_IV => 1882_u16,
            ErrorKind::ER_PLUGIN_CANNOT_BE_UNINSTALLED => 1883_u16,
            ErrorKind::ER_GTID_UNSAFE_BINLOG_SPLITTABLE_STATEMENT_AND_GTID_GROUP => 1884_u16,
            ErrorKind::ER_SLAVE_HAS_MORE_GTIDS_THAN_MASTER => 1885_u16,
        }
    }
}

impl ErrorKind {
    /// SQLSTATE is a code which identifies SQL error conditions. It composed by five characters:
    /// first two characters that indicate a class, and then three that indicate a subclass.
    ///
    /// There are three important standard classes.
    ///
    ///  - `00` means the operation completed successfully.
    ///  - `01` contains warnings (`SQLWARNING`).
    ///  - `02` is the `NOT FOUND` class.
    ///
    /// All other classes are exceptions (`SQLEXCEPTION`). Classes beginning with 0, 1, 2, 3, 4, A,
    /// B, C, D, E, F and G are reserved for standard-defined classes, while other classes are
    /// vendor-specific.
    ///
    /// The subclass, if it is set, indicates a particular condition, or a particular group of
    /// conditions within the class. `000` means 'no subclass'.
    ///
    /// See also https://mariadb.com/kb/en/library/sqlstate/
    pub fn sqlstate(self) -> &'static [u8; 5] {
        match self {
            ErrorKind::ER_BAD_HOST_ERROR
            | ErrorKind::ER_HANDSHAKE_ERROR
            | ErrorKind::ER_UNKNOWN_COM_ERROR
            | ErrorKind::ER_SERVER_SHUTDOWN
            | ErrorKind::ER_FORCING_CLOSE
            | ErrorKind::ER_IPSOCK_ERROR
            | ErrorKind::ER_ABORTING_CONNECTION
            | ErrorKind::ER_NET_PACKET_TOO_LARGE
            | ErrorKind::ER_NET_READ_ERROR_FROM_PIPE
            | ErrorKind::ER_NET_FCNTL_ERROR
            | ErrorKind::ER_NET_PACKETS_OUT_OF_ORDER
            | ErrorKind::ER_NET_UNCOMPRESS_ERROR
            | ErrorKind::ER_NET_READ_ERROR
            | ErrorKind::ER_NET_READ_INTERRUPTED
            | ErrorKind::ER_NET_ERROR_ON_WRITE
            | ErrorKind::ER_NET_WRITE_INTERRUPTED
            | ErrorKind::ER_NEW_ABORTING_CONNECTION
            | ErrorKind::ER_MASTER_NET_READ
            | ErrorKind::ER_MASTER_NET_WRITE
            | ErrorKind::ER_CONNECT_TO_MASTER => b"08S01",
            ErrorKind::ER_NO_DB_ERROR => b"3D000",
            ErrorKind::ER_DA_INVALID_CONDITION_NUMBER => b"35000",
            ErrorKind::ER_TABLE_EXISTS_ERROR => b"42S01",
            ErrorKind::ER_SP_FETCH_NO_DATA | ErrorKind::ER_SIGNAL_NOT_FOUND => b"02000",
            ErrorKind::ER_BAD_TABLE_ERROR
            | ErrorKind::ER_UNKNOWN_TABLE
            | ErrorKind::ER_NO_SUCH_TABLE => b"42S02",
            ErrorKind::ER_TRUNCATED_WRONG_VALUE | ErrorKind::ER_ILLEGAL_VALUE_FOR_TYPE => b"22007",
            ErrorKind::ER_XA_RBTIMEOUT => b"XA106",
            ErrorKind::ER_XAER_DUPID => b"XAE08",
            ErrorKind::ER_XA_RBDEADLOCK => b"XA102",
            ErrorKind::ER_XAER_OUTSIDE => b"XAE09",
            ErrorKind::ER_DATETIME_FUNCTION_OVERFLOW => b"22008",
            ErrorKind::ER_WARN_DATA_OUT_OF_RANGE
            | ErrorKind::ER_CANT_CREATE_GEOMETRY_OBJECT
            | ErrorKind::ER_DATA_OUT_OF_RANGE => b"22003",
            ErrorKind::ER_CANT_DO_THIS_DURING_AN_TRANSACTION
            | ErrorKind::ER_READ_ONLY_TRANSACTION => b"25000",
            ErrorKind::ER_OUTOFMEMORY | ErrorKind::ER_OUT_OF_SORTMEMORY => b"HY001",
            ErrorKind::ER_SP_NO_RECURSIVE_CREATE => b"2F003",
            ErrorKind::ER_DUP_KEY
            | ErrorKind::ER_BAD_NULL_ERROR
            | ErrorKind::ER_NON_UNIQ_ERROR
            | ErrorKind::ER_DUP_ENTRY
            | ErrorKind::ER_DUP_UNIQUE
            | ErrorKind::ER_NO_REFERENCED_ROW
            | ErrorKind::ER_ROW_IS_REFERENCED
            | ErrorKind::ER_ROW_IS_REFERENCED_2
            | ErrorKind::ER_NO_REFERENCED_ROW_2
            | ErrorKind::ER_FOREIGN_DUPLICATE_KEY
            | ErrorKind::ER_DUP_ENTRY_WITH_KEY_NAME
            | ErrorKind::ER_FOREIGN_DUPLICATE_KEY_WITH_CHILD_INFO
            | ErrorKind::ER_FOREIGN_DUPLICATE_KEY_WITHOUT_CHILD_INFO
            | ErrorKind::ER_DUP_UNKNOWN_IN_INDEX => b"23000",
            ErrorKind::ER_DUP_FIELDNAME => b"42S21",
            ErrorKind::ER_SELECT_REDUCED
            | ErrorKind::ER_WARN_TOO_FEW_RECORDS
            | ErrorKind::ER_WARN_TOO_MANY_RECORDS
            | ErrorKind::WARN_DATA_TRUNCATED
            | ErrorKind::ER_SP_UNINIT_VAR
            | ErrorKind::ER_SIGNAL_WARN => b"01000",
            ErrorKind::ER_XAER_RMERR => b"XAE03",
            ErrorKind::ER_XAER_RMFAIL => b"XAE07",
            ErrorKind::ER_NO_SUCH_INDEX => b"42S12",
            ErrorKind::ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION => b"25006",
            ErrorKind::ER_SP_BADSELECT
            | ErrorKind::ER_SP_BADSTATEMENT
            | ErrorKind::ER_SP_SUBSELECT_NYI
            | ErrorKind::ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG
            | ErrorKind::ER_SP_NO_RETSET
            | ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED
            | ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON => b"0A000",
            ErrorKind::ER_WRONG_VALUE_COUNT | ErrorKind::ER_WRONG_VALUE_COUNT_ON_ROW => b"21S01",
            ErrorKind::ER_ACCESS_DENIED_ERROR
            | ErrorKind::ER_ACCESS_DENIED_NO_PASSWORD_ERROR
            | ErrorKind::ER_ACCESS_DENIED_CHANGE_USER_ERROR => b"28000",
            ErrorKind::ER_SP_CURSOR_ALREADY_OPEN | ErrorKind::ER_SP_CURSOR_NOT_OPEN => b"24000",
            ErrorKind::ER_QUERY_INTERRUPTED => b"70100",
            ErrorKind::ER_SP_NORETURNEND => b"2F005",
            ErrorKind::ER_CON_COUNT_ERROR | ErrorKind::ER_NOT_SUPPORTED_AUTH_MODE => b"08004",
            ErrorKind::ER_DBACCESS_DENIED_ERROR
            | ErrorKind::ER_BAD_DB_ERROR
            | ErrorKind::ER_WRONG_FIELD_WITH_GROUP
            | ErrorKind::ER_WRONG_GROUP_FIELD
            | ErrorKind::ER_WRONG_SUM_SELECT
            | ErrorKind::ER_TOO_LONG_IDENT
            | ErrorKind::ER_DUP_KEYNAME
            | ErrorKind::ER_WRONG_FIELD_SPEC
            | ErrorKind::ER_PARSE_ERROR
            | ErrorKind::ER_EMPTY_QUERY
            | ErrorKind::ER_NONUNIQ_TABLE
            | ErrorKind::ER_INVALID_DEFAULT
            | ErrorKind::ER_MULTIPLE_PRI_KEY
            | ErrorKind::ER_TOO_MANY_KEYS
            | ErrorKind::ER_TOO_MANY_KEY_PARTS
            | ErrorKind::ER_TOO_LONG_KEY
            | ErrorKind::ER_KEY_COLUMN_DOES_NOT_EXITS
            | ErrorKind::ER_BLOB_USED_AS_KEY
            | ErrorKind::ER_TOO_BIG_FIELDLENGTH
            | ErrorKind::ER_WRONG_AUTO_KEY
            | ErrorKind::ER_WRONG_FIELD_TERMINATORS
            | ErrorKind::ER_BLOBS_AND_NO_TERMINATED
            | ErrorKind::ER_CANT_REMOVE_ALL_FIELDS
            | ErrorKind::ER_CANT_DROP_FIELD_OR_KEY
            | ErrorKind::ER_BLOB_CANT_HAVE_DEFAULT
            | ErrorKind::ER_WRONG_DB_NAME
            | ErrorKind::ER_WRONG_TABLE_NAME
            | ErrorKind::ER_TOO_BIG_SELECT
            | ErrorKind::ER_UNKNOWN_PROCEDURE
            | ErrorKind::ER_WRONG_PARAMCOUNT_TO_PROCEDURE
            | ErrorKind::ER_FIELD_SPECIFIED_TWICE
            | ErrorKind::ER_UNSUPPORTED_EXTENSION
            | ErrorKind::ER_TABLE_MUST_HAVE_COLUMNS
            | ErrorKind::ER_UNKNOWN_CHARACTER_SET
            | ErrorKind::ER_TOO_BIG_ROWSIZE
            | ErrorKind::ER_WRONG_OUTER_JOIN
            | ErrorKind::ER_NULL_COLUMN_IN_INDEX
            | ErrorKind::ER_PASSWORD_ANONYMOUS_USER
            | ErrorKind::ER_PASSWORD_NOT_ALLOWED
            | ErrorKind::ER_PASSWORD_NO_MATCH
            | ErrorKind::ER_REGEXP_ERROR
            | ErrorKind::ER_MIX_OF_GROUP_FUNC_AND_FIELDS
            | ErrorKind::ER_NONEXISTING_GRANT
            | ErrorKind::ER_TABLEACCESS_DENIED_ERROR
            | ErrorKind::ER_COLUMNACCESS_DENIED_ERROR
            | ErrorKind::ER_ILLEGAL_GRANT_FOR_TABLE
            | ErrorKind::ER_GRANT_WRONG_HOST_OR_USER
            | ErrorKind::ER_NONEXISTING_TABLE_GRANT
            | ErrorKind::ER_NOT_ALLOWED_COMMAND
            | ErrorKind::ER_SYNTAX_ERROR
            | ErrorKind::ER_TOO_LONG_STRING
            | ErrorKind::ER_TABLE_CANT_HANDLE_BLOB
            | ErrorKind::ER_TABLE_CANT_HANDLE_AUTO_INCREMENT
            | ErrorKind::ER_WRONG_COLUMN_NAME
            | ErrorKind::ER_WRONG_KEY_COLUMN
            | ErrorKind::ER_BLOB_KEY_WITHOUT_LENGTH
            | ErrorKind::ER_PRIMARY_CANT_HAVE_NULL
            | ErrorKind::ER_TOO_MANY_ROWS
            | ErrorKind::ER_REQUIRES_PRIMARY_KEY
            | ErrorKind::ER_KEY_DOES_NOT_EXITS
            | ErrorKind::ER_CHECK_NO_SUCH_TABLE
            | ErrorKind::ER_CHECK_NOT_IMPLEMENTED
            | ErrorKind::ER_TOO_MANY_USER_CONNECTIONS
            | ErrorKind::ER_NO_PERMISSION_TO_CREATE_USER
            | ErrorKind::ER_USER_LIMIT_REACHED
            | ErrorKind::ER_SPECIFIC_ACCESS_DENIED_ERROR
            | ErrorKind::ER_NO_DEFAULT
            | ErrorKind::ER_WRONG_VALUE_FOR_VAR
            | ErrorKind::ER_WRONG_TYPE_FOR_VAR
            | ErrorKind::ER_CANT_USE_OPTION_HERE
            | ErrorKind::ER_NOT_SUPPORTED_YET
            | ErrorKind::ER_WRONG_FK_DEF
            | ErrorKind::ER_DERIVED_MUST_HAVE_ALIAS
            | ErrorKind::ER_TABLENAME_NOT_ALLOWED_HERE
            | ErrorKind::ER_SPATIAL_CANT_HAVE_NULL
            | ErrorKind::ER_COLLATION_CHARSET_MISMATCH
            | ErrorKind::ER_WRONG_NAME_FOR_INDEX
            | ErrorKind::ER_WRONG_NAME_FOR_CATALOG
            | ErrorKind::ER_UNKNOWN_STORAGE_ENGINE
            | ErrorKind::ER_SP_ALREADY_EXISTS
            | ErrorKind::ER_SP_DOES_NOT_EXIST
            | ErrorKind::ER_SP_LILABEL_MISMATCH
            | ErrorKind::ER_SP_LABEL_REDEFINE
            | ErrorKind::ER_SP_LABEL_MISMATCH
            | ErrorKind::ER_SP_BADRETURN
            | ErrorKind::ER_UPDATE_LOG_DEPRECATED_IGNORED
            | ErrorKind::ER_UPDATE_LOG_DEPRECATED_TRANSLATED
            | ErrorKind::ER_SP_WRONG_NO_OF_ARGS
            | ErrorKind::ER_SP_COND_MISMATCH
            | ErrorKind::ER_SP_NORETURN
            | ErrorKind::ER_SP_BAD_CURSOR_QUERY
            | ErrorKind::ER_SP_BAD_CURSOR_SELECT
            | ErrorKind::ER_SP_CURSOR_MISMATCH
            | ErrorKind::ER_SP_UNDECLARED_VAR
            | ErrorKind::ER_SP_DUP_PARAM
            | ErrorKind::ER_SP_DUP_VAR
            | ErrorKind::ER_SP_DUP_COND
            | ErrorKind::ER_SP_DUP_CURS
            | ErrorKind::ER_SP_VARCOND_AFTER_CURSHNDLR
            | ErrorKind::ER_SP_CURSOR_AFTER_HANDLER
            | ErrorKind::ER_PROCACCESS_DENIED_ERROR
            | ErrorKind::ER_NONEXISTING_PROC_GRANT
            | ErrorKind::ER_SP_BAD_SQLSTATE
            | ErrorKind::ER_CANT_CREATE_USER_WITH_GRANT
            | ErrorKind::ER_SP_DUP_HANDLER
            | ErrorKind::ER_SP_NOT_VAR_ARG
            | ErrorKind::ER_TOO_BIG_SCALE
            | ErrorKind::ER_TOO_BIG_PRECISION
            | ErrorKind::ER_M_BIGGER_THAN_D
            | ErrorKind::ER_TOO_LONG_BODY
            | ErrorKind::ER_TOO_BIG_DISPLAYWIDTH
            | ErrorKind::ER_SP_BAD_VAR_SHADOW
            | ErrorKind::ER_SP_WRONG_NAME
            | ErrorKind::ER_SP_NO_AGGREGATE
            | ErrorKind::ER_MAX_PREPARED_STMT_COUNT_REACHED
            | ErrorKind::ER_NON_GROUPING_FIELD_USED
            | ErrorKind::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT
            | ErrorKind::ER_WRONG_PARAMETERS_TO_NATIVE_FCT
            | ErrorKind::ER_WRONG_PARAMETERS_TO_STORED_FCT
            | ErrorKind::ER_FUNC_INEXISTENT_NAME_COLLISION
            | ErrorKind::ER_DUP_SIGNAL_SET
            | ErrorKind::ER_SPATIAL_MUST_HAVE_GEOM_COL
            | ErrorKind::ER_TRUNCATE_ILLEGAL_FK => b"42000",
            ErrorKind::ER_RESIGNAL_WITHOUT_ACTIVE_HANDLER => b"0K000",
            ErrorKind::ER_CANT_CHANGE_TX_ISOLATION => b"25001",
            ErrorKind::ER_INVALID_USE_OF_NULL | ErrorKind::ER_WARN_NULL_TO_NOTNULL => b"22004",
            ErrorKind::ER_SP_CASE_NOT_FOUND => b"20000",
            ErrorKind::ER_DIVISION_BY_ZER => b"22012",
            ErrorKind::ER_BAD_FIELD_ERROR | ErrorKind::ER_ILLEGAL_REFERENCE => b"42S22",
            ErrorKind::ER_XAER_INVAL => b"XAE05",
            ErrorKind::ER_HASHCHK
            | ErrorKind::ER_NISAMCHK
            | ErrorKind::ER_NO
            | ErrorKind::ER_YES
            | ErrorKind::ER_CANT_CREATE_FILE
            | ErrorKind::ER_CANT_CREATE_TABLE
            | ErrorKind::ER_CANT_CREATE_DB
            | ErrorKind::ER_DB_CREATE_EXISTS
            | ErrorKind::ER_DB_DROP_EXISTS
            | ErrorKind::ER_DB_DROP_DELETE
            | ErrorKind::ER_DB_DROP_RMDIR
            | ErrorKind::ER_CANT_DELETE_FILE
            | ErrorKind::ER_CANT_FIND_SYSTEM_REC
            | ErrorKind::ER_CANT_GET_STAT
            | ErrorKind::ER_CANT_GET_WD
            | ErrorKind::ER_CANT_LOCK
            | ErrorKind::ER_CANT_OPEN_FILE
            | ErrorKind::ER_FILE_NOT_FOUND
            | ErrorKind::ER_CANT_READ_DIR
            | ErrorKind::ER_CANT_SET_WD
            | ErrorKind::ER_CHECKREAD
            | ErrorKind::ER_DISK_FULL
            | ErrorKind::ER_ERROR_ON_CLOSE
            | ErrorKind::ER_ERROR_ON_READ
            | ErrorKind::ER_ERROR_ON_RENAME
            | ErrorKind::ER_ERROR_ON_WRITE
            | ErrorKind::ER_FILE_USED
            | ErrorKind::ER_FILSORT_ABORT
            | ErrorKind::ER_FORM_NOT_FOUND
            | ErrorKind::ER_GET_ERRN
            | ErrorKind::ER_ILLEGAL_HA
            | ErrorKind::ER_KEY_NOT_FOUND
            | ErrorKind::ER_NOT_FORM_FILE
            | ErrorKind::ER_NOT_KEYFILE
            | ErrorKind::ER_OLD_KEYFILE
            | ErrorKind::ER_OPEN_AS_READONLY
            | ErrorKind::ER_UNEXPECTED_EOF
            | ErrorKind::ER_OUT_OF_RESOURCES
            | ErrorKind::ER_READY
            | ErrorKind::ER_NORMAL_SHUTDOWN
            | ErrorKind::ER_GOT_SIGNAL
            | ErrorKind::ER_SHUTDOWN_COMPLETE
            | ErrorKind::ER_TEXTFILE_NOT_READABLE
            | ErrorKind::ER_FILE_EXISTS_ERROR
            | ErrorKind::ER_LOAD_INF
            | ErrorKind::ER_ALTER_INF
            | ErrorKind::ER_WRONG_SUB_KEY
            | ErrorKind::ER_INSERT_INF
            | ErrorKind::ER_UPDATE_TABLE_USED
            | ErrorKind::ER_NO_SUCH_THREAD
            | ErrorKind::ER_KILL_DENIED_ERROR
            | ErrorKind::ER_NO_TABLES_USED
            | ErrorKind::ER_TOO_BIG_SET
            | ErrorKind::ER_NO_UNIQUE_LOGFILE
            | ErrorKind::ER_TABLE_NOT_LOCKED_FOR_WRITE
            | ErrorKind::ER_TABLE_NOT_LOCKED
            | ErrorKind::ER_UNKNOWN_ERROR
            | ErrorKind::ER_WRONG_PARAMETERS_TO_PROCEDURE
            | ErrorKind::ER_INVALID_GROUP_FUNC_USE
            | ErrorKind::ER_RECORD_FILE_FULL
            | ErrorKind::ER_TOO_MANY_TABLES
            | ErrorKind::ER_TOO_MANY_FIELDS
            | ErrorKind::ER_STACK_OVERRUN
            | ErrorKind::ER_CANT_FIND_UDF
            | ErrorKind::ER_CANT_INITIALIZE_UDF
            | ErrorKind::ER_UDF_NO_PATHS
            | ErrorKind::ER_UDF_EXISTS
            | ErrorKind::ER_CANT_OPEN_LIBRARY
            | ErrorKind::ER_CANT_FIND_DL_ENTRY
            | ErrorKind::ER_FUNCTION_NOT_DEFINED
            | ErrorKind::ER_HOST_IS_BLOCKED
            | ErrorKind::ER_HOST_NOT_PRIVILEGED
            | ErrorKind::ER_UPDATE_INF
            | ErrorKind::ER_CANT_CREATE_THREAD
            | ErrorKind::ER_CANT_REOPEN_TABLE
            | ErrorKind::ER_DELAYED_CANT_CHANGE_LOCK
            | ErrorKind::ER_TOO_MANY_DELAYED_THREADS
            | ErrorKind::ER_DELAYED_INSERT_TABLE_LOCKED
            | ErrorKind::ER_WRONG_MRG_TABLE
            | ErrorKind::ER_NO_RAID_COMPILED
            | ErrorKind::ER_UPDATE_WITHOUT_KEY_IN_SAFE_MODE
            | ErrorKind::ER_ERROR_DURING_COMMIT
            | ErrorKind::ER_ERROR_DURING_ROLLBACK
            | ErrorKind::ER_ERROR_DURING_FLUSH_LOGS
            | ErrorKind::ER_ERROR_DURING_CHECKPOINT
            | ErrorKind::ER_DUMP_NOT_IMPLEMENTED
            | ErrorKind::ER_FLUSH_MASTER_BINLOG_CLOSED
            | ErrorKind::ER_INDEX_REBUILD
            | ErrorKind::ER_MASTER
            | ErrorKind::ER_FT_MATCHING_KEY_NOT_FOUND
            | ErrorKind::ER_LOCK_OR_ACTIVE_TRANSACTION
            | ErrorKind::ER_UNKNOWN_SYSTEM_VARIABLE
            | ErrorKind::ER_CRASHED_ON_USAGE
            | ErrorKind::ER_CRASHED_ON_REPAIR
            | ErrorKind::ER_WARNING_NOT_COMPLETE_ROLLBACK
            | ErrorKind::ER_TRANS_CACHE_FULL
            | ErrorKind::ER_SLAVE_MUST_STOP
            | ErrorKind::ER_SLAVE_NOT_RUNNING
            | ErrorKind::ER_BAD_SLAVE
            | ErrorKind::ER_MASTER_INF
            | ErrorKind::ER_SLAVE_THREAD
            | ErrorKind::ER_SET_CONSTANTS_ONLY
            | ErrorKind::ER_LOCK_WAIT_TIMEOUT
            | ErrorKind::ER_LOCK_TABLE_FULL
            | ErrorKind::ER_DROP_DB_WITH_READ_LOCK
            | ErrorKind::ER_CREATE_DB_WITH_READ_LOCK
            | ErrorKind::ER_WRONG_ARGUMENTS
            | ErrorKind::ER_UNION_TABLES_IN_DIFFERENT_DIR
            | ErrorKind::ER_TABLE_CANT_HANDLE_FT
            | ErrorKind::ER_CANNOT_ADD_FOREIGN
            | ErrorKind::ER_QUERY_ON_MASTER
            | ErrorKind::ER_ERROR_WHEN_EXECUTING_COMMAND
            | ErrorKind::ER_WRONG_USAGE
            | ErrorKind::ER_CANT_UPDATE_WITH_READLOCK
            | ErrorKind::ER_MIXING_NOT_ALLOWED
            | ErrorKind::ER_DUP_ARGUMENT
            | ErrorKind::ER_LOCAL_VARIABLE
            | ErrorKind::ER_GLOBAL_VARIABLE
            | ErrorKind::ER_VAR_CANT_BE_READ
            | ErrorKind::ER_MASTER_FATAL_ERROR_READING_BINLOG
            | ErrorKind::ER_SLAVE_IGNORED_TABLE
            | ErrorKind::ER_INCORRECT_GLOBAL_LOCAL_VAR
            | ErrorKind::ER_KEY_REF_DO_NOT_MATCH_TABLE_REF
            | ErrorKind::ER_UNKNOWN_STMT_HANDLER
            | ErrorKind::ER_CORRUPT_HELP_DB
            | ErrorKind::ER_CYCLIC_REFERENCE
            | ErrorKind::ER_AUTO_CONVERT
            | ErrorKind::ER_SLAVE_WAS_RUNNING
            | ErrorKind::ER_SLAVE_WAS_NOT_RUNNING
            | ErrorKind::ER_TOO_BIG_FOR_UNCOMPRESS
            | ErrorKind::ER_ZLIB_Z_MEM_ERROR
            | ErrorKind::ER_ZLIB_Z_BUF_ERROR
            | ErrorKind::ER_ZLIB_Z_DATA_ERROR
            | ErrorKind::ER_CUT_VALUE_GROUP_CONCAT
            | ErrorKind::ER_WARN_USING_OTHER_HANDLER
            | ErrorKind::ER_CANT_AGGREGATE_2COLLATIONS
            | ErrorKind::ER_DROP_USER
            | ErrorKind::ER_REVOKE_GRANTS
            | ErrorKind::ER_CANT_AGGREGATE_3COLLATIONS
            | ErrorKind::ER_CANT_AGGREGATE_NCOLLATIONS
            | ErrorKind::ER_VARIABLE_IS_NOT_STRUCT
            | ErrorKind::ER_UNKNOWN_COLLATION
            | ErrorKind::ER_SLAVE_IGNORED_SSL_PARAMS
            | ErrorKind::ER_SERVER_IS_IN_SECURE_AUTH_MODE
            | ErrorKind::ER_WARN_FIELD_RESOLVED
            | ErrorKind::ER_BAD_SLAVE_UNTIL_COND
            | ErrorKind::ER_MISSING_SKIP_SLAVE
            | ErrorKind::ER_UNTIL_COND_IGNORED
            | ErrorKind::ER_WARN_QC_RESIZE
            | ErrorKind::ER_BAD_FT_COLUMN
            | ErrorKind::ER_UNKNOWN_KEY_CACHE
            | ErrorKind::ER_WARN_HOSTNAME_WONT_WORK
            | ErrorKind::ER_WARN_DEPRECATED_SYNTAX
            | ErrorKind::ER_NON_UPDATABLE_TABLE
            | ErrorKind::ER_FEATURE_DISABLED
            | ErrorKind::ER_OPTION_PREVENTS_STATEMENT
            | ErrorKind::ER_DUPLICATED_VALUE_IN_TYPE
            | ErrorKind::ER_TOO_MUCH_AUTO_TIMESTAMP_COLS
            | ErrorKind::ER_INVALID_ON_UPDATE
            | ErrorKind::ER_UNSUPPORTED_PS
            | ErrorKind::ER_GET_ERRMSG
            | ErrorKind::ER_GET_TEMPORARY_ERRMSG
            | ErrorKind::ER_UNKNOWN_TIME_ZONE
            | ErrorKind::ER_WARN_INVALID_TIMESTAMP
            | ErrorKind::ER_INVALID_CHARACTER_STRING
            | ErrorKind::ER_WARN_ALLOWED_PACKET_OVERFLOWED
            | ErrorKind::ER_CONFLICTING_DECLARATIONS
            | ErrorKind::ER_SP_DROP_FAILED
            | ErrorKind::ER_SP_STORE_FAILED
            | ErrorKind::ER_SP_WRONG_NO_OF_FETCH_ARGS
            | ErrorKind::ER_SP_CANT_ALTER
            | ErrorKind::ER_FPARSER_TOO_BIG_FILE
            | ErrorKind::ER_FPARSER_BAD_HEADER
            | ErrorKind::ER_FPARSER_EOF_IN_COMMENT
            | ErrorKind::ER_FPARSER_ERROR_IN_PARAMETER
            | ErrorKind::ER_FPARSER_EOF_IN_UNKNOWN_PARAMETER
            | ErrorKind::ER_VIEW_NO_EXPLAIN
            | ErrorKind::ER_FRM_UNKNOWN_TYPE
            | ErrorKind::ER_WRONG_OBJECT
            | ErrorKind::ER_NONUPDATEABLE_COLUMN
            | ErrorKind::ER_VIEW_SELECT_DERIVED
            | ErrorKind::ER_VIEW_SELECT_CLAUSE
            | ErrorKind::ER_VIEW_SELECT_VARIABLE
            | ErrorKind::ER_VIEW_SELECT_TMPTABLE
            | ErrorKind::ER_VIEW_WRONG_LIST
            | ErrorKind::ER_WARN_VIEW_MERGE
            | ErrorKind::ER_WARN_VIEW_WITHOUT_KEY
            | ErrorKind::ER_VIEW_INVALID
            | ErrorKind::ER_SP_NO_DROP_SP
            | ErrorKind::ER_SP_GOTO_IN_HNDLR
            | ErrorKind::ER_TRG_ALREADY_EXISTS
            | ErrorKind::ER_TRG_DOES_NOT_EXIST
            | ErrorKind::ER_TRG_ON_VIEW_OR_TEMP_TABLE
            | ErrorKind::ER_TRG_CANT_CHANGE_ROW
            | ErrorKind::ER_TRG_NO_SUCH_ROW_IN_TRG
            | ErrorKind::ER_NO_DEFAULT_FOR_FIELD
            | ErrorKind::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD
            | ErrorKind::ER_VIEW_NONUPD_CHECK
            | ErrorKind::ER_VIEW_CHECK_FAILED
            | ErrorKind::ER_RELAY_LOG_FAIL
            | ErrorKind::ER_PASSWD_LENGTH
            | ErrorKind::ER_UNKNOWN_TARGET_BINLOG
            | ErrorKind::ER_IO_ERR_LOG_INDEX_READ
            | ErrorKind::ER_BINLOG_PURGE_PROHIBITED
            | ErrorKind::ER_FSEEK_FAIL
            | ErrorKind::ER_BINLOG_PURGE_FATAL_ERR
            | ErrorKind::ER_LOG_IN_USE
            | ErrorKind::ER_LOG_PURGE_UNKNOWN_ERR
            | ErrorKind::ER_RELAY_LOG_INIT
            | ErrorKind::ER_NO_BINARY_LOGGING
            | ErrorKind::ER_RESERVED_SYNTAX
            | ErrorKind::ER_WSAS_FAILED
            | ErrorKind::ER_DIFF_GROUPS_PROC
            | ErrorKind::ER_NO_GROUP_FOR_PROC
            | ErrorKind::ER_ORDER_WITH_PROC
            | ErrorKind::ER_LOGGING_PROHIBIT_CHANGING_OF
            | ErrorKind::ER_NO_FILE_MAPPING
            | ErrorKind::ER_WRONG_MAGIC
            | ErrorKind::ER_PS_MANY_PARAM
            | ErrorKind::ER_KEY_PART_0
            | ErrorKind::ER_VIEW_CHECKSUM
            | ErrorKind::ER_VIEW_MULTIUPDATE
            | ErrorKind::ER_VIEW_NO_INSERT_FIELD_LIST
            | ErrorKind::ER_VIEW_DELETE_MERGE_VIEW
            | ErrorKind::ER_CANNOT_USER
            | ErrorKind::ER_PROC_AUTO_GRANT_FAIL
            | ErrorKind::ER_PROC_AUTO_REVOKE_FAIL
            | ErrorKind::ER_STARTUP
            | ErrorKind::ER_LOAD_FROM_FIXED_SIZE_ROWS_TO_VAR
            | ErrorKind::ER_WRONG_VALUE_FOR_TYPE
            | ErrorKind::ER_TABLE_DEF_CHANGED
            | ErrorKind::ER_FAILED_ROUTINE_BREAK_BINLOG
            | ErrorKind::ER_BINLOG_UNSAFE_ROUTINE
            | ErrorKind::ER_BINLOG_CREATE_ROUTINE_NEED_SUPER
            | ErrorKind::ER_EXEC_STMT_WITH_OPEN_CURSOR
            | ErrorKind::ER_STMT_HAS_NO_OPEN_CURSOR
            | ErrorKind::ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG
            | ErrorKind::ER_NO_DEFAULT_FOR_VIEW_FIELD
            | ErrorKind::ER_SP_NO_RECURSION
            | ErrorKind::ER_WRONG_LOCK_OF_SYSTEM_TABLE
            | ErrorKind::ER_CONNECT_TO_FOREIGN_DATA_SOURCE
            | ErrorKind::ER_QUERY_ON_FOREIGN_DATA_SOURCE
            | ErrorKind::ER_FOREIGN_DATA_SOURCE_DOESNT_EXIST
            | ErrorKind::ER_FOREIGN_DATA_STRING_INVALID_CANT_CREATE
            | ErrorKind::ER_FOREIGN_DATA_STRING_INVALID
            | ErrorKind::ER_CANT_CREATE_FEDERATED_TABLE
            | ErrorKind::ER_TRG_IN_WRONG_SCHEMA
            | ErrorKind::ER_STACK_OVERRUN_NEED_MORE
            | ErrorKind::ER_WARN_CANT_DROP_DEFAULT_KEYCACHE
            | ErrorKind::ER_CANT_UPDATE_USED_TABLE_IN_SF_OR_TRG
            | ErrorKind::ER_VIEW_PREVENT_UPDATE
            | ErrorKind::ER_PS_NO_RECURSION
            | ErrorKind::ER_SP_CANT_SET_AUTOCOMMIT
            | ErrorKind::ER_MALFORMED_DEFINER
            | ErrorKind::ER_VIEW_FRM_NO_USER
            | ErrorKind::ER_VIEW_OTHER_USER
            | ErrorKind::ER_NO_SUCH_USER
            | ErrorKind::ER_FORBID_SCHEMA_CHANGE
            | ErrorKind::ER_TRG_NO_DEFINER
            | ErrorKind::ER_OLD_FILE_FORMAT
            | ErrorKind::ER_SP_RECURSION_LIMIT
            | ErrorKind::ER_SP_PROC_TABLE_CORRUPT
            | ErrorKind::ER_TABLE_NEEDS_UPGRADE
            | ErrorKind::ER_VIEW_RECURSIVE
            | ErrorKind::ER_TABLE_CANT_HANDLE_SPKEYS
            | ErrorKind::ER_NO_TRIGGERS_ON_SYSTEM_SCHEMA
            | ErrorKind::ER_REMOVED_SPACES
            | ErrorKind::ER_AUTOINC_READ_FAILED
            | ErrorKind::ER_USERNAME
            | ErrorKind::ER_HOSTNAME
            | ErrorKind::ER_WRONG_STRING_LENGTH
            | ErrorKind::ER_NON_INSERTABLE_TABLE
            | ErrorKind::ER_ADMIN_WRONG_MRG_TABLE
            | ErrorKind::ER_TOO_HIGH_LEVEL_OF_NESTING_FOR_SELECT
            | ErrorKind::ER_NAME_BECOMES_EMPTY
            | ErrorKind::ER_AMBIGUOUS_FIELD_TERM
            | ErrorKind::ER_FOREIGN_SERVER_EXISTS
            | ErrorKind::ER_FOREIGN_SERVER_DOESNT_EXIST
            | ErrorKind::ER_ILLEGAL_HA_CREATE_OPTION
            | ErrorKind::ER_PARTITION_REQUIRES_VALUES_ERROR
            | ErrorKind::ER_PARTITION_WRONG_VALUES_ERROR
            | ErrorKind::ER_PARTITION_MAXVALUE_ERROR
            | ErrorKind::ER_PARTITION_SUBPARTITION_ERROR
            | ErrorKind::ER_PARTITION_SUBPART_MIX_ERROR
            | ErrorKind::ER_PARTITION_WRONG_NO_PART_ERROR
            | ErrorKind::ER_PARTITION_WRONG_NO_SUBPART_ERROR
            | ErrorKind::ER_WRONG_EXPR_IN_PARTITION_FUNC_ERROR
            | ErrorKind::ER_NO_CONST_EXPR_IN_RANGE_OR_LIST_ERROR
            | ErrorKind::ER_FIELD_NOT_FOUND_PART_ERROR
            | ErrorKind::ER_LIST_OF_FIELDS_ONLY_IN_HASH_ERROR
            | ErrorKind::ER_INCONSISTENT_PARTITION_INFO_ERROR
            | ErrorKind::ER_PARTITION_FUNC_NOT_ALLOWED_ERROR
            | ErrorKind::ER_PARTITIONS_MUST_BE_DEFINED_ERROR
            | ErrorKind::ER_RANGE_NOT_INCREASING_ERROR
            | ErrorKind::ER_INCONSISTENT_TYPE_OF_FUNCTIONS_ERROR
            | ErrorKind::ER_MULTIPLE_DEF_CONST_IN_LIST_PART_ERROR
            | ErrorKind::ER_PARTITION_ENTRY_ERROR
            | ErrorKind::ER_MIX_HANDLER_ERROR
            | ErrorKind::ER_PARTITION_NOT_DEFINED_ERROR
            | ErrorKind::ER_TOO_MANY_PARTITIONS_ERROR
            | ErrorKind::ER_SUBPARTITION_ERROR
            | ErrorKind::ER_CANT_CREATE_HANDLER_FILE
            | ErrorKind::ER_BLOB_FIELD_IN_PART_FUNC_ERROR
            | ErrorKind::ER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF
            | ErrorKind::ER_NO_PARTS_ERROR
            | ErrorKind::ER_PARTITION_MGMT_ON_NONPARTITIONED
            | ErrorKind::ER_FOREIGN_KEY_ON_PARTITIONED
            | ErrorKind::ER_DROP_PARTITION_NON_EXISTENT
            | ErrorKind::ER_DROP_LAST_PARTITION
            | ErrorKind::ER_COALESCE_ONLY_ON_HASH_PARTITION
            | ErrorKind::ER_REORG_HASH_ONLY_ON_SAME_N
            | ErrorKind::ER_REORG_NO_PARAM_ERROR
            | ErrorKind::ER_ONLY_ON_RANGE_LIST_PARTITION
            | ErrorKind::ER_ADD_PARTITION_SUBPART_ERROR
            | ErrorKind::ER_ADD_PARTITION_NO_NEW_PARTITION
            | ErrorKind::ER_COALESCE_PARTITION_NO_PARTITION
            | ErrorKind::ER_REORG_PARTITION_NOT_EXIST
            | ErrorKind::ER_SAME_NAME_PARTITION
            | ErrorKind::ER_NO_BINLOG_ERROR
            | ErrorKind::ER_CONSECUTIVE_REORG_PARTITIONS
            | ErrorKind::ER_REORG_OUTSIDE_RANGE
            | ErrorKind::ER_PARTITION_FUNCTION_FAILURE
            | ErrorKind::ER_PART_STATE_ERROR
            | ErrorKind::ER_LIMITED_PART_RANGE
            | ErrorKind::ER_PLUGIN_IS_NOT_LOADED
            | ErrorKind::ER_WRONG_VALUE
            | ErrorKind::ER_NO_PARTITION_FOR_GIVEN_VALUE
            | ErrorKind::ER_FILEGROUP_OPTION_ONLY_ONCE
            | ErrorKind::ER_CREATE_FILEGROUP_FAILED
            | ErrorKind::ER_DROP_FILEGROUP_FAILED
            | ErrorKind::ER_TABLESPACE_AUTO_EXTEND_ERROR
            | ErrorKind::ER_WRONG_SIZE_NUMBER
            | ErrorKind::ER_SIZE_OVERFLOW_ERROR
            | ErrorKind::ER_ALTER_FILEGROUP_FAILED
            | ErrorKind::ER_BINLOG_ROW_LOGGING_FAILED
            | ErrorKind::ER_BINLOG_ROW_WRONG_TABLE_DEF
            | ErrorKind::ER_BINLOG_ROW_RBR_TO_SBR
            | ErrorKind::ER_EVENT_ALREADY_EXISTS
            | ErrorKind::ER_EVENT_STORE_FAILED
            | ErrorKind::ER_EVENT_DOES_NOT_EXIST
            | ErrorKind::ER_EVENT_CANT_ALTER
            | ErrorKind::ER_EVENT_DROP_FAILED
            | ErrorKind::ER_EVENT_INTERVAL_NOT_POSITIVE_OR_TOO_BIG
            | ErrorKind::ER_EVENT_ENDS_BEFORE_STARTS
            | ErrorKind::ER_EVENT_EXEC_TIME_IN_THE_PAST
            | ErrorKind::ER_EVENT_OPEN_TABLE_FAILED
            | ErrorKind::ER_EVENT_NEITHER_M_EXPR_NOR_M_AT
            | ErrorKind::ER_COL_COUNT_DOESNT_MATCH_CORRUPTED
            | ErrorKind::ER_CANNOT_LOAD_FROM_TABLE
            | ErrorKind::ER_EVENT_CANNOT_DELETE
            | ErrorKind::ER_EVENT_COMPILE_ERROR
            | ErrorKind::ER_EVENT_SAME_NAME
            | ErrorKind::ER_EVENT_DATA_TOO_LONG
            | ErrorKind::ER_DROP_INDEX_FK
            | ErrorKind::ER_WARN_DEPRECATED_SYNTAX_WITH_VER
            | ErrorKind::ER_CANT_WRITE_LOCK_LOG_TABLE
            | ErrorKind::ER_CANT_LOCK_LOG_TABLE
            | ErrorKind::ER_COL_COUNT_DOESNT_MATCH_PLEASE_UPDATE
            | ErrorKind::ER_TEMP_TABLE_PREVENTS_SWITCH_OUT_OF_RBR
            | ErrorKind::ER_STORED_FUNCTION_PREVENTS_SWITCH_BINLOG_FORMAT
            | ErrorKind::ER_NDB_CANT_SWITCH_BINLOG_FORMAT
            | ErrorKind::ER_PARTITION_NO_TEMPORARY
            | ErrorKind::ER_PARTITION_CONST_DOMAIN_ERROR
            | ErrorKind::ER_PARTITION_FUNCTION_IS_NOT_ALLOWED
            | ErrorKind::ER_DDL_LOG_ERROR
            | ErrorKind::ER_NULL_IN_VALUES_LESS_THAN
            | ErrorKind::ER_WRONG_PARTITION_NAME
            | ErrorKind::ER_DUP_ENTRY_AUTOINCREMENT_CASE
            | ErrorKind::ER_EVENT_MODIFY_QUEUE_ERROR
            | ErrorKind::ER_EVENT_SET_VAR_ERROR
            | ErrorKind::ER_PARTITION_MERGE_ERROR
            | ErrorKind::ER_CANT_ACTIVATE_LOG
            | ErrorKind::ER_RBR_NOT_AVAILABLE
            | ErrorKind::ER_BASE64_DECODE_ERROR
            | ErrorKind::ER_EVENT_RECURSION_FORBIDDEN
            | ErrorKind::ER_EVENTS_DB_ERROR
            | ErrorKind::ER_ONLY_INTEGERS_ALLOWED
            | ErrorKind::ER_UNSUPORTED_LOG_ENGINE
            | ErrorKind::ER_BAD_LOG_STATEMENT
            | ErrorKind::ER_CANT_RENAME_LOG_TABLE
            | ErrorKind::ER_NATIVE_FCT_NAME_COLLISION
            | ErrorKind::ER_BINLOG_PURGE_EMFILE
            | ErrorKind::ER_EVENT_CANNOT_CREATE_IN_THE_PAST
            | ErrorKind::ER_EVENT_CANNOT_ALTER_IN_THE_PAST
            | ErrorKind::ER_SLAVE_INCIDENT
            | ErrorKind::ER_NO_PARTITION_FOR_GIVEN_VALUE_SILENT
            | ErrorKind::ER_BINLOG_UNSAFE_STATEMENT
            | ErrorKind::ER_SLAVE_FATAL_ERROR
            | ErrorKind::ER_SLAVE_RELAY_LOG_READ_FAILURE
            | ErrorKind::ER_SLAVE_RELAY_LOG_WRITE_FAILURE
            | ErrorKind::ER_SLAVE_CREATE_EVENT_FAILURE
            | ErrorKind::ER_SLAVE_MASTER_COM_FAILURE
            | ErrorKind::ER_BINLOG_LOGGING_IMPOSSIBLE
            | ErrorKind::ER_VIEW_NO_CREATION_CTX
            | ErrorKind::ER_VIEW_INVALID_CREATION_CTX
            | ErrorKind::ER_SR_INVALID_CREATION_CTX
            | ErrorKind::ER_TRG_CORRUPTED_FILE
            | ErrorKind::ER_TRG_NO_CREATION_CTX
            | ErrorKind::ER_TRG_INVALID_CREATION_CTX
            | ErrorKind::ER_EVENT_INVALID_CREATION_CTX
            | ErrorKind::ER_TRG_CANT_OPEN_TABLE
            | ErrorKind::ER_CANT_CREATE_SROUTINE
            | ErrorKind::ER_UNUSED_11
            | ErrorKind::ER_NO_FORMAT_DESCRIPTION_EVENT_BEFORE_BINLOG_STATEMENT
            | ErrorKind::ER_SLAVE_CORRUPT_EVENT
            | ErrorKind::ER_LOAD_DATA_INVALID_COLUMN
            | ErrorKind::ER_LOG_PURGE_NO_FILE
            | ErrorKind::ER_NEED_REPREPARE
            | ErrorKind::ER_DELAYED_NOT_SUPPORTED
            | ErrorKind::WARN_NO_MASTER_INF
            | ErrorKind::WARN_OPTION_IGNORED
            | ErrorKind::WARN_PLUGIN_DELETE_BUILTIN
            | ErrorKind::WARN_PLUGIN_BUSY
            | ErrorKind::ER_VARIABLE_IS_READONLY
            | ErrorKind::ER_WARN_ENGINE_TRANSACTION_ROLLBACK
            | ErrorKind::ER_SLAVE_HEARTBEAT_FAILURE
            | ErrorKind::ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE
            | ErrorKind::ER_NDB_REPLICATION_SCHEMA_ERROR
            | ErrorKind::ER_CONFLICT_FN_PARSE_ERROR
            | ErrorKind::ER_EXCEPTIONS_WRITE_ERROR
            | ErrorKind::ER_TOO_LONG_TABLE_COMMENT
            | ErrorKind::ER_TOO_LONG_FIELD_COMMENT
            | ErrorKind::ER_DATABASE_NAME
            | ErrorKind::ER_TABLE_NAME
            | ErrorKind::ER_PARTITION_NAME
            | ErrorKind::ER_SUBPARTITION_NAME
            | ErrorKind::ER_TEMPORARY_NAME
            | ErrorKind::ER_RENAMED_NAME
            | ErrorKind::ER_TOO_MANY_CONCURRENT_TRXS
            | ErrorKind::WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED
            | ErrorKind::ER_DEBUG_SYNC_TIMEOUT
            | ErrorKind::ER_DEBUG_SYNC_HIT_LIMIT
            | ErrorKind::ER_SIGNAL_EXCEPTION
            | ErrorKind::ER_SIGNAL_BAD_CONDITION_TYPE
            | ErrorKind::WARN_COND_ITEM_TRUNCATED
            | ErrorKind::ER_COND_ITEM_TOO_LONG
            | ErrorKind::ER_UNKNOWN_LOCALE
            | ErrorKind::ER_SLAVE_IGNORE_SERVER_IDS
            | ErrorKind::ER_QUERY_CACHE_DISABLED
            | ErrorKind::ER_SAME_NAME_PARTITION_FIELD
            | ErrorKind::ER_PARTITION_COLUMN_LIST_ERROR
            | ErrorKind::ER_WRONG_TYPE_COLUMN_VALUE_ERROR
            | ErrorKind::ER_TOO_MANY_PARTITION_FUNC_FIELDS_ERROR
            | ErrorKind::ER_MAXVALUE_IN_VALUES_IN
            | ErrorKind::ER_TOO_MANY_VALUES_ERROR
            | ErrorKind::ER_ROW_SINGLE_PARTITION_FIELD_ERROR
            | ErrorKind::ER_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD
            | ErrorKind::ER_PARTITION_FIELDS_TOO_LONG
            | ErrorKind::ER_BINLOG_ROW_ENGINE_AND_STMT_ENGINE
            | ErrorKind::ER_BINLOG_ROW_MODE_AND_STMT_ENGINE
            | ErrorKind::ER_BINLOG_UNSAFE_AND_STMT_ENGINE
            | ErrorKind::ER_BINLOG_ROW_INJECTION_AND_STMT_ENGINE
            | ErrorKind::ER_BINLOG_STMT_MODE_AND_ROW_ENGINE
            | ErrorKind::ER_BINLOG_ROW_INJECTION_AND_STMT_MODE
            | ErrorKind::ER_BINLOG_MULTIPLE_ENGINES_AND_SELF_LOGGING_ENGINE
            | ErrorKind::ER_BINLOG_UNSAFE_LIMIT
            | ErrorKind::ER_BINLOG_UNSAFE_INSERT_DELAYED
            | ErrorKind::ER_BINLOG_UNSAFE_SYSTEM_TABLE
            | ErrorKind::ER_BINLOG_UNSAFE_AUTOINC_COLUMNS
            | ErrorKind::ER_BINLOG_UNSAFE_UDF
            | ErrorKind::ER_BINLOG_UNSAFE_SYSTEM_VARIABLE
            | ErrorKind::ER_BINLOG_UNSAFE_SYSTEM_FUNCTION
            | ErrorKind::ER_BINLOG_UNSAFE_NONTRANS_AFTER_TRANS
            | ErrorKind::ER_MESSAGE_AND_STATEMENT
            | ErrorKind::ER_SLAVE_CONVERSION_FAILED
            | ErrorKind::ER_SLAVE_CANT_CREATE_CONVERSION
            | ErrorKind::ER_INSIDE_TRANSACTION_PREVENTS_SWITCH_BINLOG_FORMAT
            | ErrorKind::ER_PATH_LENGTH
            | ErrorKind::ER_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT
            | ErrorKind::ER_WRONG_NATIVE_TABLE_STRUCTURE
            | ErrorKind::ER_WRONG_PERFSCHEMA_USAGE
            | ErrorKind::ER_WARN_I_S_SKIPPED_TABLE
            | ErrorKind::ER_INSIDE_TRANSACTION_PREVENTS_SWITCH_BINLOG_DIRECT
            | ErrorKind::ER_STORED_FUNCTION_PREVENTS_SWITCH_BINLOG_DIRECT
            | ErrorKind::ER_TOO_LONG_INDEX_COMMENT
            | ErrorKind::ER_LOCK_ABORTED
            | ErrorKind::ER_WRONG_SPVAR_TYPE_IN_LIMIT
            | ErrorKind::ER_BINLOG_UNSAFE_MULTIPLE_ENGINES_AND_SELF_LOGGING_ENGINE
            | ErrorKind::ER_BINLOG_UNSAFE_MIXED_STATEMENT
            | ErrorKind::ER_INSIDE_TRANSACTION_PREVENTS_SWITCH_SQL_LOG_BIN
            | ErrorKind::ER_STORED_FUNCTION_PREVENTS_SWITCH_SQL_LOG_BIN
            | ErrorKind::ER_FAILED_READ_FROM_PAR_FILE
            | ErrorKind::ER_VALUES_IS_NOT_INT_TYPE_ERROR
            | ErrorKind::ER_SET_PASSWORD_AUTH_PLUGIN
            | ErrorKind::ER_GRANT_PLUGIN_USER_EXISTS
            | ErrorKind::ER_PLUGIN_IS_PERMANENT
            | ErrorKind::ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE_MIN
            | ErrorKind::ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE_MAX
            | ErrorKind::ER_STMT_CACHE_FULL
            | ErrorKind::ER_MULTI_UPDATE_KEY_CONFLICT
            | ErrorKind::ER_TABLE_NEEDS_REBUILD
            | ErrorKind::WARN_OPTION_BELOW_LIMIT
            | ErrorKind::ER_INDEX_COLUMN_TOO_LONG
            | ErrorKind::ER_ERROR_IN_TRIGGER_BODY
            | ErrorKind::ER_ERROR_IN_UNKNOWN_TRIGGER_BODY
            | ErrorKind::ER_INDEX_CORRUPT
            | ErrorKind::ER_UNDO_RECORD_TOO_BIG
            | ErrorKind::ER_BINLOG_UNSAFE_INSERT_IGNORE_SELECT
            | ErrorKind::ER_BINLOG_UNSAFE_INSERT_SELECT_UPDATE
            | ErrorKind::ER_BINLOG_UNSAFE_REPLACE_SELECT
            | ErrorKind::ER_BINLOG_UNSAFE_CREATE_IGNORE_SELECT
            | ErrorKind::ER_BINLOG_UNSAFE_CREATE_REPLACE_SELECT
            | ErrorKind::ER_BINLOG_UNSAFE_UPDATE_IGNORE
            | ErrorKind::ER_PLUGIN_NO_UNINSTALL
            | ErrorKind::ER_PLUGIN_NO_INSTALL
            | ErrorKind::ER_BINLOG_UNSAFE_WRITE_AUTOINC_SELECT
            | ErrorKind::ER_BINLOG_UNSAFE_CREATE_SELECT_AUTOINC
            | ErrorKind::ER_BINLOG_UNSAFE_INSERT_TWO_KEYS
            | ErrorKind::ER_TABLE_IN_FK_CHECK
            | ErrorKind::ER_UNSUPPORTED_ENGINE
            | ErrorKind::ER_BINLOG_UNSAFE_AUTOINC_NOT_FIRST
            | ErrorKind::ER_CANNOT_LOAD_FROM_TABLE_V2
            | ErrorKind::ER_MASTER_DELAY_VALUE_OUT_OF_RANGE
            | ErrorKind::ER_ONLY_FD_AND_RBR_EVENTS_ALLOWED_IN_BINLOG_STATEMENT
            | ErrorKind::ER_PARTITION_EXCHANGE_DIFFERENT_OPTION
            | ErrorKind::ER_PARTITION_EXCHANGE_PART_TABLE
            | ErrorKind::ER_PARTITION_EXCHANGE_TEMP_TABLE
            | ErrorKind::ER_PARTITION_INSTEAD_OF_SUBPARTITION
            | ErrorKind::ER_UNKNOWN_PARTITION
            | ErrorKind::ER_TABLES_DIFFERENT_METADATA
            | ErrorKind::ER_ROW_DOES_NOT_MATCH_PARTITION
            | ErrorKind::ER_BINLOG_CACHE_SIZE_GREATER_THAN_MAX
            | ErrorKind::ER_WARN_INDEX_NOT_APPLICABLE
            | ErrorKind::ER_PARTITION_EXCHANGE_FOREIGN_KEY
            | ErrorKind::ER_NO_SUCH_KEY_VALUE
            | ErrorKind::ER_RPL_INFO_DATA_TOO_LONG
            | ErrorKind::ER_NETWORK_READ_EVENT_CHECKSUM_FAILURE
            | ErrorKind::ER_BINLOG_READ_EVENT_CHECKSUM_FAILURE
            | ErrorKind::ER_BINLOG_STMT_CACHE_SIZE_GREATER_THAN_MAX
            | ErrorKind::ER_CANT_UPDATE_TABLE_IN_CREATE_TABLE_SELECT
            | ErrorKind::ER_PARTITION_CLAUSE_ON_NONPARTITIONED
            | ErrorKind::ER_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET
            | ErrorKind::ER_NO_SUCH_PARTITION_UNUSED
            | ErrorKind::ER_CHANGE_RPL_INFO_REPOSITORY_FAILURE
            | ErrorKind::ER_WARNING_NOT_COMPLETE_ROLLBACK_WITH_CREATED_TEMP_TABLE
            | ErrorKind::ER_WARNING_NOT_COMPLETE_ROLLBACK_WITH_DROPPED_TEMP_TABLE
            | ErrorKind::ER_MTS_FEATURE_IS_NOT_SUPPORTED
            | ErrorKind::ER_MTS_UPDATED_DBS_GREATER_MAX
            | ErrorKind::ER_MTS_CANT_PARALLEL
            | ErrorKind::ER_MTS_INCONSISTENT_DATA
            | ErrorKind::ER_FULLTEXT_NOT_SUPPORTED_WITH_PARTITIONING
            | ErrorKind::ER_INSECURE_PLAIN_TEXT
            | ErrorKind::ER_INSECURE_CHANGE_MASTER
            | ErrorKind::ER_SQLTHREAD_WITH_SECURE_SLAVE
            | ErrorKind::ER_TABLE_HAS_NO_FT
            | ErrorKind::ER_VARIABLE_NOT_SETTABLE_IN_SF_OR_TRIGGER
            | ErrorKind::ER_VARIABLE_NOT_SETTABLE_IN_TRANSACTION
            | ErrorKind::ER_GTID_NEXT_IS_NOT_IN_GTID_NEXT_LIST
            | ErrorKind::ER_CANT_CHANGE_GTID_NEXT_IN_TRANSACTION_WHEN_GTID_NEXT_LIST_IS_NULL
            | ErrorKind::ER_SET_STATEMENT_CANNOT_INVOKE_FUNCTION
            | ErrorKind::ER_GTID_NEXT_CANT_BE_AUTOMATIC_IF_GTID_NEXT_LIST_IS_NON_NULL
            | ErrorKind::ER_SKIPPING_LOGGED_TRANSACTION
            | ErrorKind::ER_MALFORMED_GTID_SET_SPECIFICATION
            | ErrorKind::ER_MALFORMED_GTID_SET_ENCODING
            | ErrorKind::ER_MALFORMED_GTID_SPECIFICATION
            | ErrorKind::ER_GNO_EXHAUSTED
            | ErrorKind::ER_BAD_SLAVE_AUTO_POSITION
            | ErrorKind::ER_AUTO_POSITION_REQUIRES_GTID_MODE_ON
            | ErrorKind::ER_CANT_DO_IMPLICIT_COMMIT_IN_TRX_WHEN_GTID_NEXT_IS_SET
            | ErrorKind::ER_GTID_MODE_2_OR_3_REQUIRES_DISABLE_GTID_UNSAFE_STATEMENTS_ON
            | ErrorKind::ER_GTID_MODE_REQUIRES_BINLOG
            | ErrorKind::ER_CANT_SET_GTID_NEXT_TO_GTID_WHEN_GTID_MODE_IS_OFF
            | ErrorKind::ER_CANT_SET_GTID_NEXT_TO_ANONYMOUS_WHEN_GTID_MODE_IS_ON
            | ErrorKind::ER_CANT_SET_GTID_NEXT_LIST_TO_NON_NULL_WHEN_GTID_MODE_IS_OFF
            | ErrorKind::ER_FOUND_GTID_EVENT_WHEN_GTID_MODE_IS_OFF
            | ErrorKind::ER_GTID_UNSAFE_NON_TRANSACTIONAL_TABLE
            | ErrorKind::ER_GTID_UNSAFE_CREATE_SELECT
            | ErrorKind::ER_GTID_UNSAFE_CREATE_DROP_TEMPORARY_TABLE_IN_TRANSACTION
            | ErrorKind::ER_GTID_MODE_CAN_ONLY_CHANGE_ONE_STEP_AT_A_TIME
            | ErrorKind::ER_MASTER_HAS_PURGED_REQUIRED_GTIDS
            | ErrorKind::ER_CANT_SET_GTID_NEXT_WHEN_OWNING_GTID
            | ErrorKind::ER_UNKNOWN_EXPLAIN_FORMAT
            | ErrorKind::ER_TOO_LONG_TABLE_PARTITION_COMMENT
            | ErrorKind::ER_SLAVE_CONFIGURATION
            | ErrorKind::ER_INNODB_FT_LIMIT
            | ErrorKind::ER_INNODB_NO_FT_TEMP_TABLE
            | ErrorKind::ER_INNODB_FT_WRONG_DOCID_COLUMN
            | ErrorKind::ER_INNODB_FT_WRONG_DOCID_INDEX
            | ErrorKind::ER_INNODB_ONLINE_LOG_TOO_BIG
            | ErrorKind::ER_UNKNOWN_ALTER_ALGORITHM
            | ErrorKind::ER_UNKNOWN_ALTER_LOCK
            | ErrorKind::ER_MTS_CHANGE_MASTER_CANT_RUN_WITH_GAPS
            | ErrorKind::ER_MTS_RECOVERY_FAILURE
            | ErrorKind::ER_MTS_RESET_WORKERS
            | ErrorKind::ER_COL_COUNT_DOESNT_MATCH_CORRUPTED_V2
            | ErrorKind::ER_SLAVE_SILENT_RETRY_TRANSACTION
            | ErrorKind::ER_DISCARD_FK_CHECKS_RUNNING
            | ErrorKind::ER_TABLE_SCHEMA_MISMATCH
            | ErrorKind::ER_TABLE_IN_SYSTEM_TABLESPACE
            | ErrorKind::ER_IO_READ_ERROR
            | ErrorKind::ER_IO_WRITE_ERROR
            | ErrorKind::ER_TABLESPACE_MISSING
            | ErrorKind::ER_TABLESPACE_EXISTS
            | ErrorKind::ER_TABLESPACE_DISCARDED
            | ErrorKind::ER_INTERNAL_ERROR
            | ErrorKind::ER_INNODB_IMPORT_ERROR
            | ErrorKind::ER_INNODB_INDEX_CORRUPT
            | ErrorKind::ER_INVALID_YEAR_COLUMN_LENGTH
            | ErrorKind::ER_NOT_VALID_PASSWORD
            | ErrorKind::ER_MUST_CHANGE_PASSWORD
            | ErrorKind::ER_FK_NO_INDEX_CHILD
            | ErrorKind::ER_FK_NO_INDEX_PARENT
            | ErrorKind::ER_FK_FAIL_ADD_SYSTEM
            | ErrorKind::ER_FK_CANNOT_OPEN_PARENT
            | ErrorKind::ER_FK_INCORRECT_OPTION
            | ErrorKind::ER_FK_DUP_NAME
            | ErrorKind::ER_PASSWORD_FORMAT
            | ErrorKind::ER_FK_COLUMN_CANNOT_DROP
            | ErrorKind::ER_FK_COLUMN_CANNOT_DROP_CHILD
            | ErrorKind::ER_FK_COLUMN_NOT_NULL
            | ErrorKind::ER_DUP_INDEX
            | ErrorKind::ER_FK_COLUMN_CANNOT_CHANGE
            | ErrorKind::ER_FK_COLUMN_CANNOT_CHANGE_CHILD
            | ErrorKind::ER_FK_CANNOT_DELETE_PARENT
            | ErrorKind::ER_MALFORMED_PACKET
            | ErrorKind::ER_READ_ONLY_MODE
            | ErrorKind::ER_GTID_NEXT_TYPE_UNDEFINED_GROUP
            | ErrorKind::ER_VARIABLE_NOT_SETTABLE_IN_SP
            | ErrorKind::ER_CANT_SET_GTID_PURGED_WHEN_GTID_MODE_IS_OFF
            | ErrorKind::ER_CANT_SET_GTID_PURGED_WHEN_GTID_EXECUTED_IS_NOT_EMPTY
            | ErrorKind::ER_CANT_SET_GTID_PURGED_WHEN_OWNED_GTIDS_IS_NOT_EMPTY
            | ErrorKind::ER_GTID_PURGED_WAS_CHANGED
            | ErrorKind::ER_GTID_EXECUTED_WAS_CHANGED
            | ErrorKind::ER_BINLOG_STMT_MODE_AND_NO_REPL_TABLES
            | ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_COPY
            | ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_PARTITION
            | ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_FK_RENAME
            | ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_COLUMN_TYPE
            | ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_FK_CHECK
            | ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_IGNORE
            | ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_NOPK
            | ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_AUTOINC
            | ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_HIDDEN_FTS
            | ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_CHANGE_FTS
            | ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_FTS
            | ErrorKind::ER_SQL_SLAVE_SKIP_COUNTER_NOT_SETTABLE_IN_GTID_MODE
            | ErrorKind::ER_IDENT_CAUSES_TOO_LONG_PATH
            | ErrorKind::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_NOT_NULL
            | ErrorKind::ER_MUST_CHANGE_PASSWORD_LOGIN
            | ErrorKind::ER_ROW_IN_WRONG_PARTITION
            | ErrorKind::ER_MTS_EVENT_BIGGER_PENDING_JOBS_SIZE_MAX
            | ErrorKind::ER_INNODB_NO_FT_USES_PARSER
            | ErrorKind::ER_BINLOG_LOGICAL_CORRUPTION
            | ErrorKind::ER_WARN_PURGE_LOG_IN_USE
            | ErrorKind::ER_WARN_PURGE_LOG_IS_ACTIVE
            | ErrorKind::ER_AUTO_INCREMENT_CONFLICT
            | ErrorKind::WARN_ON_BLOCKHOLE_IN_RBR
            | ErrorKind::ER_SLAVE_MI_INIT_REPOSITORY
            | ErrorKind::ER_SLAVE_RLI_INIT_REPOSITORY
            | ErrorKind::ER_INNODB_READ_ONLY
            | ErrorKind::ER_STOP_SLAVE_SQL_THREAD_TIMEOUT
            | ErrorKind::ER_STOP_SLAVE_IO_THREAD_TIMEOUT
            | ErrorKind::ER_TABLE_CORRUPT
            | ErrorKind::ER_TEMP_FILE_WRITE_FAILURE
            | ErrorKind::ER_INNODB_FT_AUX_NOT_HEX_ID
            | ErrorKind::ER_OLD_TEMPORALS_UPGRADED
            | ErrorKind::ER_INNODB_FORCED_RECOVERY
            | ErrorKind::ER_AES_INVALID_IV
            | ErrorKind::ER_PLUGIN_CANNOT_BE_UNINSTALLED
            | ErrorKind::ER_GTID_UNSAFE_BINLOG_SPLITTABLE_STATEMENT_AND_GTID_GROUP
            | ErrorKind::ER_SLAVE_HAS_MORE_GTIDS_THAN_MASTER => b"HY000",
            ErrorKind::ER_XAER_NOTA => b"XAE04",
            ErrorKind::ER_XA_RBROLLBACK => b"XA100",
            ErrorKind::ER_DATA_TOO_LONG => b"22001",
            ErrorKind::ER_LOCK_DEADLOCK => b"40001",
            ErrorKind::ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT
            | ErrorKind::ER_OPERAND_COLUMNS
            | ErrorKind::ER_SUBQUERY_NO_1_ROW => b"21000",
        }
    }
}
