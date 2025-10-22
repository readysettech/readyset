#!/usr/bin/env bash
set -euo pipefail

cargo run --bin readyset-logictest -- verify logictests
TZ=UTC cargo run --bin readyset-logictest -- verify logictests/timeseries --database-type postgresql
cargo run --bin readyset-logictest -- verify logictests/psql --database-type postgresql
cargo run --bin readyset-logictest -- verify logictests/mysql --database-type mysql
cargo run --bin readyset-logictest -- verify logictests/replicated/psql --database-type postgresql --replication-url postgresql://postgres:noria@postgres/noria
cargo run --bin readyset-logictest -- verify logictests/replicated/postgis --database-type postgresql --replication-url postgresql://postgres:noria@postgres/noria --parsing-preset both-prefer-sqlparser
cargo run --bin readyset-logictest -- verify logictests/replicated/mysql --database-type mysql --replication-url mysql://root:noria@mysql/noria
cargo run --bin readyset-logictest -- verify logictests/sqlparser --parsing-preset=only-sqlparser
cargo run --bin readyset-logictest -- verify logictests/sqlparser/mysql --parsing-preset=only-sqlparser --database-type mysql
cargo run --bin readyset-logictest -- verify logictests/sqlparser/psql --parsing-preset=only-sqlparser --database-type postgresql
