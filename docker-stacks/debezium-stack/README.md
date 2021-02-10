# Overview
This docker stack brings up the whole context needed to monitor
different databases through Debezium.

## Components
The following components are available from this docker stack:

| Component                | Hostname     | Internal ports       | External ports      |
| :-------:                |:--------:    | :-----------:        | :-----------:       |
| Zookeeper                | zookeeper    | 2181<br>2888<br>3888 | 2181<br>2888<br>3888|
| Kafka                    | kafka        | 9092                 | 9092                |
| Kafka Connect (Debezium) | kafkaconnect | 8083                 | 8083                |
| Postgres                 | postgres     | 5432                 | 5432                |
| MariaDB                  | mariadb      | 3308                 | 3308                |

## How to configure
To start using this stack, first you must uncomment the DBs that you want
use from the `docker-compose.yaml` file.

### Kafka Connectors
#### Add more connectors
To add a new Kafka Connector, just add a new JSON file
containing the new connector's configuration
into `kafka-connector-setup/connectors/[database]`.

### Databases
#### Access information
The database services run using the following access information:

| Variable | Value |
| :------: | :---: |
| User     | root  |
| Password | debezium |
| Database | test |

#### Initialization scripts
If you would like to do additional initialization, 
add one or more `*.sql`, `*.sql.gz`, or `*.sh` scripts under
`[postgres|mariadb]/docker-entrypoint-initdb.d`. 
After the database is initialized, it will run (in alphabetical order)
any `*.sql` files, 
run any executable `*.sh` scripts, 
and source any non-executable `*.sh` scripts found in that directory 
to do further initialization before starting the service.

### Running queries
For ease of use, after bringing up the whole docker stack and after
initializing the Debezium Connector Applications for each database
(out of the scope of this docker stack), you can run the
`databases-setup/setup.sh` script.
This script will read the different `*.sql` file under `queries/[postgres|mariadb]/`
and execute them in the corresponding database.

Keep in mind that might take some time for Debezium to catch up with
all the messages the databases will generate.

## How to run
To run the whole docker stack, simply run
```bash
docker-compose up
```