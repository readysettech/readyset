#!/bin/bash -e

HOST=127.0.0.1
CONNECTION_STRING="postgresql://postgres:readyset@${HOST}:5433/testdb"

setup_colors() {
  read -rp "Do you like colorful terminal output? (y/n, default y): " color_choice
  color_choice=${color_choice:-y}
  if [[ $color_choice == "y" ]]; then
    echo -e "Good choice!"
    export BLUE="\033[1;34m"
    export GREEN="\033[1;32m"
    export NOCOLOR="\033[0m"
    export RED="\033[1;31m"
    export YELLOW="\033[1;33m"
    export APPLE="🍏"
    export ELEPHANT="🐘"
    export GLOBE="🌐"
    export GREEN_CHECK="✅"
    export INFO="ℹ️ "
    export MAGNIFYING_GLASS="🔍"
    export ROCKET="🚀"
    export WARNING="⚠️ "
    export WHALE="🐳"
    export ROTATING_LIGHT="🚨"
    export SUNGLASSES="😎"
    export TADA="🎉"
  else
    echo -e "Very well."
  fi
}

print_banner() {
  echo ""
  echo -e "${BLUE}██████╗ ███████╗ █████╗ ██████╗ ██╗   ██╗███████╗███████╗████████╗"
  echo -e "${BLUE}██╔══██╗██╔════╝██╔══██╗██╔══██╗╚██╗ ██╔╝██╔════╝██╔════╝╚══██╔══╝"
  echo -e "${BLUE}██████╔╝█████╗  ███████║██║  ██║ ╚████╔╝ ███████╗█████╗     ██║   "
  echo -e "${BLUE}██╔══██╗██╔══╝  ██╔══██║██║  ██║  ╚██╔╝  ╚════██║██╔══╝     ██║   "
  echo -e "${BLUE}██║  ██║███████╗██║  ██║██████╔╝   ██║   ███████║███████╗   ██║   "
  echo -e "${BLUE}╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═════╝    ╚═╝   ╚══════╝╚══════╝   ╚═╝   "
  echo -e "${NOCOLOR}"
}

check_docker_dependencies() {
  if ! command -v docker &>/dev/null; then
    echo -e "${RED}Docker is not installed. Please install Docker to continue.${NOCOLOR}"
    exit 1
  elif ! docker ps &>/dev/null; then
    echo -e "${RED}Docker is installed but not running. Please start Docker to continue.${NOCOLOR}"
    exit 1
  fi
  if ! command -v docker-compose &>/dev/null; then
    echo -e "${RED}Docker Compose is not installed. Please install Docker Compose to continue.${NOCOLOR}"
    exit 1
  fi
}

check_dependencies() {
  if ! command -v curl &>/dev/null; then
    echo -e "${RED}curl is not installed. How did you even get this script?! Please install curl to continue.${NOCOLOR}"
    exit 1
  fi
}

download_demo_compose_file() {
  echo -e "${BLUE}${WHALE}Downloading the ReadySet Docker Compose file... ${NOCOLOR}"
  curl -Ls -o readyset.compose.yml "https://readyset.io/quickstart/compose.postgres.yml"
}

download_byo_compose_file() {
  echo -e "${BLUE}${WHALE}Downloading the ReadySet Docker Compose file... ${NOCOLOR}"
  curl -Ls -o /tmp/readyset.compose.yml "https://readyset.io/quickstart/compose.yml"
}

run_docker_compose() {
  echo -e "${BLUE}${WHALE}Running the ReadySet Docker Compose setup... ${NOCOLOR}"
  if ! docker compose -f readyset.compose.yml pull --quiet; then
    echo -e "${RED}${ROTATING_LIGHT}Unable to pull ReadySet images.${NOCOLOR}"
    exit 1
  fi
  if ! docker compose -f readyset.compose.yml up -d --wait-timeout 60; then
    echo -e "${RED}${ROTATING_LIGHT}Docker-compose setup failed.${NOCOLOR}"
    exit 1
  fi

  echo -e "${GREEN}${GREEN_CHECK}ReadySet Docker Compose setup complete! ${NOCOLOR}"
  echo -e "${INFO}To clean up, run \`docker-compose -f readyset.compose.yml down\`"
}

check_sample_data() {
  retry_count=0
  max_retries=5
  sleep_interval_secs=1

  echo -e "${BLUE}${ELEPHANT}Checking if sample data is already loaded...${NOCOLOR}"
  dots=""

  while :; do
    tables_exist=$(timeout 2 psql $CONNECTION_STRING -tAc "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename IN ('title_basics', 'title_ratings'));" 2>/dev/null | head -n 1 | tr -d '[:space:]')

    if [[ $tables_exist == "t" ]] || [[ $retry_count -eq $max_retries ]]; then
      break
    fi

    dots+="."
    echo -ne "$dots"
    ((retry_count++))
    sleep $sleep_interval_secs
  done

  echo ""
  if [[ $tables_exist == "t" ]]; then
    echo -e "${GREEN}${GREEN_CHECK}Sample data detected!${NOCOLOR}"
  else
    echo -e "${YELLOW}No Sample data detected.${NOCOLOR}"
  fi
}

prompt_for_import() {
  if [[ $tables_exist != "t" ]]; then
    read -rp "Import sample data? (y/n, default: y): " import_choice
    import_choice=${import_choice:-y}
  fi
}

import_data() {
  if [[ $import_choice == "y" ]]; then
    if [ ! -f imdb-postgres.sql ]; then
      echo -e "${BLUE}${ELEPHANT}Downloading IMDB sample data to imdb-postgres.sql...${NOCOLOR}"
      curl -L "https://readyset.io/quickstart/imdb-postgres.sql" -o imdb-postgres.sql
    else
      echo "Sample data found."
    fi

    echo -e "${BLUE}${ELEPHANT}Importing sample data...${NOCOLOR}"
    if command -v pv &>/dev/null; then
      pv imdb-postgres.sql | psql $CONNECTION_STRING >/dev/null 2>&1
    else
      echo -e "This may take a few minutes. Install \`pv\` if you would like to see a progress bar for this step."
      psql $CONNECTION_STRING < imdb-postgres.sql >/dev/null 2>&1
    fi

    echo -e "${GREEN}${GREEN_CHECK}Sample data imported successfully!${NOCOLOR}"
  fi
}

display_arm_warning() {
  if [[ $(uname -m) == "arm64" ]]; then
    echo -e "${YELLOW}${WARNING}You are running on an ARM-based Machine, but ReadySet is currently built for x86_64."
    echo -e "   Query performance will be slower due to virtualization overhead.${NOCOLOR}"
  fi
}

explore_data() {
  if [[ $tables_exist == "t" ]] || [[ $import_choice == "y" ]]; then
    read -rp "Explore sample data in psql? (y/n, default: y): " explore_choice
    explore_choice=${explore_choice:-y}
    if [[ $explore_choice == "y" ]]; then
      echo -e "${BLUE}${MAGNIFYING_GLASS}Connecting to ReadySet to explore the dataset.${NOCOLOR}"
  psql $CONNECTION_STRING <<EOF
\set QUIET 1
\o /dev/null
-- In case we ran this before, reset the value that we will be changing.
UPDATE title_ratings
   SET averagerating = 2.5
 WHERE tconst = 'tt0185183';
-- Also drop the cache so that the uncached->miss->hit latencies work.
DROP CACHE q_bccd97aea07c545f;
\o
\timing
\! echo "${BLUE}Let's cache a query with ReadySet!${NOCOLOR}"
\set QUIET 1
\! echo 'Press enter to continue.'
\prompt c
\! echo "${BLUE}Here's the query we want to cache:${NOCOLOR}"
\echo ''
\echo '    SELECT count(*)'
\echo '      FROM title_ratings'
\echo '      JOIN title_basics'
\echo '        ON title_ratings.tconst = title_basics.tconst'
\echo '     WHERE title_basics.startyear = 2000'
\echo '       AND title_ratings.averagerating > 5;'
\echo ''
\! echo "${BLUE}Let's run it once before caching.${NOCOLOR}"
\echo 'Press enter to run query.'
\prompt c

\! echo '${YELLOW}Query Results:'
SELECT count(*) FROM title_ratings
JOIN title_basics ON title_ratings.tconst = title_basics.tconst
WHERE title_basics.startyear = 2000
AND title_ratings.averagerating > 5;
\echo '${NOCOLOR}'
\! echo "${RED}${ROTATING_LIGHT}Too slow! ${BLUE}Let's cache it.${SUNGLASSES}${NOCOLOR}"
\echo ''
\echo 'Press enter to continue.'
\prompt c
\echo '    CREATE CACHE FROM'
\echo '       SELECT count(*)'
\echo '         FROM title_ratings'
\echo '         JOIN title_basics'
\echo '           ON title_ratings.tconst = title_basics.tconst'
\echo '        WHERE title_basics.startyear = 2000'
\echo '          AND title_ratings.averagerating > 5;'
\echo ''
\echo 'Press enter to create the cache.'
\prompt c
\! echo '${GREEN}Query Results:'
CREATE CACHE FROM
    SELECT count(*)
      FROM title_ratings
      JOIN title_basics
        ON title_ratings.tconst = title_basics.tconst
     WHERE title_basics.startyear = 2000
       AND title_ratings.averagerating > 5;
\! echo "${NOCOLOR}"
\! echo "${GREEN}${GREEN_CHECK}Cache created${NOCOLOR}"
\! echo '${BLUE}Lets take a look at the cache we created.${NOCOLOR}'
\echo ''
\echo 'Press enter to run SHOW CACHES.'
\prompt c
SHOW CACHES;
\! echo ''
\! echo "${BLUE}It worked!${NOCOLOR}"
\! echo "Press enter to continue."
\prompt c
\! echo "Let's re-run the query twice."
\! echo "The first time will be a cache miss and populate the cache."
\! echo "The second time will be a cache hit."
\! echo ''
\echo 'Press enter to re-run query.'
\prompt c
\! echo '${BLUE}Cache Miss Results:'
SELECT count(*) FROM title_ratings
JOIN title_basics ON title_ratings.tconst = title_basics.tconst
WHERE title_basics.startyear = 2000
AND title_ratings.averagerating > 5;
\! echo "${NOCOLOR}"
\set QUIET 1
\timing
\o /dev/null
-- Here we run it twice because if we dropped the cache as part of the flow,
-- the first query will be proxied, the 2nd one will be an upquery/cache miss,
-- and the 3rd one and thereafter will be a cache hit.
SELECT count(*) FROM title_ratings
JOIN title_basics ON title_ratings.tconst = title_basics.tconst
WHERE title_basics.startyear = 2000
AND title_ratings.averagerating > 5;
\o
\timing
\unset QUIET
\! echo "Press enter to re-run the query again."
\prompt c
\! echo '${GREEN}Cache Hit Results:'
SELECT count(*) FROM title_ratings
JOIN title_basics ON title_ratings.tconst = title_basics.tconst
WHERE title_basics.startyear = 2000
AND title_ratings.averagerating > 5;
\! echo "${NOCOLOR}"
\! echo "${GREEN}${TADA}Yay, it's faster!${NOCOLOR}"
\! echo "Press enter to continue."
\prompt c
\! echo "${BLUE}Next, let's see how ReadySet updates the cache automatically when we change it.${NOCOLOR}"
\echo 'Press enter to continue.'
\prompt c
\! echo "The query we have been running returns the count of movies in the year"
\! echo "2000 that had a rating greater than 5/10 (2,418 movies)."
\! echo ''
\echo 'Press enter to continue.'
\prompt c
\! echo "'Battlefield Earth' was a movie released in '00 that received poor ratings."
\! echo ''
\echo 'Press enter to run query and see just how bad.'
\prompt c
\echo '    SELECT'
\echo '      title_basics.tconst,'
\echo '      title_basics.primarytitle,'
\echo '      title_ratings.averagerating,'
\echo '      title_ratings.numvotes '
\echo '    FROM'
\echo '      title_basics'
\echo '    INNER JOIN'
\echo '      title_ratings'
\echo '    ON'
\echo '      title_ratings.tconst = title_basics.tconst'
\echo '    WHERE'
\echo '      title_basics.primarytitle = 'Battlefield Earth';'
\echo ''
\! echo "${YELLOW}"
SELECT
 title_basics.tconst,
 title_basics.primarytitle,
 title_ratings.averagerating,
 title_ratings.numvotes
FROM
 title_basics
INNER JOIN
 title_ratings
ON
 title_ratings.tconst = title_basics.tconst
WHERE
 title_basics.primarytitle = 'Battlefield Earth';
\! echo "${NOCOLOR}"
\echo 'Press enter to continue.'
\prompt c
\! echo "Looks like it scored an average rating of 2.5. Yikes."
\! echo "It was, indeed, an awful movie. Nevertheless, historical revisionism is fun"
\! echo "when you have full control of the data."
\echo ''
\echo 'Press enter to continue.'
\prompt c
\! echo "${BLUE}Let's grab the id for 'Battlefield Earth' (tt0185183) and update its average rating accordingly:${NOCOLOR}"
\echo 'Press enter to change the course of cinematic history.'
\prompt c
\! echo "    UPDATE title_ratings"
\! echo "       SET averagerating = 5.1"
\! echo "     WHERE tconst = 'tt0185183';"
\! echo ""
UPDATE title_ratings
   SET averagerating = 5.1
 WHERE tconst = 'tt0185183';
\! echo "${BLUE}Let's re-run the previously cached query that returns the count of movies:${NOCOLOR}"
\echo 'Press enter to re-run query.'
\prompt c
\! echo '${GREEN}New Results:'
SELECT count(*) FROM title_ratings
JOIN title_basics ON title_ratings.tconst = title_basics.tconst
WHERE title_basics.startyear = 2000
AND title_ratings.averagerating > 5;

\! echo "And bingo! The count has been increased by one (i.e 2,419 vs 2,418)."
\! echo "${GREEN}${TADA}The cache is auto-updated!${NOCOLOR}"
\! echo "And this time was still a cache hit. Not too shabby."
\echo ''
\echo 'Press enter to continue.'
\prompt c
\echo ''
\! echo '${BLUE}This concludes our guided exploration.${NOCOLOR}'
\echo ''
\echo 'Press enter continue.'
\prompt c
\! echo '${BLUE}Give these commands a try next!${NOCOLOR}'
\! echo ''
\! echo ' ${BLUE}Show status info. about ReadySet.${NOCOLOR}'
\! echo '    SHOW READYSET STATUS;'
\! echo ' ${BLUE}List information about current caches${NOCOLOR}'
\! echo '    SHOW CACHES;'
\! echo ' ${BLUE}List tables that ReadySet has snapshotted${NOCOLOR}'
\! echo '    SHOW READYSET TABLES;'
\! echo ' ${BLUE}Show queries that havent been cached and if they are supported or not.${NOCOLOR}'
\! echo '    SHOW PROXIED QUERIES;'
\! echo " ${BLUE}Drop an existing cache.${NOCOLOR}"
\! echo "    DROP CACHE [query_id];"
\echo ''
\echo 'Press enter to conclude and connect to readyset via psql.'
\prompt c
\unset QUIET
\q
EOF
    fi
  fi
}

free_form_connect() {
  echo -e "${BLUE}Connecting to ReadySet...${NOCOLOR}"
  if [[ $1 == "psql" ]]; then
    echo -e "${BLUE}Type \q to exit.${NOCOLOR}"
    psql $CONNECTION_STRING
  else
    echo -e "${BLUE}Type 'exit' to exit.${NOCOLOR}"
    parse_mysql_connection_string
    mysql -h "127.0.0.1" -P "3307" -u $username -p$password
  fi
}

print_exit_message() {
  echo ""
  echo -e "${BLUE}See ${NOCOLOR}https://docs.readyset.io/demo${BLUE} for more fun things to try.${NOCOLOR}"
  echo ""
  echo -e "${BLUE}Join us on slack:${NOCOLOR}"
  echo "https://join.slack.com/t/readysetcommunity/shared_invite/zt-2272gtiz4-0024xeRJUPGWlRETQrGkFw"

  if [[ $1 == "psql" ]]; then
    echo -e "${BLUE}To connect to ReadySet again, run:${NOCOLOR}"
    echo -e "    psql $CONNECTION_STRING"
  elif [[ $1 == "mysql" ]]; then
    echo -e "${BLUE}To connect to ReadySet again, run:${NOCOLOR}"
    echo -e "mysql -h 127.0.0.1 -P 3307 -u $username -p$password"
  fi
}

switch_on_mode() {
  echo -e "${BLUE}Would you like to run the demo or connect to your own db?${NOCOLOR}"
  read -rp "demo(d)/postgres(p)/mysql(m), default: d: " mode
  if [[ $mode == "p" ]]; then
    if ! command -v psql &>/dev/null; then
      echo -e "${RED}psql (PostgreSQL client) is not installed. Please install psql to continue.${NOCOLOR}"
      exit 1
    fi

    run_byo_postgres
  elif [[ $mode == "m" ]]; then
    if ! command -v mysql &>/dev/null; then
      echo -e "${RED}mysql (MySQL client) is not installed. Please install mysql to continue.${NOCOLOR}"
      exit 1
    fi
    run_byo_mysql
  elif [[ $mode == "d" ]]; then
    run_demo
  else
    echo -e "${RED}Invalid option. Please try again.${NOCOLOR}"
    switch_on_mode
  fi
}

postgres_docker_compose_setup() {
  download_byo_compose_file
  sed "s|# UPSTREAM_DB_URL:|UPSTREAM_DB_URL: $USER_CONNECTION_STRING|g" /tmp/readyset.compose.yml > readyset.compose.yml
  run_docker_compose
}

mysql_docker_compose_setup() {
  download_byo_compose_file
  # We use the same compose file and still inject the connection string, but also replace port 5433 with 3307 for mysql
  sed "s|# UPSTREAM_DB_URL:|UPSTREAM_DB_URL: $USER_CONNECTION_STRING|g" /tmp/readyset.compose.yml | sed "s|5433|3307|g" > readyset.compose.yml
  run_docker_compose
}

wait_for_snapshot() {
  if [[ $1 == "psql" ]]; then
    expected_log="Streaming replication started"
  elif [[ $1 == "mysql" ]]; then
    expected_log="Starting binlog replication"
  fi

  echo -e "${BLUE}ReadySet will now snapshot the connected database.${NOCOLOR}"
  echo -e "${BLUE}This may take a while if the data set is large.${NOCOLOR}"
  echo -e "${WHALE} Watching logs for \"$expected_log\" ${NOCOLOR}"
  echo "   with \`docker logs -f readyset-cache-1\`"

  dots=""
  error_count=0

  while true; do
    if docker logs readyset-cache-1 2> /dev/null | grep -q "$expected_log"; then
      echo -e "\n${GREEN}${GREEN_CHECK} Replication started successfully.${NOCOLOR}"
      break
    elif docker logs readyset-cache-1 2> /dev/null | grep -q "ERROR"; then
      # Allow for a few errors to occur before failing, in case the issue is transient.
      error_count=$((error_count+1))
      if [ $error_count -ge 5 ]; then
        echo "${RED}Error detected in replication:${NOCOLOR}"
        docker logs readyset-cache-1 | grep "ERROR" | tail -n 1
        echo "See https://docs.readyset.io/get-started for troubleshooting, or reach out on slack."

        exit 1
      fi
    fi
    dots+="."
    echo -ne "$dots"
    sleep 5
  done
}

explore_connection() {
  echo -e "${BLUE}Welcome to ReadySet!${NOCOLOR}"
  echo -e "${BLUE}Give these custom ReadySet SQL commands a try!${NOCOLOR}"
  echo -e " ${BLUE}Show status info. about ReadySet.${NOCOLOR}"
  echo -e "    SHOW READYSET STATUS;"
  echo -e " ${BLUE}Show version info.${NOCOLOR}"
  echo -e "    SHOW READYSET VERSION;"
  echo -e " ${BLUE}Cache a query.${NOCOLOR}"
  echo -e "    CREATE CACHE FROM <query, e.g. SELECT * from foo where bar = 1>;"
  echo -e " ${BLUE}List information about current caches${NOCOLOR}"
  echo -e "    SHOW CACHES;"
  echo -e " ${BLUE}List tables that ReadySet has snapshotted${NOCOLOR}"
  echo -e "    SHOW READYSET TABLES;"
  echo -e " ${BLUE}Show queries that havent been cached and if they are supported or not.${NOCOLOR}"
  echo -e "    SHOW PROXIED QUERIES;"
  echo -e " ${BLUE}Drop an existing cache.${NOCOLOR}"
  echo -e "    DROP CACHE [query_id];"
}

run_byo_postgres() {
  echo -e "${BLUE}Enter your postgres connection string.${NOCOLOR}";
  echo -e "Example: postgresql://postgres:readyset@127.0.0.1:5432/testdb"
  read -rp "Connection String: " USER_CONNECTION_STRING
  while ! docker run --rm postgres:14.1 psql "$USER_CONNECTION_STRING" -c ';'; do
    echo -e "${RED}Unable to connect. Please double check connection string and try again.${NOCOLOR}"
    print_localhost_hint
    echo -e "(The command used to check is \`docker run --rm postgres psql \$USER_CONNECTION_STRING -c ';'\`)"
    read -rp "Connection String: " USER_CONNECTION_STRING
  done
  echo -e "${GREEN}${GREEN_CHECK}Connection String verified.${NOCOLOR}"

  postgres_docker_compose_setup
  # Transform the user's connection string into a readyset one:
  echo "$USER_CONNECTION_STRING" | sed -E 's/@[^:/]+:[0-9]+/@127.0.0.1:5433/' > CONNECTION_STRING

  run_after_connection "psql"
}

run_after_connection() {
  display_arm_warning
  wait_for_snapshot $1
  explore_connection
  free_form_connect $1
  print_exit_message $1
}

print_localhost_hint() {
  # If it's a variation of localhost, give a hint about docker networking.
  if  [[ $USER_CONNECTION_STRING == *"localhost"* ]] || [[ $USER_CONNECTION_STRING == *"127.0.0.1"* ]]; then
    echo -e "${YELLOW}Hint: If you are connecting locally via docker, you may need to use the docker host name or ip${NOCOLOR}"
    echo -e "${YELLOW}(e.g. host.docker.internal for Macs, or 172.17.0.1 for linux.) ${NOCOLOR}"
  fi
}

parse_mysql_connection_string() {
  username=$(echo "$USER_CONNECTION_STRING" | sed -E 's|mysql://([^:]+):.*|\1|')
  password=$(echo "$USER_CONNECTION_STRING" | sed -E 's|mysql://[^:]+:(.*)@.*|\1|')
  host=$(echo "$USER_CONNECTION_STRING" | sed -E 's|mysql://[^:]+:[^@]+@([^:/]+).*|\1|')
  port=$(echo "$USER_CONNECTION_STRING" | sed -E 's|mysql://[^:]+:[^@]+@[^:]+:([0-9]+).*|\1|')
}

run_byo_mysql() {
  echo -e "${BLUE}Enter your mysql connection string.${NOCOLOR}";
  echo -e "Example: mysql://root:readyset@127.0.0.1:3306/testdb"
  read -rp "Connection String: " USER_CONNECTION_STRING
  parse_mysql_connection_string
  while ! docker run --rm  mysql mysql -h $host -P $port -u $username -p$password -e ';' > /dev/null 2>&1; do
    echo -e "${RED}Unable to connect. Please double check connection string and try again.${NOCOLOR}"
    print_localhost_hint
    echo -e "(The command used to check is \`docker run --rm mysql mysql -h $host -P $port -u $username -p$password -e ';'\`)"
    read -rp "Connection String: " USER_CONNECTION_STRING
    parse_mysql_connection_string
  done
  echo -e "${GREEN}${GREEN_CHECK}Connection String verified.${NOCOLOR}"

  mysql_docker_compose_setup
  echo "$USER_CONNECTION_STRING" | sed -E 's/@[^:/]+:[0-9]+/@127.0.0.1:3307/' > CONNECTION_STRING
  run_after_connection "mysql"
}

run_demo() {
  download_demo_compose_file
  run_docker_compose
  check_sample_data
  prompt_for_import
  import_data
  display_arm_warning
  explore_data
  free_form_connect "psql"
  print_exit_message "psql"
}

# Main
setup_colors
print_banner
check_docker_dependencies
check_dependencies
# run either demo or bring-your-own mode
switch_on_mode
