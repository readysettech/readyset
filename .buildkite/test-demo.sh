#!/usr/bin/env bash
################################################################################
# ReadySet Demo Script test
################################################################################
#
# This suite of tests uses expect(1) to interactively check the output of the
# readyset demo script when provided with different inputs.
#
################################################################################

# Path to the main script
DEMO_SCRIPT="./quickstart/readyset_demo.sh"

DEMO_SCRIPT_TMP=$(mktemp)

# If on a Mac, override the docker host address to be the host.docker.internal
if [ -z "$DOCKER_HOST_ADDR" ]; then
    # This will likely be 172.17.0.1 if on Linux
    DOCKER_HOST_ADDR=$(docker network inspect bridge --format='{{(index .IPAM.Config 0).Gateway}}')
fi

# Allows setting this manually if testing locally
if [ -z "$READYSET_DOCKER_IMG" ]; then
  # Use the latest image that hasn't been pushed to docker hub yet (if any) to test it before releasing it
  READYSET_DOCKER_IMG="public.ecr.aws/z3o1l5n4/readyset:latest"
fi

# Because the demo pulls compose files from static github paths (which in turn pull a docker image from docker hub),
# We need to do some modifications to the demo script and docker compose files so that we are testing the pre-released version
# of those artifacts.
# First, modify the compose files to pull the readyset image from the build rather than docker hub
for file in ./quickstart/compose*.yml; do
    sed -i '' "s|docker.io/readysettech/readyset:latest|$READYSET_DOCKER_IMG|g" "$file"

done

# Rename localhost instances to work inside a docker container
# Next, modify the demo script to copy those files over from the local clone instead of curling from github.
# Note: This puts the compose file in /tmp because the demo script will expect it there and do some modifications itself before writing it to .
sed -e "s|HOST=127.0.0.1|HOST=$DOCKER_HOST_ADDR|" \
  -e "s|mysql -h \"127.0.0.1\"|mysql -h \"$DOCKER_HOST_ADDR\"|" \
  -e "s|127.0.0.1:3307|${DOCKER_HOST_ADDR}|g" \
  -e "s|curl.*compose\.postgres\.yml\"|cp ./quickstart/compose.postgres.yml readyset.compose.yml|" \
  -e "s|curl.*quickstart/compose\.yml\"|cp ./quickstart/compose.yml /tmp/readyset.compose.yml|" \
  "$DEMO_SCRIPT" > "$DEMO_SCRIPT_TMP"

# Figure out how many times we need to press enter to run through the entire psql
# interactive section of the demo.
N_ENTERS=$(grep -c "Press enter" "$DEMO_SCRIPT_TMP")

# Function to run the readyset demo automatically
run_script() {
    # Annoyingly, expect exits with 0 if it sees a syntax error in the script, so let's
    # use return code 42 to signal success and manually translate that to 0 afterwards.
    expect -c "
        spawn bash $DEMO_SCRIPT_TMP

        set timeout 8
        expect \"Do you like colorful terminal output? (y/n, default y): \" {
          send \"$1\r\"
        }
        if {\"$1\" == \"y\"} {
          expect \"Good choice!\"
        } else {
          expect \"Very well.\"
        }

        expect {
          -re \"Would you like to run the demo or connect to your own db.*\" {
            send \"$2\r\"
          }
        }
        if {\"$2\" == \"d\"} {
          set timeout 60
          expect \"Checking if sample data is already loaded\" {}

          expect {
              \"No Sample data detected\" {
              expect -re \".*Import sample data.*\" {
                send \"$3\r\"
              }

              # Importing the sample data can take a few minutes
              # so there is a pretty long timeout.
              set timeout 500
              if {\"$3\" == \"y\"} {
                expect -re \".*Explore sample data in psql.*\" {
                  send \"$4\r\"
                }
              }

              set timeout 8
              if {\"$4\" == \"y\"} {
                for {set i 0} {\$i < $N_ENTERS} {incr i} {
                  expect {
                    -re \".*Press enter.*\" {
                        send \"\r\"
                    }
                    -re \".*error:,*\" {
                      send_user -- psql error encountered
                      exit 2
                    }
                  }
                }
              }
            }
          }

          expect \"Press enter to conclude and connect to readyset via psql.\" {
            send \"\r\"
          }
        } else {
          expect -re \"Connection String:\" {
            send \"$3\r\"
          }

          set timeout 60
          expect -re \"Replication started successfully\" {}
          set timeout 8
        }

        if {\"$2\" == \"d\" || \"$2\" == \"p\"} {
          # Postgres prompt
          expect -re \".*testdb=>\" {
            send \"exit\r\n\"
          }
        } else {
          # MySQL prompt
          expect -re \"MySQL\" {
            send \"exit\r\n\"
        }
      }


        expect {
          -re \"Join us on slack:\" {
            send_user -- \"PASS: Test reached the expected end\"
            exit 42
          }
        }

        exit 3
    "

    ret=$?
    if [[ $ret -eq 42 ]]; then
      return 0
    else
      # If we are going to fail, dump some debug information that may be useful to look at
      show_docker_info
      docker logs readyset-cache-1
      # The return code could have been 0, so exit with 1 explicitly
      exit 1
    fi
}

test_combination() {
    combo=$1;

    echo -e "--- Testing combination (colorful input?, import sample data? explore?) ${combo}"
    read -ra answers <<< "$combo"
    run_script "${answers[0]}" "${answers[1]}" "${answers[2]}" "${answers[3]}"
    echo "Test passed for combination: $combo"
    rm -f readyset.compose.yml
}

show_docker_info() {
  docker ps -a --format '{{.Names}}'
  docker volume ls --format '{{.Name}}'
}

# If testing locally, it's convenient to automatically reset the initial state by
# stopping the containers and removing the associated volumes.
reset_deployment_state() {
  if [ -f "readyset.compose.yml" ]; then
    echo "'readyset.compose.yml' found. Running 'docker-compose down' to reset the environment."
    docker-compose -f readyset.compose.yml down -v > /dev/null 2>&1
  else
    echo "'readyset.compose.yml' not found. Proceeding with the tests."
  fi

  # Try to stop any docker containers that could have been made by previous runs.
  # This is because we sometimes see a previous docker container still running at the beginning of a retry
  echo "Cleaning up any stale docker containers and volumes:"
  show_docker_info
  docker ps -a      --format '{{.Names}}' | grep 'readyset' | xargs -I {} docker stop {}
  docker volume  ls --format '{{.Name}}'  | grep 'readyset' | xargs -I {} docker volume  rm {}
}

run_mysql_docker() {
  docker run --name readyset-byo-mysql --rm -d \
    -p 3306:3306 \
    -e MYSQL_ROOT_PASSWORD=readyset \
    -e MYSQL_DATABASE=testdb \
    --health-cmd='mysqladmin ping -h localhost' \
    --health-interval=10s \
    --health-timeout=5s \
    --health-retries=3 \
    mysql:8.2
}

run_postgres_docker() {
  # Use POSTGRES_HOST_AUTH_METHOD=trust to allow testing connection strings without passwords.
  docker run --name readyset-byo-psql --rm -d \
    -p 5434:5432 \
    -e POSTGRES_PASSWORD=readyset \
    -e POSTGRES_DB=testdb \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    --health-cmd='pg_isready -U postgres -d testdb' \
    --health-interval=2s \
    --health-timeout=5s \
    --health-retries=3 \
    postgres:14.1 -c 'wal_level=logical'

  # Wait for PostgreSQL to start
  sleep 10

  docker exec readyset-byo-psql psql -U postgres -c "CREATE DATABASE \"testdbnopw\";"
}

################################################################################
# Test different combinations of answers to branching prompts.
#
# Prompts:
# 1. Color output? (always provided)
# 2. Demo(d) or bring-your-own postgres? (p)
# 3. Import sample data? (only valid if data is not already loaded)
# 4. Explore sample data? (only provided if data is loaded)
################################################################################
test_all_combinations() {
    reset_deployment_state

    # Note: This intentionally uses 127.0.0.1 instead of $DOCKER_HOST_ADDR to test the functionality
    # that we auto fixup 127.0.0.1 if we need to for docker

    local psql_connection_string="postgresql://postgres:readyset@127.0.0.1:5434/testdb"
    local psql_connection_string_no_pw="postgresql://postgres@127.0.0.1:5434/testdbnopw"
    local psql_connection_string_empty_pw="postgresql://postgres:@127.0.0.1:5434/testdbnopw"
    local mysql_connection_string="mysql://root:readyset@127.0.0.1:3306/testdb"

    local combinations=(
      "n d n n"
      "y d y y"
      "y p $psql_connection_string"
      "y p $psql_connection_string_empty_pw"
      "y p $psql_connection_string_no_pw"
      "y m $mysql_connection_string"
    )

    for combo in "${combinations[@]}"; do
      if [[ "$combo" == *"m"* ]]; then
        reset_deployment_state
        run_mysql_docker
        # Give the containers some extra time to start up and stabilize.
        sleep 30
      elif [[ "$combo" == *"p"* ]]; then
        reset_deployment_state
        run_postgres_docker
        # Give the containers some extra time to start up and stabilize.
        sleep 30
      fi


      # Allow for a few retries for each step.
      max_retries=3
      test_combination "${combo}" $max_retries
    done
}

# Make sure 'expect' is installed
if ! which expect > /dev/null; then
  exit 1
fi

test_all_combinations
