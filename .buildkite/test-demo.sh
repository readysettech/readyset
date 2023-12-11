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

# rename localhost to work inside a docker container
# If running locally on a mac, use this line to switch 127.0.0.1 with host.docker.internal.
# sed 's/HOST="127.0.0.1"/HOST=host.docker.internal/' "$DEMO_SCRIPT" > "$DEMO_SCRIPT_TMP"
# Use 172.17.0.1 for docker-in-docker on linux
sed 's/HOST="127.0.0.1"/HOST=172.17.0.1/' "$DEMO_SCRIPT" > "$DEMO_SCRIPT_TMP"

# Figure out how many times we need to press enter to run through the entire psql
# interactive section of the demo.
N_ENTERS=$(grep -c "Press enter" "$DEMO_SCRIPT_TMP")

# Function to run the readyset demo automatically
# Importing the sample data can take a few minutes, so there is a pretty long timeout.
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
          -re \"Would you like to run the demo or connect to your own postgres db.*\" {
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
        }

        expect -re \".*testdb=>\" {
          send \"exit\r\n\"
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
      return $ret
    fi
}

test_combination() {
    combo=$1;
    retries=$2

    echo -e "--- Testing combination (colorful input?, import sample data? explore?) ${combo}"
    read -ra answers <<< "$combo"
    if ! run_script "${answers[0]}" "${answers[1]}" "${answers[2]}" "${answers[3]}"; then
        echo "Test failed for combination $combo with code $?"
        if [[ $retries -gt 0 ]]; then
          echo "Retrying..."
          retries=$((retries-1))
          test_combination "${combo}" $retries
        else
          echo "All retries failed for combo: $combo"
          exit 1
        fi
    else
        echo "Test passed for combination: $combo"
    fi
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
  docker run --name readyset-byo-psql --rm -d \
    -p 5434:5432 \
    -e POSTGRES_PASSWORD=readyset \
    -e POSTGRES_DB=testdb \
    --health-cmd='pg_isready -U postgres -d testdb' \
    --health-interval=10s \
    --health-timeout=5s \
    --health-retries=3 \
    postgres:14.1 -c 'wal_level=logical'
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

    # Mac
    # local connection_string="postgresql://postgres:readyset@host.docker.internal:5434/testdb"
    # Linux
    local connection_string="postgresql://postgres:readyset@172.17.0.1:5434/testdb"

    local combinations=("n d n n" "y d y y" "y p $connection_string")

    for combo in "${combinations[@]}"; do
      if [[ "$combo" == *"m"* ]]; then
        reset_deployment_state
        run_mysql_docker
      elif [[ "$combo" == *"p"* ]]; then
        reset_deployment_state
        run_postgres_docker
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
