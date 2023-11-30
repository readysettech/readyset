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

# Suppress banner since it is fairly noisy
sed '/^print_banner$/s/^/#/' "$DEMO_SCRIPT" > "$DEMO_SCRIPT_TMP"
# rename localhost to work inside a docker container
# If running locally on a mac, use this line to switch 127.0.0.1 with host.docker.internal.
# sed 's/HOST="127.0.0.1"/HOST=host.docker.internal/' "$DEMO_SCRIPT_TMP" > "$DEMO_SCRIPT"
# Use 172.17.0.1 for docker-in-docker on linux
sed 's/HOST="127.0.0.1"/HOST=172.17.0.1/' "$DEMO_SCRIPT_TMP" > "$DEMO_SCRIPT"

# Figure out how many times we need to press enter to run through the entire psql
# interactive section of the demo.
N_ENTERS=$(grep -c "Press enter" "$DEMO_SCRIPT_TMP")

# Function to run the readyset demo automatically
# Importing the sample data can take a few minutes, so there is a pretty long timeout.
run_script() {
    expect -d -c "
        spawn bash $DEMO_SCRIPT

        set timeout 8
        send_user -- \"--- Testing colorful terminal prompt \n\"
        expect -re \"Do you like colorful terminal output.*:\" {
          send \"$1\r\"
        }
        if {\"$1\" == \"y\"} {
          expect -re \"Good choice!\"
        } else {
          expect -re \"Very well.\"
        }

        send_user -- \"--- Testing Sample data detection \n\"
        expect -re \".*Downloading the ReadySet Docker Compose file.*\" {}
        expect -re \".*Running the ReadySet Docker Compose setup.*\" {}
        expect -re \".*ReadySet Docker Compose setup complete.*\" {}
        expect -re \".*Checking if sample data is already loaded.*\" {}

        set timeout 30
        expect {
          -re \".*Sample data detected!.*\" {}
          -re \".*No Sample data detected.*\" {}
          timeout {
            send_user -- \"FAIL: Timed out waiting for sample data check.\n\"
            exit 1
          }
        }

        send_user -- \"--- Testing import of sample data \n\"
        expect -re \".*Import sample data.*\" {
          send \"$2\r\"
          if {\"$2\" == \"y\"} {
            # Sample data takes a while to load.
            set timeout 500
            expect -re \".*Sample data imported successfully.*\" {}
            set timeout 8
          }
        }

        send_user -- \"--- Testing Explore sample data prompt \n\"
        if {\"$2\" == \"y\"} {
          expect -re \".*Explore sample data in psql.*\" {
            send \"$2\r\"
          }
        }

        send_user -- \"--- Testing interactive psql section \n\"
        set timeout 8
        if {\"$3\" == \"y\"} {
            for {set i 0} {\$i < $N_ENTERS} {incr i} {
                expect {
                  -re \".*Press enter.*\" {
                      send \"\r\"
                    }
                  -re \".*error:,*\" {
                    send_user -- psql error encountered
                    exit 1
                  }
                }
            }
        }

        send_user -- \"--- Testing conclusion \n\"
        expect -re \".*testdb\" {
          send \"exit\r\n\"
        }

        expect {
          -re \"Join us on slack:\" { exit 0 }
          timeout { exit 1}
        }

        exit 1
    "
}

test_combination() {
    combo=$1;
    section=$2

    echo -e "Testing combination (colorful input?, import sample data? explore?) ${combo}"
    read -ra answers <<< "$combo"
    if ! run_script "${answers[0]}" "${answers[1]}" "${answers[2]}"; then
        echo "Test failed for combination: $combo $section"
        exit 1
    else
        echo "Test passed for combination: $combo $section"
    fi
}

# If testing locally, it's convenient to automatically reset the initial state by
# stopping the containers and removing the associated volumes.
reset_docker_compose() {
  if [ -f "readyset.compose.yml" ]; then
    echo "'readyset.compose.yml' found. Running 'docker-compose down' to reset the environment."
    docker-compose -f readyset.compose.yml down -v > /dev/null 2>&1
  else
    echo "'readyset.compose.yml' not found. Proceeding with the tests."
  fi
}

################################################################################
# Test different combinations of answers to branching prompts.
#
# Prompts:
# 1. Color ouput? (always provided)
# (2. Import sample data? (only valid if data is not already loaded))
# 3. Explore sample data? (only provided if data is loaded)
################################################################################
test_all_combinations() {
    reset_docker_compose

    local combinations=("n n n" "y y y")

    for combo in "${combinations[@]}"; do
      test_combination "${combo}" "before sample data loaded"
    done
}

# Make sure 'expect' is installed
if ! which expect > /dev/null; then
  exit 1
fi

test_all_combinations
