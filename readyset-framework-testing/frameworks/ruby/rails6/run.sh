#!/bin/sh -ex
set -o pipefail

# TODO:  Validate that each step produces data that we expect, instead of just
#    displaying it to the user and failing if the server rejects a query

rake db:migrate
rake db:seed
rake user:list
rake 'user:get[1]'
rake user:delete_first
rake user:list | head
rake user:create
rake 'user:get[101]'
rake 'user:set_name[101, "User Name"]'
rake 'user:get[101]'
rake user:create_fail || true
rake user:list | tail

./ActiveRecordMigrations.sh
./ActiveRecordValidations.sh
./ActiveRecordCallbacks.sh
./ActiveRecordAssociations.sh
./ActiveRecordQueryInterface.sh
