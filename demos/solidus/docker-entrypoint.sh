#!/usr/bin/env bash

set -euo pipefail

export RAILS_ENV=production
export IN_DOCKER=1
export AUTO_ACCEPT=1

sleep 30

bundle exec rake db:migrate
if [ ! -f /state/loaded ]; then
    bundle exec rake db:seed
    bundle exec rake spree_sample:load
    touch /state/loaded
fi

USE_READYSET=1 bundle exec rails s -p 3000 -b '0.0.0.0'
