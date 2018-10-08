#!/bin/bash
dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
docker_image="hotcrp_v11_querylogging"
out_dir=${dir}/paper_queries

hotcrp_host="127.0.0.1"
hotcrp_port="8080"
hotcrp_base_url="${hotcrp_host}:${hotcrp_port}/testconf"
hotcrp_user="malte%40csail.mit.edu"
hotcrp_repo="https://github.com/kohler/hotcrp.git"

mkdir -p ${out_dir}

rm -rf /tmp/hotcrp
git clone ${hotcrp_repo} /tmp/hotcrp
cd /tmp/hotcrp

# start up HotCRP container
container_name=$(docker run -d -p 8080:80 ${docker_image} /bin/sleep 10d)

# log all queries
docker exec ${container_name} /bin/bash -c "echo \"log = /var/log/mysql/queries.log\" >> /etc/mysql/my.cnf"

# start up web server and DB
docker exec ${container_name} /bin/bash -c "service mysql start; service apache2 start"

# move to first version of interest
docker exec ${container_name} /bin/bash -c "cd /root/hotcrp; git checkout b0054f80"

sleep 1

# get login cookie -- remains valid across all revisions
wget --save-cookies cookies.txt \
     --keep-session-cookies \
     --delete-after \
     "http://${hotcrp_base_url}/?cookie=1&email=${hotcrp_user}&password=test&action=login&signin=go"

go=0
for l in $(git log --abbrev=8 --oneline --follow src/schema.sql | cut -d' ' -f1 | tac); do
  # start upgrading at b0054f80185d624597d5bbbec0f2eafc73afe69b, which is the first version to have
  # an auto-upgrader; this corresponds to schema version 11
  if [[ ${go} == 0 && ${l} == "b0054f80" ]]; then
    go=1
  elif [[ ${go} == 0 ]]; then
    continue
  fi

  # go == 1 at this point, so we're beyond v11
  echo "Upgrading to git revision $l"
  docker exec ${container_name} /bin/bash -c "cd /root/hotcrp; cp Code/options.inc Code/options.bak; git reset --hard; git checkout ${l}; cp Code/options.bak Code/options.inc"

  # also move to the same revision locally to find the schema version
  git checkout ${l}
  schema_file=$(find . -name schema.sql)
  res=$(grep "values ('allowPaperOption" ${schema_file})
  cur_schema_ver=$(echo ${res} | cut -d' ' -f 8 | sed -e "s/);//" | tr -d [:space:])

  # log in
  wget --load-cookies cookies.txt \
       --delete-after \
       "http://${hotcrp_base_url}/?cookie=1&email=${hotcrp_user}&password=test&action=login&signin=go"

  # paper list
  docker exec ${container_name} /bin/bash -c "echo \"############# CHAIR PAPER LIST @ ${l} #############\" >> /var/log/mysql/queries.log"
  wget --load-cookies cookies.txt \
       --delete-after \
       "http://${hotcrp_base_url}/search.php?q=&s=t"

  # grab the queries
  docker cp ${container_name}:/var/log/mysql/queries.log ${out_dir}/hotcrp_v${cur_schema_ver}_${l}.log

  # clean up
  docker exec ${container_name} /bin/bash -c "service mysql stop; rm /var/log/mysql/queries.log; service mysql start"
done

# get rid of the container
docker kill ${container_name}
docker rm ${container_name}
