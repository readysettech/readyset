#!/usr/bin/env bash

sudo docker tag readyset-base:latest 069491470376.dkr.ecr.us-east-2.amazonaws.com/noria-deploy:base
sudo docker push 069491470376.dkr.ecr.us-east-2.amazonaws.com/noria-deploy:base
sudo docker tag readyset-deploy:latest 069491470376.dkr.ecr.us-east-2.amazonaws.com/noria-deploy:deploy
sudo docker push 069491470376.dkr.ecr.us-east-2.amazonaws.com/noria-deploy:deploy
