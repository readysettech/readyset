#!/usr/bin/env bash

sudo docker pull 069491470376.dkr.ecr.us-east-2.amazonaws.com/noria-deploy:base
sudo docker tag 069491470376.dkr.ecr.us-east-2.amazonaws.com/noria-deploy:base readyset-base:latest 
sudo docker pull 069491470376.dkr.ecr.us-east-2.amazonaws.com/noria-deploy:deploy
sudo docker tag 069491470376.dkr.ecr.us-east-2.amazonaws.com/noria-deploy:deploy readyset-deploy:latest
