#!/usr/bin/env bash

sudo docker build . -f Dockerfile.base -t readyset-base:latest
sudo docker build . -f Dockerfile.deploy -t readyset-deploy:latest
