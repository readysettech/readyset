#!/usr/bin/env bash

echo -n "Enter docker password: "
read -s password
echo $password | sudo docker login --username AWS --password-stdin 069491470376.dkr.ecr.us-east-2.amazonaws.com
