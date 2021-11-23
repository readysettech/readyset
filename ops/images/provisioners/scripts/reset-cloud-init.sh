#!/bin/bash

echo "Cleaning up cloud-init records for new AMI."
sudo rm -rf /var/lib/cloud/*
sudo cloud-init clean
echo "Clean up complete! The next AMI should launch userdata as expected."
