#!/bin/bash

sudo rm -rf /home/cephnvme/artifact/*
sudo ls -lta /home/cephnvme/artifact

sudo rm -rf /home/cephnvme/artifact.tar.gz
sudo ls -lta /home/cephnvme/

sudo cp -r /root/.ssh/atom_backup/artifact/multiIBMCloudServers_m6 /home/cephnvme/artifact
sudo ls -lta /home/cephnvme/artifact

sudo tar -czf /home/cephnvme/artifact.tar.gz -C /home/cephnvme/artifact .
sudo ls -lta /home/cephnvme/artifact
sudo ls -lta /home/cephnvme
sudo chmod +rx /home/cephnvme/artifact.tar.gz
sudo rm -rf /home/cephnvme/busyServer.txt
