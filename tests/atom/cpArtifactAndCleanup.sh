#!/bin/bash

sudo rm -rf /tmp/artifact/multiIBMCloudServers_m2
sudo cp -r /root/.ssh/atom_backup/artifact/multiIBMCloudServers_m2 /tmp/artifact
sudo ls -lta /tmp/artifact
sudo chmod -R +rx /tmp/artifact
rm -rf /home/cephnvme/busyServer.txt
