#!/bin/bash

sudo cp -r /root/.ssh/atom_backup/artifact/multiIBMCloudServers_m2 /tmp/artifact
sudo ls -lta /tmp/artifact
sudo chmod -R +rx /tmp/artifact
rm -rf /home/cephnvme/busyServer.txt
