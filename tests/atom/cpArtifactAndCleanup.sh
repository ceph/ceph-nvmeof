#!/bin/bash

RUNNER_PASS=$1

echo $RUNNER_PASS | sudo -S cp -r /root/.ssh/atom_backup/artifact /tmp/
sudo ls -lta /tmp/artifact
sudo chmod -R +rx /tmp/artifact
rm -rf /home/cephnvme/busyServer.txt