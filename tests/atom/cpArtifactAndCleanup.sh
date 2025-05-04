#!/bin/bash

NIGHTLY=$1

process_artifacts() {
    local artifact_dir=$1
    local backup_dir=$2
    local tar_file=$3
    local busy_file=$4

    sudo rm -rf "${artifact_dir:?}"/*
    sudo ls -lta "$artifact_dir"

    sudo rm -rf "$tar_file"
    sudo ls -lta "$(dirname "$artifact_dir")"
    sudo cp -r "$backup_dir" "$artifact_dir"
    sudo ls -lta "$artifact_dir"

    sudo tar -czf "$tar_file" -C "$artifact_dir" .
    sudo ls -lta "$artifact_dir"
    sudo ls -lta "$(dirname "$artifact_dir")"
    sudo chmod +rx "$tar_file"
    sudo rm -rf "$busy_file"
}

if [ "$NIGHTLY" != "nightly" ]; then
    process_artifacts \
        "/home/cephnvme/artifact_m7" \
        "/root/.ssh/atom_backup/artifact/multiIBMCloudServers_m7" \
        "/home/cephnvme/artifact_m7.tar.gz" \
        "/home/cephnvme/busyServer.txt"
else
    process_artifacts \
        "/home/cephnvme/artifact_m8" \
        "/root/.ssh/atom_backup/artifact/multiIBMCloudServers_m8" \
        "/home/cephnvme/artifact_m8.tar.gz" \
        "/home/cephnvme/busyServerNightly.txt"
fi
