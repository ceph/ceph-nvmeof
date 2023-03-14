- [nvmeof-gateway](#nvmeof-gateway)
- [Initial configuration](#initial-configuration)
- [Docker build and usage](#docker-build-and-usage)
- [CLI Usage](#cli-usage)
- [mTLS Configuration for testing purposes](#mtls-configuration-for-testing-purposes)
- [Example NVMe volume access](#example-nvme-volume-access)


# nvmeof-gateway

Management gateway daemon to setup access to Ceph storage over NVMeoF

This daemon runs as root. It provides the ability to export existing RBD images as NVMeoF namespaces. Creation of RBD images is not within the scope of this daemon.


# Initial configuration

1. The daemon is a gRPC server, so the host running the server will need to install gRPC packages:

		$ make setup

2. Modify the config file (default ceph-nvmeof.conf) to reflect the IP/ Port where the server can be reached:

		addr = <IP address at which the client can reach the gateway>
		port = <port at which the client can reach the gateway>

3. To [enable mTLS](#mtls-configuration-for-testing-purposes) using self signed certificates, edit the config file to set:

		enable_auth = True  # Setting this to False will open an insecure port

4. Compile protobuf files for gRPC:

	    $ make grpc

5. SPDK v21.04 is included in this repository. Edit the config file to set:

		spdk_path = <complete path to SPDK parent directory>
		spdk_tgt = <relative path to SPDK target executable>

6. Setup SPDK

	Navigate to the spdk folder & install dependencies:

		$ ./scripts/pkgdep.sh

	Initialize configuration:

		$ apt install librbd-dev
		$ ./configure --with-rbd

	Build the SPDK app:

	    $ make

	SPDK requires hugepages to be set up:

		$ sh -c 'echo 4096 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages'

7. Start the gateway server daemon:

		$ python3 -m control [-c CONFIG]

# Docker build and usage

In order to build and run the gateway in a docker container, please follow these steps:

1. Follow steps #1 to #4 in section `Initial configuration`

2. Build a fedora based spdk container. This will compile spdk in a container with spdk repo tag.

		$ make spdk-image

1. Build the gateway container. Uses the spdk-image container for spdk.

		$ make gateway-image

2. Pushes the build gateway to the remote repository

		$ REMOTE_REPO=<remote repo location> make push-gateway-image

3. Running the gateway container
	* The host where the container runs needs to have a valid `ceph.conf` file

	* In this example, the `ceph.conf` file is located at `/etc/ceph` directory

	* Run the container as

	```
	docker run -it --network host \
		-v //dev//hugepages://dev//hugepages \
		-v /var/tmp:/var/tmp  \
		-v /etc/ceph:/etc/ceph \
		-v /dev/shm:/dev/shm \
		--privileged ceph-nvmeof:latest"
	```

# CLI Usage

The CLI tool can be used to initiate a connection to the gateway and run commands to configure the NVMe targets.

Run the tool with the -h flag to see a list of available commands:

	$ python3 -m control.cli -h
	usage: python3 -m control.cli [-h] [-c CONFIG]
			{create_bdev,delete_bdev,create_subsystem,delete_subsystem,add_namespace,remove_namespace,add_host,remove_host,create_listener,delete_listener,get_subsystems} ...

	CLI to manage NVMe gateways

	positional arguments:
	{create_bdev,delete_bdev,create_subsystem,delete_subsystem,add_namespace,remove_namespace,add_host,remove_host,create_listener,delete_listener,get_subsystems}

	optional arguments:
	-h, --help            			show this help message and exit
	-c CONFIG, --config CONFIG
			      			Path to config file

Example:

	$ python3 -m control.cli create_bdev -h
	usage: python3 -m control.cli create_bdev [-h] -i IMAGE -p POOL [-b BDEV_NAME] [-s BLOCK_SIZE]

	optional arguments:
	-h, --help            			show this help message and exit
	-i IMAGE, --image IMAGE
			      			RBD image name
	-p POOL, --pool POOL  			Ceph pool name
	-b BDEV_NAME, --bdev BDEV_NAME
						Bdev name
	-s BLOCK_SIZE, --block_size BLOCK_SIZE
						Block size

# mTLS Configuration for testing purposes

For testing purposes, self signed certificates and keys can be generated locally using OpenSSL.

For the server, generate credentials for server name 'my.server' in files called server.key and server.crt:

  	$ openssl req -x509 -newkey rsa:4096 -nodes -keyout server.key -out server.crt -days 3650 -subj '/CN=my.server'

For client:

  	$ openssl req -x509 -newkey rsa:4096 -nodes -keyout client.key -out client.crt -days 3650 -subj '/CN=client1'

Indicate the location of the keys and certificates in the config file:

	[mtls]

	server_key = ./server.key
	client_key = ./client.key
	server_cert = ./server.crt
	client_cert = ./client.crt

# Example NVMe volume access

1. Start the gateway server:

		$ python3 -m control
		INFO:root:SPDK PATH: /path/to/spdk
		INFO:root:Starting /path/to/spdk/tgt/nvmf_tgt all -u
		INFO:root:Attempting to initialize SPDK: server_addr: /var/tmp/spdk.sock, port: 5260, conn_retries: 3, timeout: 60.0
		INFO: Setting log level to ERROR
		INFO:JSONRPCClient(/var/tmp/spdk.sock):Setting log level to ERROR


2. Run the CLI (ensure a ceph pool 'rbd' with an rbdimage 'mytestdevimage' is created prior to this step):

		$ python3 -m control.cli create_bdev -i mytestdevimage -p rbd -b Ceph0
		INFO:root:Created bdev Ceph0: True

		$ python3 -m control.cli create_subsystem -n nqn.2016-06.io.spdk:cnode1 -s SPDK00000000000001
		INFO:root:Created subsystem nqn.2016-06.io.spdk:cnode1: True

		$ python3 -m control.cli add_namespace -n nqn.2016-06.io.spdk:cnode1 -b Ceph0
		INFO:root:Added namespace 1 to nqn.2016-06.io.spdk:cnode1: True
		
		$ python3 -m control.cli add_host -n nqn.2016-06.io.spdk:cnode1 -t '*'
		INFO:root:Allowed open host access to nqn.2016-06.io.spdk:cnode1: True

		** NOTE ** If running against the gateway container, add the following
			   -a "The ip address of the nvme client"
			   -g "The name of the gateway"
		$ python3 -m control.cli create_listener -n nqn.2016-06.io.spdk:cnode1 -s 5001 -a 10.22.64.1 -g gateway1
		-or- 
		$ python3 -m control.cli create_listener -n nqn.2016-06.io.spdk:cnode1 -s 5001
		INFO:root:Created nqn.2016-06.io.spdk:cnode1 listener: True

3. On the storage client system (ubuntu-21.04):

	- Install requisite packages

			$ apt install nvme-cli
			$ modprobe nvme-fabrics

	- Run nvme command to discover available subsystems

			$ nvme discover -t tcp -a 192.168.50.4 -s 5001

			Discovery Log Number of Records 1, Generation counter 6
			=====Discovery Log Entry 0======
			trtype:  tcp
			adrfam:  ipv4
			subtype: nvme subsystem
			treq:    not required
			portid:  0
			trsvcid: 5001
			subnqn:  nqn.2016-06.io.spdk:cnode1
			traddr:  192.168.50.4
			sectype: none

	- Connect to desired subsystem

			$ nvme connect -t tcp --traddr 192.168.50.4 -s 5001 -n nqn.2016-06.io.spdk:cnode1

	- List targets that are available

			$ nvme list
			Node             SN                   Model                                    Namespace Usage                      Format           FW Rev
			---------------- -------------------- ---------------------------------------- --------- -------------------------- ---------------- --------
			/dev/nvme0n1     SPDK00000000000001   SPDK bdev Controller                     1           6.44  GB /   6.44  GB      4 KiB +  0 B   21.04

	- Create a filesystem on the desired target

			$  mkfs /dev/nvme0n1

			mke2fs 1.45.7 (28-Jan-2021)
			Creating filesystem with 1572864 4k blocks and 393216 inodes
			Filesystem UUID: 1308f6ff-621b-4d17-b127-65eded31abe2
			Superblock backups stored on blocks:
				32768, 98304, 163840, 229376, 294912, 819200, 884736

			Allocating group tables: done
			Writing inode tables: done
			Writing superblocks and filesystem accounting information: done

	- Mount and use the storage volume

			$ mount /dev/nvme0n1 /mnt

			$ ls /mnt
			lost+found

			$ echo "NVMe volume" > /mnt/test.txt

			$ ls /mnt
			lost+found  test.txt

