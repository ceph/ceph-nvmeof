#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: anita.shekar@ibm.com, sandy.kaur@ibm.com
#

MODULE := control
CONFIG ?= ceph-nvmeof.conf

setup: requirements.txt
	pip3 install -r requirements.txt

grpc:
	@mkdir -p $(MODULE)/generated
	@python3 -m grpc_tools.protoc \
			--proto_path=./proto \
			--python_out=./$(MODULE)/generated \
			--grpc_python_out=./$(MODULE)/generated \
			./proto/*.proto
	@sed -i 's/^import.*_pb2/from . \0/' ./$(MODULE)/generated/*.py

run:
	@python3 -m $(MODULE) -c $(CONFIG)

test:
	@pytest

clean:
	rm -rf .pytest_cache __pycache__
