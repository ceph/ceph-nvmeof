REPO := github.com/ceph/ceph-nvmeof
CONTAINER := ceph-nvmeof:lib-go

proto: .container-id
	podman run --rm -v $(CURDIR)/../..:/go/src/$(REPO):Z --workdir=/go/src/github.com/ceph/ceph-nvmeof/lib/go/nvmeof $(CONTAINER) go generate


.PHONY: proto regenerate clean

.container-id:
	podman build -t $(CONTAINER) -f Containerfile .
	podman inspect -f {{.Id}} $(CONTAINER) > .container-id

regenerate:
	$(MAKE) clean
	$(RM) nvmeof/gateway_grpc.pb.go nvmeof/gateway.pb.go
	$(MAKE) proto

clean:
	test -f .container-id && podman rmi $(CONTAINER) || true
	$(RM) .container-id
