import pytest

import json
import requests
import socket

endpoint="http://127.0.0.1:8001"
image = "iscsidevimage"
pool = "rbd"
bdev_name = "Ceph0"
subsystem_nqn = "nqn.2016-06.io.spdk:cnode1"
serial = "SPDK00000000000001"
host_list = ["nqn.2016-06.io.spdk:host1", "*"]
nsid = None
trtype = "TCP"
gateway_name = socket.gethostname()
addr = "127.0.0.1"
listener_list = [
    {"gateway_name" : gateway_name, "traddr" : addr, "trsvcid":"5001"},
    {"trsvcid":"5002"},
]


def get(path):
    res = requests.get(url=f"{endpoint}{path}")
    assert res.ok
    return json.loads(res.text)

def post(path, data):
    res = requests.post(url=f"{endpoint}{path}",
                        data=json.dumps(data),
                        headers={'Content-Type': 'application/json'})
    assert res.ok
    return json.loads(res.text)

def delete(path, data=None):
    res = requests.delete(url=f"{endpoint}{path}",
                          data=json.dumps(data),
                          headers={'Content-Type': 'application/json'})
    assert res.ok
    return json.loads(res.text)

class TestCreate:
    def test_create_bdev(self, caplog):
        post("/bdevs", {"name" : bdev_name, "pool" : pool, "image": image})

    def test_create_subsystem(self, caplog):
        post("/subsystems", {"nqn" : subsystem_nqn, "serial" : serial})

    def test_create_namespace(self, caplog):
        global nsid
        namespace = post(f"/subsystems/{subsystem_nqn}/namespaces",
                         {"bdev_name" : bdev_name})
        assert namespace['nsid']
        nsid = namespace['nsid']

    @pytest.mark.parametrize("host_nqn", host_list)
    def test_add_host(self, caplog, host_nqn):
        post(f"/subsystems/{subsystem_nqn}/hosts", {"nqn" : host_nqn})

    @pytest.mark.parametrize("listener", listener_list)
    def test_create_listener(self, caplog, listener):
        post(f"/subsystems/{subsystem_nqn}/listeners", listener)

class TestGet:
    def test_get_bdevs(self, caplog):
        bdevs = get("/bdevs")
        # TODO: assert bdevs

    def test_get_bdev(self, caplog):
        bdev = get(f"/bdevs/{bdev_name}")
        assert bdev

    def test_get_subsystems(self, caplog):
        subsystems = get("/subsystems")
        assert subsystems

    def test_get_subsystem(self, caplog):
        subsystem = get(f"/subsystems/{subsystem_nqn}")
        assert subsystem

    def test_get_namespaces(self, caplog):
        namespaces = get(f"/subsystems/{subsystem_nqn}/namespaces")
        assert namespaces

    def test_get_namespace(self, caplog):
        assert nsid
        namespace = get(f"/subsystems/{subsystem_nqn}/namespaces/{nsid}")
        assert namespace

    def test_get_hosts(self, caplog):
        hosts = get(f"/subsystems/{subsystem_nqn}/hosts")
        assert hosts

    @pytest.mark.parametrize("host_nqn", host_list)
    def test_get_host(self, caplog, host_nqn):
        host = get(f"/subsystems/{subsystem_nqn}/hosts/{host_nqn}")
        assert host

    def test_get_listeners(self, caplog):
        listeners = get(f"/subsystems/{subsystem_nqn}/listeners")
        assert listeners

class TestDelete:
    @pytest.mark.parametrize("host_nqn", host_list)
    def test_delete_host(self, caplog, host_nqn):
        delete(f"/subsystems/{subsystem_nqn}/hosts/{host_nqn}")

    @pytest.mark.parametrize("listener", listener_list)
    def test_delete_listener(self, caplog, listener):
        delete(f"/subsystems/{subsystem_nqn}/listeners", listener)

    def test_delete_namespace(self, caplog):
        delete(f"/subsystems/{subsystem_nqn}/namespaces/{nsid}")

    def test_delete_bdev(self, caplog):
        delete(f"/bdevs/{bdev_name}")

    def test_delete_subsystem(self, caplog):
        delete(f"/subsystems/{subsystem_nqn}")
