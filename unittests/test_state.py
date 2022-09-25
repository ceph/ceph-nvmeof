import unittest
from unittest import mock
from unittest.mock import Mock, MagicMock
from control.state import OmapGatewayState
from control.generated.gateway_pb2 import create_bdev_req
from control.generated.gateway_pb2 import add_namespace_req
from control.generated.gateway_pb2 import create_subsystem_req
from control.generated.gateway_pb2 import add_host_req
from control.generated.gateway_pb2 import create_listener_req
from google.protobuf import json_format
import rados
import pdb

class OmapPersistentConfigTester(unittest.TestCase):


    @mock.patch('control.state.rados')
    def test_init_pass(self, mock_rados):
        settingsMock = Mock()
        settingsMock.get.return_value = Mock()

        # Test succcess
        mock_rados.Rados.return_value.open_ioctx.return_value.set_omap.return_value=Mock()
        omap = OmapGatewayState(settingsMock)
        assert omap.version == 1

    @mock.patch('control.state.rados')
    def test_init_fail_1(self, mock_rados):
        settingsMock = Mock()
        settingsMock.get.return_value = Mock()

        omap = OmapGatewayState(settingsMock)
        mock_rados.WriteOpCtx.return_value.__enter__.return_value.new.side_effect = Exception()

        with self.assertRaises(Exception):
            omap = OmapGatewayState(settingsMock)

    @mock.patch('control.state.rados')
    def test_init_fail_2(self, mock_rados):
        settingsMock = Mock()
        settingsMock.get.return_value = Mock()

        mock_rados.WriteOpCtx.side_effect = Exception()
        with self.assertRaises(Exception):
            omap = OmapGatewayState(settingsMock)

    @mock.patch('control.state.rados')
    def test_init_fail_3(self, mock_rados):
        settingsMock = Mock()
        settingsMock.get.return_value = Mock()

        # Reset mock and test exception
        mock_rados.ObjectExists = rados.ObjectExists
        mock_rados.WriteOpCtx.side_effect = rados.ObjectExists()
        # with self.assertRaises(rados.ObjectExists):
        omap = OmapGatewayState(settingsMock)
        mock_rados.WriteOpCtx.return_value.__enter__.return_value.new.assert_not_called()

    @mock.patch('control.state.rados')
    def test_init_fail_4(self, mock_rados):
        settingsMock = Mock()
        settingsMock.get.return_value = Mock()

        # Test exception
        mock_rados.Rados.return_value.open_ioctx.return_value.set_omap.side_effect = Exception()
        with self.assertRaises(Exception):
            omap = OmapGatewayState(settingsMock)
        mock_rados.WriteOpCtx.return_value.__enter__.return_value.new.assert_called()

    @mock.patch('control.state.rados')
    def test_get_local_version_pass(self, mock_rados):
        settingsMock = Mock()
        omap = OmapGatewayState(settingsMock)
        assert omap.version == 1
        test_ver = omap.get_local_version()
        assert test_ver == 1 
    
    @mock.patch('control.state.rados')
    def test_get_local_version_fail(self, mock_rados):
        settingsMock = Mock()
        omap = OmapGatewayState(settingsMock)
        omap.set_local_version = Mock()
        omap.set_local_version(2)
        assert omap.version != 2
        test_ver = omap.get_local_version()
        assert test_ver == 1 

    @mock.patch('control.state.rados')
    def test_write_key_pass(self, mock_rados):
        settingsMock = Mock()
        settingsMock.get.return_value = "abc"

        # In debugger, print(write_op.omap_cmp.called) true in config.py
        mock_rados.WriteOpCtx.__enter__.return_value = Mock()
        mock_rados.WriteOpCtx.return_value.__enter__ = Mock()
        omap = OmapGatewayState(settingsMock)
        omap.ioctx = Mock()
        omap._add_key(Mock(),Mock())
        omap.ioctx.operate_write_op.assert_called_with(
                mock_rados.WriteOpCtx.return_value.__enter__.return_value, 'nvmeof.abc.state')
        assert omap.version == 2

    @mock.patch('control.state.rados')
    def test_write_key_fail(self, mock_rados):
        settingsMock = Mock()
        settingsMock.get.return_value = "abc"
        omap = OmapGatewayState(settingsMock)

        mock_rados.WriteOpCtx.side_effect = Exception()
        with self.assertRaises(Exception):
            omap._add_key(Mock(),Mock())
        assert omap.version == 1

    @mock.patch('control.state.rados')
    def test_delete_key_pass(self, mock_rados):
        settingsMock = Mock()
        omap = OmapGatewayState(settingsMock)
        omap.ioctx = Mock()
        omap._remove_key("delete")
        assert omap.ioctx.set_omap.call_count == 1
        assert omap.ioctx.remove_omap_keys.call_count == 1
        assert omap.version == 2

    @mock.patch('control.state.rados')
    def test_add_bdev_pass(self, mock_rados):
        settingsMock = Mock()
        omap = OmapGatewayState(settingsMock)
        omap._add_key= Mock()
        omap._add_key.return_value = True
        omap.add_bdev("test1", "test2")
        omap._add_key.assert_called_with("bdev_test1", "test2")

    @mock.patch('control.state.rados')
    def test_add_bdev_fail(self, mock_rados):
        settingsMock = Mock()
        omap = OmapGatewayState(settingsMock)
        # Test exception thrown
        settingsMock.reset_mock()
        omap = OmapGatewayState(settingsMock)
        omap._add_key= Mock()
        omap._add_key.side_effect = Exception()
        with self.assertRaises(Exception):
            omap.add_bdev("test1", "test2")

    @mock.patch('control.state.rados')
    def test_delete_bdev_pass(self, mock_rados):
        settingsMock = Mock()
        omap = OmapGatewayState(settingsMock)
        omap._remove_key= Mock()
        omap._remove_key.return_value = True
        omap.remove_bdev("test1")
        omap._remove_key.assert_called_with("bdev_test1")

    @mock.patch('control.state.rados')
    def test_delete_bdev_fail(self, mock_rados):
        settingsMock = Mock()
        omap = OmapGatewayState(settingsMock)

        omap = OmapGatewayState(settingsMock)
        mock_rados.WriteOpCtx.return_value.__enter__.return_value.omap_cmp.side_effect = Exception()

        with self.assertRaises(Exception):
            omap.remove_bdev("test1")

    @mock.patch('control.state.rados')
    def test_add_namespace(self, mock_rados):
        settingsMock = Mock()
        omap = OmapGatewayState(settingsMock)
        omap._add_key = Mock()
        omap.add_namespace("namespace", "nsid", "value")
        omap._add_key.assert_called_with("namespace_namespace_nsid", "value")

    @mock.patch('control.state.rados')
    def test_add_namespace_fail(self, mock_rados):
        settingsMock = Mock()
        omap = OmapGatewayState(settingsMock)

        mock_rados.WriteOpCtx.return_value.__enter__.return_value.omap_cmp.side_effect = Exception()
        with self.assertRaises(Exception):
            omap.add_namespace("namespace", "nsid", "value")

    @mock.patch('control.state.rados')
    def test_remove_namespace_pass(self, mock_rados):
        settingsMock = Mock()
        omap = OmapGatewayState(settingsMock)
        omap._remove_key = Mock()
        omap.remove_namespace("namespace", "nsid")
        omap._remove_key.assert_called_with("namespace_namespace_nsid")

    @mock.patch('control.state.rados')
    def test_remove_namespace_fail(self, mock_rados):
        settingsMock = Mock()
        # Reset mock and test exception
        omap = OmapGatewayState(settingsMock)
        mock_rados.WriteOpCtx.return_value.__enter__.return_value.omap_cmp.side_effect = Exception()
        with self.assertRaises(Exception):
            omap.remove_namespace("namespace", "nsid")
            omap._remove_key.assert_called_with("namespace_namespace_nsid")

    @mock.patch('control.state.rados')
    def test_add_subsystem_pass(self, mock_rados):
        settingsMock = Mock()
        omap = OmapGatewayState(settingsMock)
        omap._add_key = Mock()
        omap.add_subsystem("namespace", "nsid")
        omap._add_key.assert_called_with("subsystem_namespace", "nsid")

    @mock.patch('control.state.rados')
    def test_add_subsystem_fail(self, mock_rados):
        settingsMock = Mock()
        omap = OmapGatewayState(settingsMock)
        # Reset mock and test exception

        omap = OmapGatewayState(settingsMock)
        mock_rados.WriteOpCtx.return_value.__enter__.return_value.omap_cmp.side_effect = Exception()
        with self.assertRaises(Exception):
            omap.delete_subsystem("namespace", "nsid")
            omap._add_key.assert_called_with("subsystem_namespace", "nsid")

    @mock.patch('control.state.rados')
    def test_add_host_pass(self, mock_rados):
        settingsMock = Mock()
        omap = OmapGatewayState(settingsMock)
        omap._add_key = Mock()
        omap.add_host("subsystem", "host_nqn", "value")
        omap._add_key.assert_called_with("host_subsystem_host_nqn", "value")

    @mock.patch('control.state.rados')
    def test_add_host_fail(self, mock_rados):
        settingsMock = Mock()
        omap = OmapGatewayState(settingsMock)
        # Reset mock and test exception

        omap = OmapGatewayState(settingsMock)
        mock_rados.WriteOpCtx.return_value.__enter__.return_value.omap_cmp.side_effect = Exception()
        with self.assertRaises(Exception):
            omap.add_host("subsystem", "host_nqn", "value")
            omap._add_key.assert_called_with("host_subsystem_host_nqn", "value")

    @mock.patch('control.state.rados')
    def test_remove_host_pass(self, mock_rados):
        settingsMock = Mock()
        omap = OmapGatewayState(settingsMock)
        omap._remove_key = Mock()
        omap.remove_host("subsystem", "host_nqn")
        omap._remove_key.assert_called_with("host_subsystem_host_nqn")


    @mock.patch('control.state.rados')
    def test_add_listener(self, mock_rados):
        settingsMock = Mock()
        omap = OmapGatewayState(settingsMock)
        omap._add_key = Mock()
        omap.add_listener("nqn", "gateway", "trtype", "traddr", "trsvcid", "val")
        omap._add_key.assert_called_with("listener_gateway_nqn_trtype_traddr_trsvcid", "val")

    @mock.patch('control.state.rados')
    def test_add_listener_fail(self, mock_rados):
        settingsMock = Mock()
        omap = OmapGatewayState(settingsMock)
        # Reset mock and test exception

        omap = OmapGatewayState(settingsMock)
        mock_rados.WriteOpCtx.return_value.__enter__.return_value.omap_cmp.side_effect = Exception()
        with self.assertRaises(Exception):
            omap.add_listener("nqn", "gateway", "trtype", "traddr", "trsvcid", "val")


    @mock.patch('control.state.rados')
    def test_remove_listener(self, mock_rados):
        settingsMock = Mock()
        omap = OmapGatewayState(settingsMock)
        omap._remove_key = Mock()
        omap.remove_listener("nqn", "gateway", "trtype", "traddr", "trsvcid")
        omap._remove_key.assert_called_with("listener_gateway_nqn_trtype_traddr_trsvcid")
