# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import sys
import threading
import unittest
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, call, patch


def load_module(path, dependencies):
    # Load the real lifecycle code without starting inference or requiring generated RPC code.
    source = Path(__file__).parents[1] / "iotdb/ainode/core" / path
    spec = spec_from_file_location(f"shutdown_test_{source.stem}", source)
    module = module_from_spec(spec)
    with patch.dict(sys.modules, dependencies):
        spec.loader.exec_module(module)
    return module


class AINodeShutdownTest(unittest.TestCase):
    def setUp(self):
        dependencies = {
            name: Mock()
            for name in (
                "psutil",
                "iotdb.ainode.core.config",
                "iotdb.ainode.core.constant",
                "iotdb.ainode.core.log",
                "iotdb.ainode.core.rpc.client",
                "iotdb.ainode.core.rpc.handler",
                "iotdb.ainode.core.rpc.service",
                "iotdb.thrift.common.ttypes",
                "iotdb.thrift.confignode.ttypes",
            )
        }
        self.module = load_module("ai_node.py", dependencies)
        self.node = self.module.AINode()
        self.node._rpc_service = Mock()
        self.client = Mock()
        self.borrow_client = (
            self.module.ClientManager.return_value.borrow_config_node_client
        )
        self.borrow_client.return_value = self.client
        self.location = object()
        self.module._generate_configuration = Mock(
            return_value=SimpleNamespace(location=self.location)
        )

    def test_report_before_stopping_rpc_and_only_once(self):
        self.assertFalse(self.node.is_stopping())

        def report(location):
            self.assertTrue(self.node.is_stopping())
            self.node._rpc_service.stop.assert_not_called()

        self.client.report_shutdown.side_effect = report
        self.node.stop()
        self.node.stop()

        self.client.report_shutdown.assert_called_once_with(self.location)
        self.client.close.assert_called_once()
        self.node._rpc_service.stop.assert_called_once()
        self.borrow_client.assert_called_once_with(timeout_ms=2000)

    def test_report_failure_still_closes_client_and_stops_rpc(self):
        self.client.report_shutdown.side_effect = OSError("ConfigNode unavailable")

        self.node.stop()

        self.assertTrue(self.node.is_stopping())
        self.client.close.assert_called_once()
        self.node._rpc_service.stop.assert_called_once()

    def test_connection_failure_still_stops_rpc(self):
        self.borrow_client.side_effect = OSError("ConfigNode unavailable")

        self.node.stop()

        self.node._rpc_service.stop.assert_called_once()

    def test_concurrent_stop_does_not_close_rpc_while_report_is_in_progress(self):
        report_started = threading.Event()
        finish_report = threading.Event()

        def report(location):
            report_started.set()
            if not finish_report.wait(5):
                raise TimeoutError("Test did not release the shutdown report")

        self.client.report_shutdown.side_effect = report
        stopping = threading.Thread(target=self.node.stop)
        stopping.start()
        try:
            self.assertTrue(report_started.wait(5))
            self.node.stop()
            self.assertTrue(self.node.is_stopping())
            self.node._rpc_service.stop.assert_not_called()
        finally:
            finish_report.set()
            stopping.join(5)

        self.assertFalse(stopping.is_alive())
        self.client.report_shutdown.assert_called_once_with(self.location)
        self.node._rpc_service.stop.assert_called_once()

    def test_heartbeat_checks_stopping_after_load_sampling(self):
        dependencies = {
            name: Mock()
            for name in (
                "iotdb.ainode.core.constant",
                "iotdb.ainode.core.log",
                "iotdb.ainode.core.manager.cluster_manager",
                "iotdb.ainode.core.manager.device_manager",
                "iotdb.ainode.core.manager.inference_manager",
                "iotdb.ainode.core.manager.model_manager",
                "iotdb.ainode.core.rpc.status",
                "iotdb.thrift.ainode.ttypes",
                "iotdb.thrift.common.ttypes",
            )
        }
        dependencies["iotdb.thrift.ainode"] = SimpleNamespace(
            IAINodeRPCService=SimpleNamespace(Iface=object)
        )
        handler_module = load_module("rpc/handler.py", dependencies)
        handler = handler_module.AINodeRPCServiceHandler(self.node)
        response = SimpleNamespace(status="Running")
        heartbeat = handler_module.ClusterManager.get_heart_beat
        heartbeat.return_value = response
        self.assertEqual("Running", handler.getAIHeartbeat(Mock()).status)

        def sample_load(req):
            self.node._stop_event.set()
            return response

        heartbeat.side_effect = sample_load
        self.assertEqual("Stopped", handler.getAIHeartbeat(Mock()).status)


class AINodeRPCServiceShutdownTest(unittest.TestCase):
    def setUp(self):
        dependencies = {
            name: Mock()
            for name in (
                "thrift.protocol",
                "thrift.transport",
                "iotdb.ainode.core.config",
                "iotdb.ainode.core.log",
                "iotdb.ainode.core.rpc.handler",
                "iotdb.thrift.ainode",
            )
        }
        dependencies["thrift.server"] = SimpleNamespace(
            TServer=SimpleNamespace(TThreadPoolServer=object)
        )
        module = load_module("rpc/service.py", dependencies)
        module.AINodeDescriptor().get_config().get_ain_internal_ssl_enabled.return_value = (
            False
        )
        self.handler = Mock()
        with patch.object(module, "AINodeThreadPoolServer") as pool:
            self.service = module.AINodeRPCService(self.handler)
            self.pool = pool.return_value

    def test_handler_cleanup_finishes_before_listener_stops(self):
        self.handler.stop.side_effect = self.pool.stop.assert_not_called

        self.service.stop()
        self.service.stop()

        self.handler.stop.assert_called_once()
        self.pool.stop.assert_called_once()

    def test_handler_cleanup_failure_still_stops_listener(self):
        self.handler.stop.side_effect = RuntimeError("Inference cleanup failed")

        with self.assertRaisesRegex(RuntimeError, "Inference cleanup failed"):
            self.service.stop()

        self.pool.stop.assert_called_once()


class ConfigNodeShutdownClientTest(unittest.TestCase):
    def setUp(self):
        dependencies = {
            name: Mock()
            for name in (
                "thrift.protocol",
                "thrift.transport",
                "iotdb.ainode.core.config",
                "iotdb.ainode.core.constant",
                "iotdb.ainode.core.log",
                "iotdb.ainode.core.rpc.status",
                "iotdb.thrift.common.ttypes",
                "iotdb.thrift.confignode",
                "iotdb.thrift.confignode.ttypes",
            )
        }
        dependencies["thrift.Thrift"] = SimpleNamespace(TException=OSError)
        dependencies["thrift.transport"].TTransport.TException = OSError
        dependencies["thrift.transport"].TTransport.TTransportException = OSError
        dependencies["iotdb.ainode.core.constant"] = load_module(
            "constant.py", dependencies
        )
        dependencies["iotdb.ainode.core.rpc.status"] = load_module(
            "rpc/status.py", dependencies
        )
        dependencies["iotdb.ainode.core.util.decorator"] = load_module(
            "util/decorator.py", {}
        )
        self.module = load_module("rpc/client.py", dependencies)
        config = self.module.AINodeDescriptor().get_config()
        config.get_ain_internal_ssl_enabled.return_value = False
        config.get_ain_thrift_compression_enabled.return_value = False
        self.manager = self.module.ClientManager()

    def test_startup_remembers_config_node_endpoints_for_shutdown(self):
        for operation, rpc_name in (
            ("node_register", "registerAINode"),
            ("node_restart", "restartAINode"),
        ):
            with self.subTest(operation=operation), patch.object(
                self.module.ConfigNodeClient, "_try_to_connect"
            ):
                client = self.manager.borrow_config_node_client()
                endpoints = [object(), object()]
                client._client = Mock()
                getattr(client._client, rpc_name).return_value = SimpleNamespace(
                    status=SimpleNamespace(code=200),
                    aiNodeId=7,
                    configNodeList=[
                        SimpleNamespace(internalEndPoint=endpoint)
                        for endpoint in endpoints
                    ],
                )
                getattr(client, operation)("cluster", Mock(), Mock())

                shutdown_client = self.manager.borrow_config_node_client(
                    timeout_ms=2000
                )
                self.assertEqual(endpoints, shutdown_client._config_nodes)

    def test_shutdown_connects_to_known_node_when_seed_is_unavailable(self):
        seed = self.manager._config_node_endpoint
        available = object()
        self.manager.update_config_nodes([available])
        with patch.object(
            self.module.ConfigNodeClient, "_connect", side_effect=[OSError(), None]
        ) as connect:
            self.manager.borrow_config_node_client(timeout_ms=2000)

        self.assertEqual([call(seed), call(available)], connect.call_args_list)

    def test_shutdown_report_retries_after_redirection(self):
        with patch.object(self.module.ConfigNodeClient, "_try_to_connect"):
            client = self.manager.borrow_config_node_client(timeout_ms=2000)
        client._client = Mock()
        leader = object()
        client._client.reportAINodeShutdown.side_effect = [
            SimpleNamespace(code=400, redirectNode=leader),
            SimpleNamespace(code=200),
        ]
        client._wait_and_reconnect = Mock()
        location = object()

        client.report_shutdown(location)

        self.assertEqual(
            [call(location), call(location)],
            client._client.reportAINodeShutdown.call_args_list,
        )
        client._wait_and_reconnect.assert_called_once()
        self.assertIs(leader, client._config_leader)

    def test_shutdown_report_retries_transport_failure(self):
        with patch.object(self.module.ConfigNodeClient, "_try_to_connect"):
            client = self.manager.borrow_config_node_client(timeout_ms=2000)
        client._client = Mock()
        client._client.reportAINodeShutdown.side_effect = [
            OSError("Connection closed"),
            SimpleNamespace(code=200),
        ]
        client._wait_and_reconnect = Mock()
        location = object()

        client.report_shutdown(location)

        self.assertEqual(2, client._client.reportAINodeShutdown.call_count)
        client._wait_and_reconnect.assert_called_once()
        self.assertIsNone(client._config_leader)

    def test_shutdown_report_exhausts_retries_and_propagates_failure(self):
        with patch.object(self.module.ConfigNodeClient, "_try_to_connect"):
            client = self.manager.borrow_config_node_client(timeout_ms=2000)
        client._client = Mock()
        client._client.reportAINodeShutdown.side_effect = OSError("Connection closed")
        client._wait_and_reconnect = Mock()

        with self.assertRaisesRegex(OSError, "Fail to connect to any config node"):
            client.report_shutdown(object())

        self.assertEqual(
            client._RETRY_NUM, client._client.reportAINodeShutdown.call_count
        )
        self.assertEqual(client._RETRY_NUM - 1, client._wait_and_reconnect.call_count)

    def test_shutdown_report_rejects_failed_consensus_write(self):
        with patch.object(self.module.ConfigNodeClient, "_try_to_connect"):
            client = self.manager.borrow_config_node_client(timeout_ms=2000)
        client._client = Mock()
        client._client.reportAINodeShutdown.return_value = SimpleNamespace(
            code=500, message="Consensus write failed"
        )

        with self.assertRaisesRegex(RuntimeError, "Consensus write failed"):
            client.report_shutdown(object())

    def test_connect_sets_timeout_and_releases_transports(self):
        transport = self.module.TTransport.TFramedTransport.return_value
        transport.isOpen.return_value = False
        client = self.manager.borrow_config_node_client(timeout_ms=2000)
        self.module.TSocket.TSocket.return_value.setTimeout.assert_called_once_with(
            2000
        )
        transport.open.assert_called_once()

        replacement = Mock()
        self.module.TTransport.TFramedTransport.return_value = replacement
        client._connect(SimpleNamespace(ip="127.0.0.1", port=10710))
        transport.close.assert_called_once()
        client.close()
        client.close()
        replacement.close.assert_called_once()

    def test_failed_connection_closes_transport(self):
        transport = self.module.TTransport.TFramedTransport.return_value
        transport.isOpen.return_value = False
        transport.open.side_effect = OSError("Connection refused")

        with self.assertRaisesRegex(OSError, "Fail to connect to any config node"):
            self.manager.borrow_config_node_client(timeout_ms=2000)

        transport.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
