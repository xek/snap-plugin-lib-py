#!/usr/bin/env python

# http://www.apache.org/licenses/LICENSE-2.0.txt
#
# Copyright 2016 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from threading import Lock
import time

from oslo_config import cfg
import oslo_messaging as messaging
import snap_plugin.v1 as snap

LOG = logging.getLogger(__name__)

METRICS = {} # single operations on dict are thread safe

class NotificationHandler(object):
    def info(self, ctxt, publisher_id, event_type, payload, metadata):
        LOG.info('Handled')
        METRICS[payload['node_uuid']] = payload['payload']
        # other available fields: 'event_type', 'timestamp', 'node_uuid'
        return messaging.NotificationResult.HANDLED


class Cups(snap.Collector):
    """Ironic CUPS collector

    /intel/node_manager/cups/cpu_cstate uint16 CUPS CPU Bandwidth
    /intel/node_manager/cups/io_bandwith uint16 CUPS I/O Bandwidth
    /intel/node_manager/cups/memory_bandwith uint16 CUPS Memory Bandwidth
    """

    init_lock = Lock()
    oslo_listener = None
    queue = None

    def collect(self, metrics):
        LOG.debug("CollectMetrics called")
        metrics = self.queue.get()
        for metric in metrics:
            switch = {
                'cpu_cstate': 'CPU_Utilization',
                'io_bandwith': 'IO_Utilization',
                'memory_bandwith': 'Mem_Utilization',
            }
            type = metric.namespace[4].value
            uuid = metric.namespace[5].value # FIXME: is this right?
            metric.data = METRICS[uuid][switch[type]]
            metric.timestamp = time.time() # or METRICS[uuid]['timestamp']
        return metrics

    def update_catalog(self, config):
        LOG.debug("GetMetricTypes called")
        while self.init_lock:
            if not self.oslo_listener:
                LOG.debug("Starting oslo.messaging listener")
                self.oslo_listener = messaging.get_notification_listener(
                    messaging.get_transport(
                        cfg.CONF, config["transport_url"]),
                    [messaging.Target(topic='notifications')],
                    [NotificationHandler()], 'threading', allow_requeue=True)
                self.oslo_listener.start()

        metrics = []
        for key in ("cpu_cstate", "io_bandwith", "memory_bandwith"):
            metric = snap.Metric(
                namespace=[
                    snap.NamespaceElement(value="intel"),
                    snap.NamespaceElement(value="node_manager"),
                    snap.NamespaceElement(value="cups"),
                    snap.NamespaceElement(value=key)
                ],
                version=1,
                tags={"mtype": "gauge"},
                description="CUPS {}".format(key.replace('_', ' ')),
            )
            metric.namespace.add_dynamic_element("uuid", "node UUID")
            metrics.append(metric)

        return metrics

    def get_config_policy(self):
        LOG.debug("GetConfigPolicy called")
        return snap.ConfigPolicy(
            [
                ("ironic-cups"),
                [
                    (
                        "transport_url",
                        snap.StringRule(
                            default='rabbit://openstack:password@rabbit:5672/',
                            required=True)
                    ),
                ]
            ]
        )


if __name__ == "__main__":
    Cups("ironic-cups", 1).start_plugin()
