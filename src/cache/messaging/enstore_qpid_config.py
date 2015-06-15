#!/usr/bin/env python
"""
Create and configure all qpid exchanges and queues used
in Enstore Small File Aggregation environment.
"""

import os

import configuration_client
import cache.messaging.client as cmc

MY_NAME = "QPID_CFG"
RECONNECT_INTERVAL = 10


class QPIDConfigurator:
    def __init__(self):
        self.csc = configuration_client.ConfigurationClient((os.environ['ENSTORE_CONFIG_HOST'],
                                                        int(os.environ['ENSTORE_CONFIG_PORT'])))
        #Trace.init(MY_NAME)
        self.queues = []
        # get configuration parameters
        broker_config = self.csc.get('amqp_broker')
        broker_host = broker_config.get('host')
        broker_port = broker_config.get('port')
        dispatcher_config = self.csc.get('dispatcher')
        self.migrator_list = self.csc.get_migrators2()
        self.clustered_configuration = dispatcher_config.get("clustered_configuration")

        for migrator in self.migrator_list:
            # Work request exchange and queue
            work_exchange = migrator.get('migrator_work')
            if self.clustered_configuration:
                work_queue_key = "_".join((work_exchange, migrator.get('disk_library')))
                work_queue = work_queue_key
                # create exchange
                command = "%s; {create:always,node:{type:topic,x-declare:{type:direct}}}"%(work_exchange,)
                if not command in self.queues:
                    self.queues.append(command)

                # create migrator incoming request queue and bind it to exchange
                command = "%s; {create: always, node:{x-bindings:[{exchange:'%s', queue:'%s',key:'%s'}]}}"%(work_queue, work_exchange, work_queue, work_queue_key)
                if not command in self.queues:
                    self.queues.append(command)

            else:
                command = "%s; {create: always}"%(work_exchange,)
                self.session.queue_declare(command, durable=True)
                if not command in self.queues:
                    self.queues.append(command)
            # migrator input control queue
            queue_in_name = migrator['name'].split('.')[0]
            command = "%s; {create: always}"%(queue_in_name,)
            self.queues.append(command)

        # dispatcher input control queue
        command = "%s; {create: always}"%("dispatcher",)
        self.queues.append(command)

        # dispatcher input events queue (from file clerk)
        command = "%s; {create: always}"%(dispatcher_config.get('queue_work'),)
        self.queues.append(command)

        # dispatcher reply queue
        command = "%s; {create: always}"%(dispatcher_config.get('queue_reply'),)
        self.queues.append(command)

        broker = (broker_config['host'], broker_config['port'])
        for queue in self.queues:
            rc = cmc.EnQpidClient(broker, myaddr=None, target=queue)
            if rc:
                rc.start()
                rc.stop()

if __name__ == "__main__":
    qpid_conf = QPIDConfigurator()
