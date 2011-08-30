# !/bin/bash
echo "add exchange direct enstore.fcache"
qpid-config add exchange direct enstore.fcache # --durable

 echo "qpid-config add queue --durable policy_engine"
 qpid-config add queue --durable policy_engine
 echo "qpid-config add queue --durable migration_dispatcher"
 qpid-config add queue --durable migration_dispatcher
 echo "qpid-config add queue --durable migrator"
 qpid-config add queue --durable migrator
 echo "qpid-config add queue  file_clerk"
 qpid-config add queue  file_clerk

 echo "qpid-config bind  enstore.fcache migration_dispatcher"
 qpid-config bind  enstore.fcache migration_dispatcher
 echo "qpid-config bind  enstore.fcache policy_engine"
 qpid-config bind  enstore.fcache policy_engine
 echo "qpid-config bind  enstore.fcache migrator"
 qpid-config bind  enstore.fcache migrator

 echo "qpid-config add queue --durable pe_test"
 qpid-config add queue --durable pe_test
 echo "qpid-config bind  enstore.fcache pe_test"
 qpid-config bind  enstore.fcache pe_test
 echo "qpid-config add queue --durable migrator_reply"
 qpid-config add queue --durable migrator_reply
 echo "qpid-config bind  enstore.fcache migrator_reply"
 qpid-config bind  enstore.fcache migrator_reply

# more queues for tests :
host=`hostname --ip-address`
port=7710
 echo "qpid-config add queue udp_relay_test"
 qpid-config add queue udp_relay_test
 echo "qpid-config add queue udp2amq_${host}_${port}"
 qpid-config add queue udp2amq_${host}_${port}


