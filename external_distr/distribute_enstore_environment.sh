#!/bin/sh
###############################################################################
#
# $Id$
#
###############################################################################
# this script is for distributing enstore ensvironment nodes described in the list
# it needs to be run on an enstore configuration server node

#set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-q" ] ; then export quiet=1; shift; else quiet=0; fi

if [ "`whoami`" != 'root' ]
then
    echo You need to run this script as user "root"
    exit 1
fi
if [ -z $ENSTORE_INSTALL_DIR ] 
then
    install_dir=/tmp/enstore_install
else
    install_dir=$ENSTORE_INSTALL_DIR
fi

if [ -n "${1:-}" ]; then
    node_list="${1:-}"
else
    node_list=$install_dir/nodes
fi

# run this script on the configuration node
rm -rf /tmp/enstore_nodes_done_env
rm -rf /tmp/enstore_nodes_failed_env

echo node list $node_list
this_host=`uname -n | cut -f1 -d\.`
echo this host $this_host

(
source /usr/local/etc/setups.sh
tot_rc=0
cat $node_list | while read remote_node; do
    rc=0
    if [ $remote_node != $this_host ]; then
	echo "Copying setup-enstore to $remote_node:$ENSTORE_HOME"
 	enrsh ${remote_node} "mkdir -p $ENSTORE_HOME/site_specific/config"
	enrcp $ENSTORE_HOME/site_specific/config/* ${remote_node}:$ENSTORE_HOME/site_specific/config
	rc=$?
	if [ $rc -eq 0 ]; then
	    echo "completing enstore environment distribution on $remote_node"
	    enrsh $remote_node "$ENSTORE_DIR/external_distr/create_enstore_environment.sh"
	    rc=$?
	fi

	if [ $rc -eq 0 ]; then
	    echo ${remote_node} >> /tmp/enstore_nodes_done
	    echo "Success on ${remote_node}"
	else
	    echo ${remote_node} >> /tmp/enstore_nodes_failed
	    echo "failure on ${remote_node}"
	fi
    fi
    tot_rc=`expr $tot_rc + $rc`
done
exit $tot_rc
)

if [ $? -ne 0 ]; then
echo "Installation on some nodes has failed. The nodes where installation has succeeded are in
/tmp/enstore_nodes_done
The nodes where installation has failed are in 
/tmp/enstore_nodes_failed
After fuguring out reasons for failure you can rerun this script with the argument 
that is a file containing all nodes where you want to install enstore.
Please do not use /tmp/enstore_nodes_failed. Rename it if you want to use it as the argument to $0
"
fi
exit $rc
