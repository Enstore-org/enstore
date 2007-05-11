#!/bin/sh
###############################################################################
#
# $Id$
#
###############################################################################
# this script is for distributing enstore product to all nodes described in the common enstore farmlet
# it needs to be run on an enstore configuration server node

#set -u  # force better programming and ability to use check for not set
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-q" ] ; then export quiet=1; shift; else quiet=0; fi

if [ "`whoami`" != 'root' ]
then
    echo You need to run this script as user "root"
    exit 1
fi
source /usr/local/etc/setups.sh
if [ -n "${1:-}" ]; then
    node_list="${1:-}"
else
    node_list=$FARMLETS_DIR/enstore
fi

if [ $? -ne 0 ]
then 
    echo setup failed
    exit 1
fi

# run this script on the configuration node
PATH=/usr/sbin:$PATH
config_host=`echo $ENSTORE_CONFIG_HOST | cut -f1 -d\.`
this_host=`uname -n | cut -f1 -d\.`

rm -rf /tmp/enstore_nodes_done
rm -rf /tmp/enstore_nodes_failed

echo $config_host $this_host

if [ $this_host != $config_host ]; then
    echo "You must run this script on configuration host"
    exit 1
fi

echo node list $node_list
# Prepare distribution packages
tar -Pczf /tmp/enstore_python.tgz ${PYTHON_DIR}
tar -Pczf /tmp/enstore.tgz ${ENSTORE_DIR}
tar -Pczf /tmp/enstore_ftt.tgz ${FTT_DIR}
tar -Pczf /tmp/enstore_farmlets.tgz ${FARMLETS_DIR}

(
tot_rc=0
cat $node_list | while read remote_node; do
    if [ $remote_node != $config_host ]; then
	echo "Copying python to $remote_node"
	enrcp /tmp/enstore_python.tgz ${remote_node}:/tmp
	rc=$?
	if [ $rc -eq 0 ]; then
	    enrsh ${remote_node} "tar -Pxzf /tmp/enstore_python.tgz"
 	    rc=$?
	fi
	if [ $rc -eq 0 ]; then
	    # check if this is a mover node
	    grep ${remote_node} $FARMLETS_DIR/movers
	    if [ $? -eq 0 ]; then
		echo "Copying ftt to $remote_node"
		enrcp /tmp/enstore_ftt.tgz ${remote_node}:/tmp
		rc=$?
		if [ $rc -eq 0 ]; then
		    enrsh ${remote_node} "tar -Pxzf /tmp/enstore_ftt.tgz"
		    rc=$?
		fi
	    fi
	fi
	if [ $rc -eq 0 ]; then
	    echo "Copying enstore to $remote_node"
	    enrcp /tmp/enstore.tgz ${remote_node}:/tmp
	    rc=$?
	    if [ $? -eq 0 ]; then
		enrsh ${remote_node} "tar -Pxzf /tmp/enstore.tgz"
		rc=$?
	    fi
	fi

	if [ $rc -eq 0 ]; then
	    echo "Copying farmlets to $remote_node"
	    enrcp /tmp/enstore_farmlets.tgz ${remote_node}:/tmp
	    rc=$?
	    if [ $? -eq 0 ]; then
		enrsh ${remote_node} "tar -Pxzf /tmp/enstore_farmlets.tgz"
		rc=$?
	    fi
	fi

	if [ $rc -eq 0 ]; then
	    echo "creating enstore account"
	    enrsh ${remote_node} "$ENSTORE_DIR/external_distr/rpm_preinstall.sh"
	    #rc=$?
	fi

	if [ $rc -eq 0 ]; then
	    echo "copying setups.sh"
	    enrsh ${remote_node} "if [ ! -d '/usr/local/etc' ]; then mkdir -p /usr/local/etc; fi; rm -rf /usr/local/etc/setups.sh" 
	    enrcp /usr/local/etc/setups.sh  ${remote_node}:/usr/local/etc
	    rc=$?
	fi

	if [ $rc -eq 0 ]; then
	    echo "Installing sudoers, and boot scripts"
	    enrsh ${remote_node} "$ENSTORE_DIR/external_distr/rpm_postinstall.sh"
	    rc=$?
	fi
	    
	if [ $rc -eq 0 ]; then
	    echo ${remote_node} >> /tmp/enstore_nodes_done
	else
	    echo ${remote_node} >> /tmp/enstore_nodes_failed
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
