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
if [ -z $ENSTORE_INSTALL_DIR ] 
then
    install_dir=/tmp/enstore_istall
else
    install_dir=$ENSTORE_INSTALL_DIR
fi

if [ -n "${1:-}" ]; then
    node_list="${1:-}"
else
    node_list=$install_dir/nodes
fi

# run this script on the configuration node
rm -rf /tmp/enstore_nodes_done
rm -rf /tmp/enstore_nodes_failed

echo node list $node_list

(
tot_rc=0
cat $node_list | while read remote_node; do
    if [ $remote_node != $config_host ]; then
	echo "Copying rpms to $remote_node:$install_dir"
 	enrsh ${remote_node} "mkdir $install_dir"
	rc=$?
	if [ $rc -eq 0 ]; then
	    enrcp $install_dir/* ${remote_node}:$install_dir
	    rc=$?
	fi
	if [ $rc -eq 0 ]; then
	    echo "installing rpms"
	    ftt=`ls $install_dir/ftt*`
	    python=`ls $install_dir/Python*`
	    tcl=`ls $install_dir/tcl*`
	    tk=`ls $install_dir/tk*`
	    enstore=`ls $install_dir/enstore*`
	    # what to do about postgres?
	    enrsh ${remote_node} "rpm -Uvh $python"
	    rc=$?
	fi
	if [ $rc -eq 0 ]; then
	    enrsh ${remote_node} "rpm -Uvh $ftt"
	    rc=$?
	fi
	if [ $rc -eq 0 ]; then
	    enrsh ${remote_node} "rpm -Uvh $tcl"
	    rc=$?
	fi
	if [ $rc -eq 0 ]; then
	    enrsh ${remote_node} "rpm -Uvh $tk"
	    rc=$?
	fi
	if [ $rc -eq 0 ]; then
	    enrsh ${remote_node} "rpm -Uvh $enstore"
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
