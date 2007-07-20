#!/bin/sh
###############################################################################
#
# $Id$
#
###############################################################################
# this script is for distributing enstore product to all nodes described in the list
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
rm -rf /tmp/enstore_nodes_done
rm -rf /tmp/enstore_nodes_failed

echo node list $node_list
this_host=`uname -n | cut -f1 -d\.`
echo this host $this_host

(
tot_rc=0
cat $node_list | while read remote_node; do
    rc=0
    if [ $remote_node != $this_host ]; then
	echo "Copying rpms to $remote_node:$install_dir"
 	enrsh ${remote_node} "if [ ! -d $install_dir ];then mkdir $install_dir;fi"
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
	    # for rsh do not use " - double quotes
 	    # dirty way, but I do not corrently have time to make it better
	    ###echo -n "rpm -qp ${python};" > /tmp/enstore_cmd
	    #echo -n 'if [ $? -ne 0 ];'>> /tmp/enstore_cmd
	    #echo -n "then rpm -Uvh ${python};fi" >> /tmp/enstore_cmd
	    enrsh ${remote_node} "rpm -Uvh --force ${python}"
	    #enrsh ${remote_node} /tmp/enstore_cmd
	    rc=$?
	fi
	if [ $rc -eq 0 ]; then
	    #echo -n "rpm -qp ${ftt};" > /tmp/enstore_cmd
	    #echo -n 'if [ $? -ne 0 ];'>> /tmp/enstore_cmd
	    #echo -n "then rpm -Uvh ${ftt};fi" >> /tmp/enstore_cmd
	    #enrsh ${remote_node} /tmp/enstore_cmd
	    enrsh ${remote_node} "rpm -Uvh --force ${ftt}"
	    rc=$?
	fi
	if [ $rc -eq 0 ]; then
	    #echo -n "rpm -qp ${tcl};" > /tmp/enstore_cmd
	    #echo -n 'if [ $? -ne 0 ];'>> /tmp/enstore_cmd
	    #echo -n "then rpm -Uvh ${tcl};fi" >> /tmp/enstore_cmd
	    #enrsh ${remote_node} /tmp/enstore_cmd
	    enrsh ${remote_node} "rpm -Uvh --force ${tcl}"
	    rc=$?
	fi
	if [ $rc -eq 0 ]; then
	    #echo -n "rpm -qp ${tk};" > /tmp/enstore_cmd
	    #echo -n 'if [ $? -ne 0 ];'>> /tmp/enstore_cmd
	    #echo -n "then rpm -Uvh ${tk};fi" >> /tmp/enstore_cmd
	    #enrsh ${remote_node} /tmp/enstore_cmd
	    enrsh ${remote_node} "rpm -Uvh --force ${tk}"
	    rc=$?
	fi
	if [ $rc -eq 0 ]; then
	    #echo -n "rpm -qp ${enstore};" > /tmp/enstore_cmd
	    #echo -n 'if [ $? -ne 0 ];'>> /tmp/enstore_cmd
	    #echo -n "then rpm -Uvh ${enstore};fi" >> /tmp/enstore_cmd
	    #enrsh ${remote_node} /tmp/enstore_cmd
	    enrsh ${remote_node} "rpm -Uvh --force ${enstore}"
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
