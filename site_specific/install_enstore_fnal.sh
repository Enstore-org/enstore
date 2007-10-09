#!/bin/sh
###############################################################################
#
# $Id:
#
###############################################################################
if [ ! -x /root/install_enstore_rpm.sh ]
then
    echo "copy install_enstore_rpm.sh to /root"
    exit 1
fi

/root/install_enstore_rpm.sh server fnal ftp://enconfig1/en/lts44/i386/
