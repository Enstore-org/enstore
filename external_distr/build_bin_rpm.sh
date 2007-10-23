#!/bin/sh -xv
###############################################################################
#
# $Id:
#
###############################################################################


set -u  # force better programming and ability to use check for not set
export CVSROOT=hppccvs@cdcvs.fnal.gov:/cvs/hppc
if [ "`whoami`" != 'root' ]
then
    echo You need to run this script as user "root"
    exit 1
fi
cd /tmp
mkdir enstore_build
cd enstore_build
rm -rf *
cvs co -r production enstore
cd enstore
tar czf enstore.tgz *
cp -f enstore.tgz /usr/src/redhat/SOURCES
rm -f enstore.tgz  
cp -f spec/enstore_bin.spec /usr/src/redhat/SPECS
rpmbuild -ba /usr/src/redhat/SPECS/enstore_bin.spec

cd
rm -rf /tmp/enstore_build

