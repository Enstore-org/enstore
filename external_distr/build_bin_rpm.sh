#!/bin/sh -xv
###############################################################################
#
# $Id:
#
###############################################################################


set -u  # force better programming and ability to use check for not set
EVersion="${1:-1.0b.1}"
ERelease="${2:-10}"


export CVSROOT=hppccvs@cdcvs.fnal.gov:/cvs/hppc
if [ "`whoami`" != 'root' ]
then
    echo You need to run this script as user "root"
    exit 1
fi
buildroot="/usr/src/redhat/"
for d in BUILD RPMS SOURCES SPECS SRPMS; do
    if [ ! -d $buildroot$d ];then
	mkdir -p $buildroot$d
    fi
done

cd /tmp
mkdir enstore_build
cd enstore_build
rm -rf *
cvs co -r production enstore
cd enstore
tar czf enstore.tgz *
cp -f enstore.tgz /usr/src/redhat/SOURCES
rm -f enstore.tgz  
rm -f /usr/src/redhat/SPECS/enstore_bin.spec
sed "
/Version:/c Version: $EVersion
/Release:/c Release: $ERelease
" < spec/enstore_bin.spec > /usr/src/redhat/SPECS/enstore_bin.spec
#cp -f spec/enstore_bin.spec /usr/src/redhat/SPECS
rpmbuild -ba /usr/src/redhat/SPECS/enstore_bin.spec

cd
rm -rf /tmp/enstore_build

