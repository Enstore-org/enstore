#!/bin/sh -xv
###############################################################################
#
# $Id:
#
###############################################################################


set -u  # force better programming and ability to use check for not set
EVersion="${1:-1.0.1}"
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
rm -rf enstore_build
mkdir enstore_build
cd enstore_build
cvs co -r production enstore

cd enstore
if [ -f "rpm_version" ];
then
    source rpm_version
fi
if [ "${EVersion:-x}" != "x" ];
then
    EVersion="${1:-1.0.1}"
fi
if [ "${ERelease:-x}" != "x" ];
then
    ERelease="${2:-10}"
fi
#cvs does no like dots
cvs_EVersion=`echo ${EVersion} | sed -e "s/\./_/g"`

cvs tag -F -r production ENSTORE_RPM_${cvs_EVersion}_${ERelease} enstore

tar czf enstore.tgz *
cp -f enstore.tgz /usr/src/redhat/SOURCES
rm -f enstore.tgz  
tar czf enstore_sa.tgz *

cp -f enstore_sa.tgz /usr/src/redhat/SOURCES
rm -f enstore_sa.tgz
rm -f /usr/src/redhat/SPECS/enstore.spec
rm -f /usr/src/redhat/SPECS/enstore_sa.spec
#cp -f spec/enstore.spec /usr/src/redhat/SPECS
sed "
/Version:/c Version: $EVersion
/Release:/c Release: $ERelease
" < spec/enstore.spec > /usr/src/redhat/SPECS/enstore.spec
#cp -f spec/enstore_sa.spec /usr/src/redhat/SPECS
sed "
/Version:/c Version: $EVersion
/Release:/c Release: $ERelease
" < spec/enstore_sa.spec > /usr/src/redhat/SPECS/enstore_sa.spec
rpmbuild -ba /usr/src/redhat/SPECS/enstore.spec
rpmbuild -ba /usr/src/redhat/SPECS/enstore_sa.spec

cd
rm -rf /tmp/enstore_build

