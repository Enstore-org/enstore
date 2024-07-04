#!/bin/bash
cd /root
rm -rf /root/rpmbuild /root/enstore
for DIR in BUILD  BUILDROOT  RPMS	SOURCES  SPECS	SRPMS; do
    mkdir -p /root/rpmbuild/$DIR
done
cp /data/enstore_auto.spec /root/rpmbuild/SPECS
git clone https://github.com/Enstore-org/enstore.git
cd /root/enstore
git checkout $1
tar czf /root/rpmbuild/SOURCES/enstore.tgz .
cd /root
rpmbuild -ba --noclean  /root/rpmbuild/SPECS/enstore_auto.spec 2>&1 | tee /tmp/build.log


