#!/bin/bash
cd ~
rm -rf ~/rpmbuild
for DIR in BUILD  BUILDROOT  RPMS	SOURCES  SPECS	SRPMS; do
    mkdir -p ~/rpmbuild/$DIR
done
cp /data/enstore_auto.spec ~/rpmbuild/SPECS
#git clone https://github.com/Enstore-org/enstore.git
cd enstore
tar czf ~/rpmbuild/SOURCES/enstore.tgz .
rpmbuild -ba --noclean ~/rpmbuild/SPECS/enstore_auto.spec 2>&1 | tee /tmp/build.log


