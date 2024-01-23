#!/bin/sh 
# builds Enstore-python rpms and places them in ~/rpmbuild/RPMS/%{arch}
#
rm -rf ~/rpmbuild
for DIR in BUILD  BUILDROOT  RPMS	SOURCES  SPECS	SRPMS; do
    mkdir -p ~/rpmbuild/$DIR
done
rm -rf Python-2.7.18
if [ ! -e Python-2.7.18.tgz ]; then
    wget https://www.python.org/ftp/python/2.7.18/Python-2.7.18.tgz
fi
tar xzf Python-2.7.18.tgz
cd Python-2.7.18/Lib/site-packages/
pip2 install -U pip
pip2  install qpid-python -t .
pip2  install pip -t .
pip2  install wheel -t .
pip2  install setuptools -t .
for P in `find . -name '*.pyc'`; do rm -f $P; done
cd -
/bin/cp Makefile.pre.in Python-2.7.18
tar czf ~/rpmbuild/SOURCES/Python-2.7.18.tgz Python-2.7.18
cp /data/python27.spec  ~/rpmbuild/SPECS/python27.spec
QA_SKIP_BUILD_ROOT=1 rpmbuild -ba --noclean ~/rpmbuild/SPECS/python27.spec

