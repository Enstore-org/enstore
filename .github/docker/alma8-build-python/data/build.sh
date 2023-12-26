#!/bin/sh 
rm -rf ~/rpmbuild
for DIR in BUILD  BUILDROOT  RPMS	SOURCES  SPECS	SRPMS; do
    mkdir -p ~/rpmbuild/$DIR
done
#git clone https://github.com/nmilford/rpm-python27.git
wget https://www.python.org/ftp/python/2.7.18/Python-2.7.18.tgz -O ~/rpmbuild/SOURCES/Python-2.7.18.tgz
cp /data/python27.spec  ~/rpmbuild/SPECS/python27.spec
QA_RPATHS=$[ 0x0001|0x0010 ] rpmbuild -bb ~/rpmbuild/SPECS/python27.spec
