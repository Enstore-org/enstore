#!/bin/sh 
# builds Enstore-python rpms and places them in ~/rpmbuild/RPMS/%{arch}
#
touch /var/log/enstore_build.log
/bin/bash -x /data/run_build.sh 2>&1 | tee -a  /var/log/enstore_build.log
