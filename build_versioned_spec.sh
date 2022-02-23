#!/bin/bash

source ./rpm_version

ERPMSuffix="-nonprod"

while getopts ":p" opt; do
  case ${opt} in
    p) # set version prod
      ERPMSuffix=""
      ;;
    \?) # Invalid option
      echo "Error: Invalid option"
      exit;;
  esac
done

specfile=./spec/enstore_RH7_python_2.7.16_with_start_on_boot.spec

cp $specfile /tmp/enstore_rpm.spec

for var in ERPMSuffix EVersion ERelease ECommit; do
  sed -i s/#$var#/${!var}/ /tmp/enstore_rpm.spec
done

cat /tmp/enstore_rpm.spec
rm /tmp/enstore_rpm.spec
