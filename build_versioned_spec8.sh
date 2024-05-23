#!/bin/bash

source ./rpm_version

if [ -z ${ERPMSuffix+x} ]; then

  # TODO: lots of Enstore related things check to see if 'enstore' is installed
  # Changing the package name breaks this. For now, just name enstore-nonprod 'enstore'
  # ERPMSuffix="-nonprod"
  ERPMSuffix=""

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

  export ERPMSuffix=$ERPMSuffix

fi

specfile=./spec/enstore_el8_auto.spec

cp $specfile /tmp/enstore_rpm.spec

for var in ERPMSuffix EVersion ERelease ECommit; do
  sed -i s/#$var#/${!var}/ /tmp/enstore_rpm.spec
done

cat /tmp/enstore_rpm.spec
rm /tmp/enstore_rpm.spec
