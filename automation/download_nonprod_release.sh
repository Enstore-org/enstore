#!/bin/bash

# This script:
# - Queries the main Enstore Github page for new releases
# - Downloads any release with an uploaded asset that looks like enstore-nonprod.*rpm
# - Attempts to verify the signature of each downloaded rpm
# - - If successful, rebuilds the repo and touches new-nonprod-version to signal later automation
# - - If unsuccessful, removes the rpm from disk and notes in nonprod-failed-checksig rpms

json_releases="$(curl -L -H "Accept: application/vnd.github+json" -H "X-GitHub-Api-Version: 2022-11-28" https://api.github.com/repos/Enstore-org/enstore/releases)"
rpms_available="$(echo $json_releases | jq -r '.[] | select(.tag_name | test("^enstore-nonprod")) | .assets | .[] | select(.name | test("^enstore-[0-9]")) | .name')"
new_version=0
for rpm in $rpms_available; do
  if [ ! -f $rpm ]; then
    wget $(echo $json_releases | jq -r ".[] | .assets | .[] | select(.name | test(\"${rpm}\")) | .browser_download_url")
    sig_ok=$(rpm --checksig $rpm | grep pgp | grep OK | wc -l)
    if [ $sig_ok -eq 1 ]; then
      echo "checksig ok $rpm"
      new_version=1
    else
      echo "checksig failed $rpm ($(date))"
      echo "$rpm" >> /tmp/enstore-automation/nonprod-failed-checksig-rpms
      rm -f $rpm
    fi
  fi
done
if [ $new_version -eq 1 ]; then
  createrepo .
  touch /tmp/enstore-automation/new-nonprod-version
fi
