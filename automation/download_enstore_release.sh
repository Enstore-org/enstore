#!/bin/bash

# This script:
# - Queries the main Enstore Github page for new releases
# - Downloads any release with an uploaded asset that looks like enstore-nonprod.*rpm
# - Attempts to verify the signature of each downloaded rpm
# - - If successful, rebuilds the repo and touches new-nonprod-version to signal later automation
# - - If unsuccessful, removes the rpm from disk and notes in nonprod-failed-checksig rpms

# Args:
# -d|--repodir: Director of the repo
# -p|--prod: Match production releases instead of nonprod.

repo_dir="."
prod=0

usage()
{ 
  echo "Usage: $0 [--repodir DIR] [--prod]"  
  exit 1
}

while [[ $# -gt 0 ]]; do
  case $1 in
    -d|--repodir)
      repo_dir="$2"
      shift # past argument
      shift # past value
      ;;
    -p|--prod)
      prod=1
      shift # past argument
      ;;
    -*|--*)
      echo "unknown option $1"
      usage
      ;;
    *)
      echo "unknown arg $1"
      usage
      ;;
  esac
done

pushd ${repo_dir}
json_releases="$(curl -L -H "Accept: application/vnd.github+json" -H "X-GitHub-Api-Version: 2022-11-28" https://api.github.com/repos/Enstore-org/enstore/releases)"
rpms_available=""
output_dir=""
if [ $prod -gt 0 ]; then
  rpms_available="$(echo $json_releases | jq -r '.[] | select(.tag_name | test("^enstore-[0-9]")) | .assets | .[] | select(.name | test("^enstore-[0-9]")) | .name')"
  output_dir="prod"
else
  rpms_available="$(echo $json_releases | jq -r '.[] | select(.tag_name | test("^enstore-nonprod-[0-9]")) | .assets | .[] | select(.name | test("^enstore-[0-9]")) | .name')"
  output_dir="nonprod"
fi
mkdir -p /tmp/enstore-automation/${output_dir}
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
      echo "$rpm" >> /tmp/enstore-automation/${output_dir}/failed-checksig-rpms
      rm -f $rpm
    fi
  fi
done
if [ $new_version -eq 1 ]; then
  createrepo .
  touch /tmp/enstore-automation/${output_dir}/new-version
fi
popd
