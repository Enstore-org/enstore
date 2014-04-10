#!/bin/sh
###############################################################################
#
# $Id$
#
###############################################################################


GIT_ORIGIN=ssh://p-enstore-git-test@cdcvs.fnal.gov/cvs/projects/enstore-git-test

git_tag=""

if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-t" ] ; then shift; git_tag=$1;shift; fi

set -u  # force better programming and ability to use check for not set

if [ "`whoami`" != 'root' ]
then
    echo You need to run this script as user "root"
    exit 1
fi

buildroot="/usr/src/redhat"
for d in BUILD RPMS SOURCES SPECS SRPMS; do
    if [ ! -d $buildroot$d ];then
	mkdir -p $buildroot$d
    fi
done

rm -rf /tmp/enstore_build
mkdir  /tmp/enstore_build
cd     /tmp/enstore_build

if [ -z $git_tag ]
then 
    # @todo:
    # I must NOT use production tag.
    # now production is a branch
    # git_tag="production"
    echo "Please specify tag in remote git repository $GIT_ORIGIN"
    exit 1
fi

# get tar file from remote repository
rm -f ${buildroot}/SOURCES/enstore.tgz
git archive --format=tar --prefix=enstore/  \
  --remote=${GIT_ORIGIN} $git_tag \
| gzip > ${buildroot}/SOURCES/enstore.tgz

# extract EVersion,ERelease from file rpm_version
# if it is not set there, use version given in command line args
tar xfz ${buildroot}/SOURCES/enstore.tgz \
  enstore/rpm_version \
  enstore/spec/enstore.spec

if [ -f "enstore/rpm_version" ];
then
    source enstore/rpm_version
fi

if [ "${EVersion:-x}" = "x" ];
then
    EVersion="${1:-1.0.1}"
fi
if [ "${ERelease:-x}" = "x" ];
then
    ERelease="${2:-10}"
fi

# echo ${EVersion} ${ERelease}

#cvs does no like dots
# git can work with dots
# cvs_EVersion=`echo ${EVersion} | sed -e "s/\./_/g"`

# I did not extract repository. Shall we tag central repository at all?
#
#if [ $git_tag = "production" ]; then
#    # tag cvs files only if we checked out production tag
#    # this is needed to build rpm from the same cvs tag on different platforms
#    cvs tag -F -r production ENSTORE_RPM_${cvs_EVersion}_${ERelease}
#fi


# Update spec file with version
rm -f ${buildroot}/SPECS/enstore.spec

# someone needs to add git hash code into spec file when available 
sed "
/Version:/c Version: $EVersion
/Release:/c Release: $ERelease
" < enstore/spec/enstore.spec \
  > ${buildroot}/SPECS/enstore.spec

# Build
echo rpmbuild -ba ${buildroot}/SPECS/enstore.spec
rpmbuild -ba ${buildroot}/SPECS/enstore.spec

# Cleanup
rm -rf /tmp/enstore_build
