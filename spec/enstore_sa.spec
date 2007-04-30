Summary: Enstore: Mass Storage System
Name: enstore_sa
Version: 1.0.0
Release: 1
Copyright: GPL
Group: System Environment/Base
Source: enstore_sa.tgz
BuildRoot: /usr/src/redhat/BUILD
AutoReqProv: no
AutoProv: no
AutoReq: no
Prefix: opt/enstore
Requires: Python-enstore, ftt

%description
Standalone Enstore. Enstore is a Distributed Mass Storage System. 
The main storage media it uses is magnetic tape, although the new media can be added.
For the postinstallation and configuration instructions please see enstore/README

%prep
# create a tepmorary setup file
#+++++++++++
cd $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT/%{prefix}
rm -rf enstore-setup
PYTHON_DIR=`rpm -ql Python-enstore | head -1`
echo PYTHON_DIR=`rpm -ql Python-enstore | head -1`> /tmp/enstore-setup
echo export PYTHON_DIR >> /tmp/enstore-setup
echo PYTHONINC=`ls -d $PYTHON_DIR/include/python*`>> /tmp/enstore-setup
echo export PYTHONINC >> /tmp/enstore-setup
echo PYTHONLIB=`ls -d $PYTHON_DIR/lib/python*` >> /tmp/enstore-setup
echo export PYTHONLIB >> /tmp/enstore-setup
echo FTT_DIR=`rpm -ql ftt | head -1` >> /tmp/enstore-setup
echo export FTT_DIR >> /tmp/enstore-setup

echo PATH="$"PYTHON_DIR/bin:"$"PATH >> /tmp/enstore-setup
#++++++++++++

%setup -q -c -n %{prefix}
#find . -name "CVS" | xargs rm -rf

%build
. /tmp/enstore-setup
echo "BUILD"
cd $RPM_BUILD_ROOT/%{prefix}/modules
make clean
make

%install
echo INSTALL
echo $RPM_BUILD_ROOT
pwd
#chown -R enstore.enstore /home/enstore

%pre
$RPM_BUILD_ROOT/%{prefix}/external_distr/rpm_preinstall.sh
%post
$RPM_BUILD_ROOT/%{prefix}/external_distr/rpm_postinstall.sh

%preun
$RPM_BUILD_ROOT/%{prefix}/external_distr/rpm_uninstall.sh
%clean
rm -rf $RPM_BUILD_ROOT/*
rm /tmp/enstore-setup

%files
%defattr(-,enstore,enstore,-)
%doc
/%{prefix}
#/home/enstore/debugfiles.list
#/home/enstore/debugsources.list
%changelog
* Wed Feb 21 2007  <moibenko@fnal.gov> - 
- Initial build.
