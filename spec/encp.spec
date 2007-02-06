%define upsversion v3_6c
%define upsproduct encp
#define updflags   -q :
%define prefix     /opt/encp

# turn off fascist build flag, so we don't whine about .manifest files
# etc.
%define _unpackaged_files_terminate_build 0

#
# Usual RPM definitions...
#
Summary: ups package %{upsproduct} as an RPM in %{prefix}
Release: 1
Name: %{upsproduct}-ups-opt
Version: 3.6c
URL: ftp://ftp.fnal.gov/products/%{upsproduct}/%{upsversion}
#BuildRequires: upsupdbootstrap
Group: Enstore
License: GPL
BuildRoot: %{_tmppath}/%{name}-buildroot

%description
ENCP utility

%prep

%build

%install
rm -rf  $RPM_BUILD_ROOT

# get environment, make a scratch product area
. /usr/local/etc/setups.sh || . /afs/fnal.gov/ups/etc/setups.sh
setup upd
rm -rf /tmp/ups2rpm
mkprd /tmp/ups2rpm

# put package files in $RBPM_BUILD_ROOT%{prefix}...
upd install -z /tmp/ups2rpm/db -j -r $RPM_BUILD_ROOT%{prefix} %{upsproduct} %{upsversion} %{upsflags} -G "%{updflags}"

# prepare to build /etc/profile.d files from ups setup data
unsetup %{upsproduct} || true

mkdir -p $RPM_BUILD_ROOT/etc/profile.d

#Determine the qualifier to use.  This allows us to pass this to the ups
# setup commands below and avoid the anoying warning messages.
qualifier=`$RPM_BUILD_ROOT%{prefix}/chooseConfig qualifier`
if [ -n "$qualifer" ]; then qualifer="-q $qualifer"; fi

# build the .sh setup files, fix the paths (take out $RPM_BUILD_ROOT) 
# and stuff them in the profile.d area
export UPS_SHELL=sh
tf=`ups setup -z /tmp/ups2rpm/db %{upsproduct} %{upsversion} $qualifier`
sed -e "s|$RPM_BUILD_ROOT||g" -e "s|^/bin/rm -f $tf|#&|" < $tf >  $RPM_BUILD_ROOT/etc/profile.d/%{upsproduct}.$UPS_SHELL

# ditto for .csh setup
export UPS_SHELL=csh
tf=`ups setup -z /tmp/ups2rpm/db %{upsproduct} %{upsversion} $qualifier`
sed -e "s|$RPM_BUILD_ROOT||g" -e "s|^/bin/rm -f $tf|#&|" < $tf >  $RPM_BUILD_ROOT/etc/profile.d/%{upsproduct}.$UPS_SHELL

%files
%attr(0755,root,root) /etc/profile.d/%{upsproduct}.*
%defattr(0555,root,root)
%{prefix}/EPS
%{prefix}/chooseConfig
%{prefix}/ddencp
%attr(-,root,root) %{prefix}/e_errors.py
%{prefix}/encp
%{prefix}/ecrc
%attr(-,root,root) %{prefix}/encp.table
%{prefix}/encp_mni_install.sh
%{prefix}/enroute2
%{prefix}/enstore
%{prefix}/enstore_tape
%{prefix}/ensync
%attr(-,root,root) %{prefix}/volume_import/*

