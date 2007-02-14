%define upsproduct encp
%if %{!?upsversion:1}%{?upsversion:0}
   %define upsversion v3_6d
%endif
%if %{!?rpmversion:1}%{?rpmversion:0}
   %define rpmversion %(echo %{upsversion} | sed -e 's/^[vx]//' -e 's/_/\./g')
%endif
%if %{!?upsflags:1}%{?upsflags:0}
    %define upsflags -q :
%endif
%define setupvar   SETUP_ENCP
#upsflags (if defined on the command line) needs to look something like:
#   --define 'upsflags -q dcache'
%define prefix     /opt/encp

# turn off fascist build flag, so we don't whine about .manifest files
# etc.
%define _unpackaged_files_terminate_build 0

#If this is a dcache build, we need to handle naming the rpm accordingly.
%define is_dcache %(test -e `echo %{upsflags} | grep dcache > /dev/null` && echo 1 || echo 0)
%if %{is_dcache}
%define opt_dcache_name -dcache
%endif

#
# Usual RPM definitions...
#
Summary: ups package %{upsproduct} as an RPM in %{prefix}
Release: 1
Name: %{upsproduct}-ups-opt%{opt_dcache_name}
Version: %{rpmversion}
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
echo UPSVERSION %{upsversion}
if [ -d $RPM_BUILD_ROOT%{prefix} ]; then
    chmod -fR u+wx $RPM_BUILD_ROOT%{prefix}
fi
rm -rf  $RPM_BUILD_ROOT

#Unsetup any old versions.
unsetup %{upsproduct} || unset %{setupvar} || true

# get environment, make a scratch product area
if [ -f /usr/local/etc/setups.sh ]; then
    . /usr/local/etc/setups.sh
elif [ -f /afs/fnal.gov/ups/etc/setups.sh ]; then
    . /afs/fnal.gov/ups/etc/setups.sh
else
    echo "Unable to find setups.sh"
    exit 1
fi
setup upd
rm -rf /tmp/ups2rpm
mkprd /tmp/ups2rpm

# put package files in $RBPM_BUILD_ROOT%{prefix}...
upd install -z /tmp/ups2rpm/db -j -r $RPM_BUILD_ROOT%{prefix} %{upsproduct} %{upsversion} %{upsflags} -G "%{upsflags}"
# making sure they are owned by the user makes for modifying things easier
chmod u+w $RPM_BUILD_ROOT%{prefix}/*

# prepare to build /etc/profile.d files from ups setup data
mkdir -p $RPM_BUILD_ROOT/etc/profile.d

# build the .sh setup files, fix the paths (take out $RPM_BUILD_ROOT), 
# pass -c to chooseConfig and stuff them in the profile.d area
export UPS_SHELL=sh
tf=`ups setup -z /tmp/ups2rpm/db %{upsproduct} %{upsversion} %{upsflags}`
sed -e "s|$RPM_BUILD_ROOT||g" -e "s|^/bin/rm -f $tf|#&|" -e "s|chooseConfig|chooseConfig -c|" < $tf >  $RPM_BUILD_ROOT/etc/profile.d/%{upsproduct}.$UPS_SHELL

#cleanup
rm -f $tf

# ditto for .csh setup
export UPS_SHELL=csh
tf=`ups setup -z /tmp/ups2rpm/db %{upsproduct} %{upsversion} %{upsflags}`
sed -e "s|$RPM_BUILD_ROOT||g" -e "s|^/bin/rm -f $tf|#&|" -e "s|chooseConfig|chooseConfig -c|" < $tf >  $RPM_BUILD_ROOT/etc/profile.d/%{upsproduct}.$UPS_SHELL

#cleanup
rm -f $tf

%files
%attr(0755,root,root) /etc/profile.d/%{upsproduct}.*
%attr(-,root,root) %{prefix}/*
