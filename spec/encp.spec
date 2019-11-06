#product (if defined on the command line) needs to be one of the following:
#   --define 'product encp'
#   --define 'product enstore'
#   --define 'product entv'
%if %{!?product:1}%{?product:0}
   %define product encp
%endif
#One of upsversion or rpmversion should be passed in using --define.
%if %{!?upsversion:1}%{?upsversion:0}
   %define upsversion v3_6d
%endif
%if %{!?rpmversion:1}%{?rpmversion:0}
   %define rpmversion %(echo %{upsversion} | sed -e 's/^[vx]//' -e 's/_/\./g')
%endif
#upsflags (if defined on the command line) needs to look something like:
#   --define 'upsflags -q dcache'
%if %{!?upsflags:1}%{?upsflags:0}
    %define upsflags -q :
%endif
#rpm with SLF5 (4.4.2.3) has a bug that is causing it to not honor _rpmdir
# specified on the command line.  So, we pass it rpmdir and assign it to
# _rpmdir.
%if %{!?rpmdir:1}%{?rpmdir:0}
   %define _rpmdir %(echo $ENSTORE_DIR/rpmbuild)
%else
   %define _rpmdir %{rpmdir}
%endif

%define setupvar   SETUP_ENCP
%define prefix     /opt/%{product}

# turn off fascist build flag, so we don't whine about .manifest files
# etc.
%define _unpackaged_files_terminate_build 0

#If this is a dcache build, we need to handle naming the rpm accordingly.
%define is_dcache %(echo "%{upsflags}" | grep dcache > /dev/null && echo 1 || echo 0)
%if %is_dcache
   %define opt_dcache_name -dcache
%else
   %define opt_dcache_name %{nil}
%endif

#
# Usual RPM definitions...
#
Summary: ups package %{product} as an RPM in %{prefix}
Release: 1
Name: %{product}-ups-opt%{opt_dcache_name}
Version: %{rpmversion}
URL: ftp://ftp.fnal.gov/products/%{product}/%{upsversion}
#BuildRequires: upsupdbootstrap
Group: Enstore
License: GPL
%if %{product} == encp
Buildroot: %(echo $ENSTORE_DIR/src/ENCPBIN)
   %define chooseConfig_path /opt/%{product}
   %define builddir %(echo $ENSTORE_DIR/src/ENCPBIN)
%else
   %if %{product} == entv
Buildroot: %(echo $ENSTORE_DIR/src/ENTV_BIN)
      %define chooseConfig_path /opt/%{product}
      %define builddir %(echo $ENSTORE_DIR/src/ENTV_BIN)
   %else
Buildroot: %(echo $ENSTORE_DIR/src/ENSTORE_BIN)
      %define chooseConfig_path /opt/%{product}/ups
      %define builddir %(echo $ENSTORE_DIR/src/ENSTORE_BIN)
   %endif
%endif
Packager: Enstore Admin <enstore-admin@fnal.gov>
Vendor: Fermilab

%description
ENCP utility

%prep

%build

%install
cp -r  %{builddir}/*  %{buildroot}

%files
#%attr(0755,root,root) /etc/profile.d/%{product}.*
%attr(-,root,root) %{prefix}/*

%ghost %{chooseConfig_path}/chooseConfig
