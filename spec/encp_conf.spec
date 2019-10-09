%if %{!?product:1}%{?product:0}
   %define product encp
%endif
#rpm with SLF5 (4.4.2.3) has a bug that is causing it to not honor _rpmdir
# specified on the command line.  So, we pass it rpmdir and assign it to
# _rpmdir.
%if %{!?rpmdir:1}%{?rpmdir:0}
   %define _rpmdir %(echo $ENSTORE_DIR/rpmbuild)
%else
   %define _rpmdir %{rpmdir}
%endif

# turn off fascist build flag, so we don't whine about .manifest files
# etc.
%define _unpackaged_files_terminate_build 0
%define _missing_doc_files_terminate_build 0

#
# Usual RPM definitions...
#
Summary: %{product}-conf as an RPM
Release: 1
Name: %{product}-conf-FNAL
Version: %{rpmversion}
URL: ftp://ftp.fnal.gov/products/%{product}/%{rpmversion}
#BuildRequires: upsupdbootstrap
Group: Enstore
License: GPL
BuildArch: noarch
#Buildroot: /var/tmp/%{name}-buildroot
%if %{product} == encp
Buildroot: %(echo $ENSTORE_DIR/src/ENCPBIN)
   %define chooseConfig_path /opt/%{product}
%else
   %if %{product} == entv
Buildroot: %(echo $ENSTORE_DIR/src/ENTV_BIN)
      %define chooseConfig_path /opt/%{product}
   %else
Buildroot: %(echo $ENSTORE_DIR/src/ENSTORE_BIN)
      %define chooseConfig_path /opt/%{product}/ups
   %endif
%endif
Packager: Enstore Admin <enstore-admin@fnal.gov>
Vendor: Fermilab



%description
ENCP config files (FNAL specific)

%prep

%build

%install

mkdir -p  %{buildroot}/etc/profile.d
mkdir -p  %{buildroot}/%{chooseConfig_path}

cp -p $ENSTORE_DIR/ups/encp.*  %{buildroot}/etc/profile.d
cp -p $ENSTORE_DIR/ups/chooseConfig %{buildroot}/%{chooseConfig_path}

%files
%attr(0755,root,root) /etc/profile.d/%{product}.*
%attr(0755,root,root) %{chooseConfig_path}/chooseConfig

