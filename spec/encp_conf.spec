%if %{!?product:1}%{?product:0}
   %define product encp
%endif

%define _rpmdir %(echo $ENSTORE_DIR/rpmbuild)

# turn off fascist build flag, so we don't whine about .manifest files
# etc.
%define _unpackaged_files_terminate_build 0
%define _missing_doc_files_terminate_build 0

#
# Usual RPM definitions...
#
Summary: %{product}-conf as an RPM in %{prefix}
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
ENCP utility

%prep
echo %{buildroot}
echo %{_rpmdir}
echo %{prefix}

%build

%install

%files
%attr(0755,root,root) /etc/profile.d/%{product}.*
%attr(0755,root,root) %{chooseConfig_path}/chooseConfig

