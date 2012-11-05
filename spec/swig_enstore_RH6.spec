Summary: Python for enstore
Name: swig-enstore
Version: 1_883
Release: 1
License: GPL
Group: Development/Languages
Source: swig.%{version}.tgz
BuildRoot: rpmbuild/BUILD
AutoReqProv: no
AutoProv: no
AutoReq: no
Prefix: opt/swig
%global _missing_build_ids_terminate_build 0
%define debug_package %{nil}
%global __arch_install_post %{nil} 

%description

Swig for Enstore.

%prep
#echo PREP
#mkdir -p $RPM_BUILD_ROOT/%{prefix}/%{name}.%{version}
#cp -rp * $RPM_BUILD_ROOT/%{prefix}/%{name}-%{version}
#echo END PREP
%setup -q -c -n %{prefix}/swig-enstore.%{version}
#%setup -c -n $RPM_BUILD_ROOT/%{prefix}/python.2.4.3-enstore
%build
echo PWD `pwd`
mkdir -p $RPM_BUILD_ROOT/%{prefix}/%{name}.%{version}
cp -rp * $RPM_BUILD_ROOT/%{prefix}/%{name}.%{version}
cd  $RPM_BUILD_ROOT/%{prefix}/%{name}.%{version}
./configure
make clean
make
#do nothing

%install
echo INSTALL
mkdir -p $RPM_BUILD_ROOT/%{prefix}/%{name}.%{version}
cp -rp * $RPM_BUILD_ROOT/%{prefix}/%{name}.%{version}
cd  $RPM_BUILD_ROOT/%{prefix}/%{name}.%{version}

%clean
echo CLEAN
rm -rf $RPM_BUILD_ROOT/*
%files
%defattr(-,root,root,-)
/%{prefix}/swig-enstore.%{version}
