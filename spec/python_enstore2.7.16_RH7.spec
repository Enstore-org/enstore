Summary: Python for enstore
Name: Python-enstore2.7
Version: 16.0.0
Release: 1
License: GPL
Group: Development/Languages
Source: python-enstore-2.7.16.tgz
#BuildRoot: /usr/src/redhat/BUILD
BuildRoot: rpmbuild/BUILD
AutoReqProv: no
AutoProv: no
AutoReq: no
Prefix: opt/python
#Requires:tcl, tcl-devel, tk, tk-devel

%global _missing_build_ids_terminate_build 0
%define debug_package %{nil}
%global __arch_install_post %{nil}
# disable python_byte_compile
%global __os_install_post %(echo '%{__os_install_post}' | sed -e 's!/usr/lib[^[:space:]]*/brp-python-bytecompile[[:space:]].*$!!g')

%description
#export RPM_BUILD_ROOT=~/rpmbuild/BUILD
Special version of python for Enstore. It includes tcl, tk
If you do not want to use this version you need to contact enstore developers to
get instructions on how to build a custom python .

%prep
%setup -c -n %{prefix}/python-enstore-2.7.16
#%setup -c -n %{prefix}/python-enstore-%{version}
echo "BUILD ROOT" $RPM_BUILD_ROOT
%build
#export RPM_BUILD_ROOT=/root/rpmbuild/BUILD
echo "BUILD ROOT1" $RPM_BUILD_ROOT
#do nothing

%install
echo INSTALL
#export RPM_BUILD_ROOT=/root/rpmbuild/BUILD
echo $RPM_BUILD_ROOT
echo `pwd`
mkdir -p $RPM_BUILD_ROOT/%{prefix}/python-enstore-2.7.16
cp -rp * $RPM_BUILD_ROOT/%{prefix}/python-enstore-2.7.16


%clean
echo CLEAN
#export RPM_BUILD_ROOT=/root/rpmbuild/BUILD
#rm -rf $RPM_BUILD_ROOT/*
%files
%defattr(-,root,root,-)
/%{prefix}/python-enstore-2.7.16
%changelog
* Thu May 30 2019  <moibenko@fnal.gov> -
- initial
