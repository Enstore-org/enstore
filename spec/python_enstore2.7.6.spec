Summary: Python for enstore
Name: Python-enstore2.7
Version: 6.0.0
Release: 0
License: GPL
Group: Development/Languages
Source: python-enstore-2.7.6.tgz
BuildRoot: /usr/src/redhat/BUILD
AutoReqProv: no
AutoProv: no
AutoReq: no
Prefix: opt/python
#Requires:tcl, tcl-devel, tk, tk-devel 

%description

Special version of python for Enstore. It includes tcl, tk
If you do not want to use this version you need to contact enstore developers to 
get instructions on how to build a custom python . 

%prep
%setup -c -n %{prefix}/python-enstore-2.7.6
%build
#do nothing

%install
echo INSTALL
echo $RPM_BUILD_ROOT
pwd
%define debug_package %{nil}

%clean
rm -rf $RPM_BUILD_ROOT/*
%files
%defattr(-,root,root,-)
/%{prefix}/python-enstore-2.7.6
%changelog
* Fri Jan 10 2014 <moibenko@fnal.gov> -
- initial
