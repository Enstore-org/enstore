Summary: Python for enstore
Name: Python-enstore2.6
Version: 3.0.0
Release: 4
License: GPL
Group: Development/Languages
Source: python-enstore-2.6.3.tgz
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
%setup -c -n %{prefix}/python-enstore-2.6.3
%build
#do nothing

%install
echo INSTALL
echo $RPM_BUILD_ROOT
pwd

%clean
rm -rf $RPM_BUILD_ROOT/*
%files
%defattr(-,root,root,-)
/%{prefix}/python-enstore-2.6.3
%changelog
* Wed Jul 13 2011  <moibenko@fnal.gov> -
- added readline
* Tue Jun 08 2010  <moibenko@fnal.gov> -
- added ssl support
