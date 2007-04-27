Summary: Python for enstore
Name: Python-enstore
Version: 1.0.0
Release: 3
Copyright: GPL
Group: Development/Languages
Source: python.2.4.3-enstore.tgz
BuildRoot: /usr/src/redhat/BUILD
AutoReqProv: no
AutoProv: no
AutoReq: no
#Prefix: /opt/python/python.2.4.3-enstore
Prefix: opt/python
#Requires:tcl, tcl-devel, tk, tk-devel 

%description

Special version of python for Enstore. It enables rexec and includes tk and pygresql
If you do not want to use this version you need to contact enstore developers to 
get instructions on how to build a custom python . 

%prep
# do nothing
#%setup -c -n python/python.2.4.3-enstore
#%setup -c -n opt/python/python.2.4.3-enstore
%setup -c -n %{prefix}/python.2.4.3-enstore
#%setup -c -n $RPM_BUILD_ROOT/%{prefix}/python.2.4.3-enstore
%build
#do nothing

%install
echo INSTALL
echo $RPM_BUILD_ROOT
pwd
#rm -rf /home/enstore/enstore_products/python/v2_4_3_E_1
#mkdir -p /home/enstore/enstore_products/python/v2_4_3_E_1
#cp -rp $RPM_BUILD_ROOT/python.2.4.3-enstore/* /home/enstore/enstore_products/python/v2_4_3_E_1
#chown -R enstore.enstore /home/enstore/enstore_products/python/v2_4_3_E_1

%clean
rm -rf $RPM_BUILD_ROOT/*
%files
%defattr(-,enstore,enstore,-)
#/python.2.4.3-enstore
/%{prefix}/python.2.4.3-enstore
#/opt/python/python.2.4.3-enstore
#/opt/python.2.4.3-enstore
