Summary: Python for enstore
Name: swig-enstore
Version: 1_883
Release: 1
License: GPL
Group: Development/Languages
Source: swig.%{version}.tgz
BuildRoot: /usr/src/redhat/BUILD
AutoReqProv: no
AutoProv: no
AutoReq: no
Prefix: opt/swig

%description

Swig for Enstore.

%prep
mkdir -p $RPM_BUILD_ROOT/%{prefix}/swig-enstore.%{version}
%setup -c -n %{prefix}/swig-enstore.%{version}
#%setup -c -n $RPM_BUILD_ROOT/%{prefix}/python.2.4.3-enstore
%build
cd  $RPM_BUILD_ROOT/%{prefix}/swig-enstore.%{version}
./configure
make clean
make
#do nothing

%install

%clean
rm -rf $RPM_BUILD_ROOT/*
%files
%defattr(-,root,root,-)
/%{prefix}/swig-enstore.%{version}
