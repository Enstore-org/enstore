Summary: entv monitor for enstore
Name: entv
Version: 0.1.0
Release: 1
License: GPLv2
Group: Applications/System
Source: entv.tgz
BuildRoot: rpmbuild/BUILD
AutoReqProv: no
AutoProv: no
AutoReq: no
Prefix: opt/entv

%description
The entv program displays the current state of enstore movers, clients and transfers. To build rpm build entv binary and create entv.tgz:
cd enstore/src
make entv
cd ENTV_BIN
tar czf entv.tgz .

%prep
%setup -q -c -n %{prefix}

%build
#do nothing
%install
echo INSTALL
echo $RPM_BUILD_ROOT
pwd
mkdir -p $RPM_BUILD_ROOT/%{prefix}
cp -rp * $RPM_BUILD_ROOT/%{prefix}
#do nothing

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
/opt/entv

%changelog
* Thu Nov  1 2018  <enstore@gccensrv1.fnal.gov> - 
- Initial build.

