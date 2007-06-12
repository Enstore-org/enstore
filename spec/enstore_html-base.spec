Summary: Enstore html 
Name: enstore_html
Version: %{MajVersion}.%{MinVersion}
License:  FNAL/DOE (BSD-like Open Source licensing)
Group: Applications/System
URL: https://plone4.fnal.gov/P0/Enstore_and_Dcache/
Source: %{name}-%{version}.tar.gz
BuildRoot: %{_topdir}/BUILD/%{name}-%{version}
Prefix: /opt/%{name}
Requires: Python-enstore,  enstore_sa, httpd
%description
Fermilab enstore html 
%prep


%setup -q

%build

%install
rm -rf $RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT


%files
%defattr(-,root,root,-)
/opt/%{name}

%post
echo "installing %{name}-%{version}"
cd %{prefix}
./deploy_enstore_html
#
%preun
cd %{prefix}
./undeploy_enstore_html



%changelog
* Tue Jun 12 2007 Dmitry Litvintsev <litvinse@cduqbar.fnal.gov> - html-1
- Initial build.

