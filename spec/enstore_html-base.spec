Summary: Enstore html 
Name: enstore_html
Version: %{MajVersion}.%{MinVersion}
License:  FNAL/DOE (BSD-like Open Source licensing)
Group: Applications/System
URL: https://plone4.fnal.gov/P0/Enstore_and_Dcache/
Source: %{name}_%{version}.tar.gz
BuildRoot: %{_topdir}/BUILD/%{name}-%{version}
Prefix: /opt/%{name}
Requires: Python-enstore,  enstore_sa, httpd
%description
Fermilab enstore html 
%pre 
echo "Pre"
#
%prep
#
%build
#
%install
#
%clean
#
%post
echo "installing %{name}-%{version}"
cd %{prefix}/%{name}-%{version}
./deploy_enstore_html
#
%preun
cd %{prefix}/%{name}-%{version}
./undeploy_enstore_html
#
%files
%defattr(-,root,root,-)
%{prefix}

%changelog
* Tue Jun 12 2007 Dmitry Litvintsev <litvinse@cduqbar.fnal.gov> - html-1
- Initial build.

