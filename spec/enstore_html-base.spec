Summary: Enstore html 
Name: enstore_html
Version: %{MajVersion}.%{MinVersion}
License:  FNAL/DOE (BSD-like Open Source licensing)
Group: Applications/System
URL: https://plone4.fnal.gov/P0/Enstore_and_Dcache/
Source: %{name}_%{version}.tar.gz
BuildRoot: %{_topdir}/BUILD/%{name}-%{version}
BuildArch: noarch
Prefix: /opt/%{name}
Requires: httpd enstore
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
* Tue Feb 16 2010 Alexander Moibenko <moibenko@.fnal.gov> - html-2
- Pyhton was moved
* Tue Jun 12 2007 Dmitry Litvintsev <litvinse@cduqbar.fnal.gov> - html-1
- Initial build.
