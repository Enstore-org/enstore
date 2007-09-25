Summary: fermi tape tool 
Name: aci
Version: 3.1.2
Release: 1
License: GPL
Group: Applications/File
#URL: 
Source0: %{name}-%{version}.tgz
#BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
Prefix:opt/%{name}
#BiuldDir:%{buildroot}/%{prefix}
BuildRoot:/usr/src/redhat/BUILD
# we do not need autodependencies here
AutoReqProv: no
%description
aml 2 access API

%prep
%setup -q -c -n $RPM_BUILD_ROOT/%{prefix}/%{name}-%{version}
%setup -q -c -n %{prefix}/%{name}-%{version}


%build

%install

%post
rm -rf %{prefix}/%{name}-%{version}/debugfiles.list
rm -rf %{prefix}/%{name}-%{version}/debugsources.list


%clean
rm -rf $RPM_BUILD_ROOT/*


%files
%defattr(-,root,root,-)
%doc
/%prefix/%{name}-%{version}
#/$RPM_BUILD_ROOT/%prefix/%{name}-%{version}/debugfiles.list

%changelog
* Thu Sep 13 2007  <moibenko@fnal.gov> - 
- Initial build.

