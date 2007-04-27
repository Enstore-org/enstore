Summary: fermi tape tool 
Name: ftt
Version: 2.26
Release: 1
License: GPL
Group: Applications/File
#URL: 
Source0: %{name}-%{version}.tgz
#BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
Prefix:opt/ftt
#BiuldDir:%{buildroot}/%{prefix}
BuildRoot:/usr/src/redhat/BUILD
# we do not need autodependencies here
AutoReqProv: no
%description

%prep
#echo RPMBUILDROOT=$RPM_BUILD_ROOT/%{prefix}
#RPM_BUILD_DIR=$RPM_BUILD_ROOT/%{prefix}
#RPMBUILDDIR=$RPM_BUILD_ROOT/%{prefix}
#echo RPM_BUILD_DIR=$RPM_BUILD_DIR
#mkdir -p $RPM_BUILD_ROOT/%{prefix}
#echo TOP=%{_topdir}
#cd $RPM_BUILD_ROOT
#%%setup -q
%setup -q -c -n $RPM_BUILD_ROOT/%{prefix}/%{name}-%{version}
%setup -q -c -n %{prefix}/%{name}-%{version}
#%setup -c -n opt/python.2.4.3-enstore


%build
cd $RPM_BUILD_ROOT/%{prefix}/%{name}-%{version}/ftt_lib
make clean
make
cd $RPM_BUILD_ROOT/%{prefix}/%{name}-%{version}/ftt_test
make clean
make

%install
cd $RPM_BUILD_ROOT/%{prefix}/%{name}-%{version}/ftt_lib
make install
cd $RPM_BUILD_ROOT/%{prefix}/%{name}-%{version}/ftt_test
make install
#%rm -rf $RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT/*


%files
%defattr(-,root,root,-)
%doc
/%prefix/%{name}-%{version}
#/$RPM_BUILD_ROOT/%prefix/%{name}-%{version}/debugfiles.list

%changelog
* Wed Feb 21 2007  <moibenko@fnal.gov> - 
- Initial build.

