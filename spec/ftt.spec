Summary: fermi tape tool
Name: ftt
Version: 2.28
Release: 0
License: GPL
Group: Applications/File
#URL:
Source0: %{name}-%{version}.tgz
#BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
Prefix:opt/ftt
#BiuldDir:%{buildroot}/%{prefix}
BuildRoot:rpmbuild/BUILD
# we do not need autodependencies here
AutoReqProv: no
%global _missing_build_ids_terminate_build 0
%define debug_package %{nil}
%global __arch_install_post %{nil}

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
#%setup -q -c -n $RPM_BUILD_ROOT/%{prefix}/%{name}-%{version}
%setup -q -c -n %{prefix}/%{name}-%{version}
#%setup -c -n opt/python.2.4.3-enstore


%build
mkdir -p $RPM_BUILD_ROOT/%{prefix}/%{name}-%{version}
cp -rp * $RPM_BUILD_ROOT/%{prefix}/%{name}-%{version}
cd $RPM_BUILD_ROOT/%{prefix}/%{name}-%{version}/ftt_lib
make clean
make
cd $RPM_BUILD_ROOT/%{prefix}/%{name}-%{version}/ftt_test
make clean
make
echo BUILD DONE

%install
mkdir -p $RPM_BUILD_ROOT/%{prefix}/%{name}-%{version}
cp -rp * $RPM_BUILD_ROOT/%{prefix}/%{name}-%{version}
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
#/$RPM_BUILD_ROOT/%prefix/%{name}-%{version}
#/$RPM_BUILD_ROOT/%prefix/%{name}-%{version}/debugfiles.list
#$RPM_BUILD_ROOT/%prefix/%{name}-%{version}.x86_64
%changelog
* Wed Apr 26 2011 <moibenko@fnal.gov> -
- New version. Fixed bug in ftt_lib/Linux/ftt_scsi.c
- Added ftt_test/mode_select.c as an example of mode sense and mode select commands
* Mon Dec 27 2010 <moibenko@fnal.gov> -
- New version. Added T10K tape drive
* Wed Sep 17 2008 <moibenko@fnal.gov> -
- Release 3 Added dignostic messages for detecting compression
* Wed Feb 21 2007  <moibenko@fnal.gov> -
- Initial build.


