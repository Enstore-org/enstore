Summary: Enstore: Mass Storage System
Name: enstore
Version: 4.2.2
Release: 6
License: GPL
Group: System Environment/Base
Source: enstore.tgz
BuildRoot: rpmbuild/BUILD
AutoReqProv: no
AutoProv: no
AutoReq: no
Prefix: opt/enstore
Requires: mt-st

%global _missing_build_ids_terminate_build 0
%define debug_package %{nil}
%global __arch_install_post %{nil}
# disable python_byte_compile
%global __os_install_post %(echo '%{__os_install_post}' | sed -e 's!/usr/lib[^[:space:]]*/brp-python-bytecompile[[:space:]].*$!!g')
%description
Enstore Distributed Mass Storage System.
The main storage media it uses is magnetic tape, although the new media can be added.
Beginning with version 3.X File Aggregation Feature is added.
For the postinstallation and configuration instructions please see enstore/README

%prep
# check if all supporting rpms are installed
rpm -q Python-enstore2.7
if [ $? -ne 0 ]; then
	echo "Python-enstore2.7 is not installed"
	exit 1
fi

rpm -q swig-enstore
if [ $? -ne 0 ]; then
	echo "swig-enstore is not installed"
	exit 1
fi
mkdir -p $RPM_BUILD_ROOT/%{prefix}
cd $RPM_BUILD_ROOT
echo "BUILD ROOT $RPM_BUILD_ROOT "
rm -rf enstore-setup

%setup -q -c -n %{prefix}
# copy all supporting products
cp -rp * $RPM_BUILD_ROOT/%{prefix}
pydir=`rpm -ql Python-enstore2.7 | head -1`
PYTHON_DIR=$RPM_BUILD_ROOT/%{prefix}/Python
mkdir -p $PYTHON_DIR
mkdir -p Python
cp -rp $pydir/* $PYTHON_DIR
cp -rp $pydir/* Python
rm -rf $PYTHON_DIR/*.tgz
#fttdir=`rpm -ql ftt | head -1`
FTT_DIR=$RPM_BUILD_ROOT/%{prefix}/FTT
mv $RPM_BUILD_ROOT/%{prefix}/ftt $FTT_DIR
#mkdir -p $FTT_DIR
#mkdir -p FTT
#cp -rp $fttdir/* $FTT_DIR
#cp -rp $fttdir/* FTT
#rm -rf $FTT_DIR/*.tgz
swigdir=`rpm -ql swig-enstore | head -1`
SWIG_DIR=$RPM_BUILD_ROOT/%{prefix}/SWIG
mkdir -p $SWIG_DIR
mkdir -p SWIG
cp -rp $swigdir/* $SWIG_DIR
cp -rp $swigdir/* SWIG
#tar xzf /tmp/enstore_qpid_python2.7.tgz

# create a tepmorary setup file
#+++++++++++
echo PYTHON_DIR=$PYTHON_DIR> /tmp/enstore-setup
echo export PYTHON_DIR >> /tmp/enstore-setup
echo PYTHONINC=`ls -d $PYTHON_DIR/include/python*`>> /tmp/enstore-setup
echo export PYTHONINC >> /tmp/enstore-setup
echo PYTHONLIB=`ls -d $PYTHON_DIR/lib/python*` >> /tmp/enstore-setup
echo export PYTHONLIB >> /tmp/enstore-setup
echo FTT_DIR=$FTT_DIR >> /tmp/enstore-setup
echo export FTT_DIR >> /tmp/enstore-setup
echo ENSTORE_DIR=$RPM_BUILD_ROOT/%{prefix} >> /tmp/enstore-setup
echo export ENSTORE_DIR >> /tmp/enstore-setup
echo SWIG_DIR=$SWIG_DIR >> /tmp/enstore-setup
echo export SWIG_DIR >> /tmp/enstore-setup
echo SWIG_LIB=$SWIG_DIR/swig_lib >> /tmp/enstore-setup
echo export SWIG_LIB >> /tmp/enstore-setup
echo PATH="$"SWIG_DIR:"$"PYTHON_DIR/bin:/usr/pgsql-9.2/bin:"$"PATH >> /tmp/enstore-setup

%build
. /tmp/enstore-setup
echo "BUILD"
pushd .
cd $FTT_DIR/ftt_lib
rm -f ftt_mtio.h # do not know how did it get here
make clean
make
make install
cd ../ftt_test
make clean
make
make install
popd
make clean
make all

%install
echo INSTALL `pwd`
echo LS `ls`
echo LS1 `$RPM_BUILD_ROOT/%{prefix}`
mkdir -p $RPM_BUILD_ROOT/%{prefix}
cp -rp * $RPM_BUILD_ROOT/%{prefix}
if [ ! -d $RPM_BUILD_ROOT/usr/local/etc ]; then
	mkdir -p $RPM_BUILD_ROOT/usr/local/etc
fi
if [ ! -f $RPM_BUILD_ROOT/usr/local/etc/setups.sh ];then
	cp -r $RPM_BUILD_ROOT/%{prefix}/external_distr/setups.sh $RPM_BUILD_ROOT/usr/local/etc/setups.sh
fi
echo INSTALL DONE
%pre
PATH=/usr/sbin:$PATH
# check if user "enstore" and group "enstore "exist"

echo 'Checking if group "enstore" exists'
grep enstore /etc/group
if [ $? -ne 0 ]; then
    echo 'Creating group "enstore"'
    groupadd -g 6209 enstore
fi
echo 'Checking if user "enstore" exists'
id enstore
if [ $? -ne 0 ]; then
	echo 'Creating user "enstore"'
	useradd -u 5744 -g enstore enstore
	chmod 775 ~enstore
fi

#$RPM_BUILD_ROOT/%{prefix}/external_distr/rpm_preinstall.sh
#%post
#$RPM_BUILD_ROOT/%{prefix}/external_distr/rpm_postinstall.sh

%post
echo "POSTINSTALL"
export ENSTORE_DIR=$RPM_BUILD_ROOT/%{prefix}
rm -rf /tmp/enstore-setup
PYTHON_DIR=$ENSTORE_DIR/Python
PYTHONLIB=`ls -d $PYTHON_DIR/lib/python*`
echo PYTHON_DIR=$PYTHON_DIR > /tmp/enstore-setup
echo export PYTHON_DIR >> /tmp/enstore-setup
echo PYTHONINC=`ls -d $PYTHON_DIR/include/python*`>> /tmp/enstore-setup
echo export PYTHONINC >> /tmp/enstore-setup
echo PYTHONLIB=$PYTHONLIB >> /tmp/enstore-setup
echo export PYTHONLIB >> /tmp/enstore-setup
FTT_DIR=$ENSTORE_DIR/FTT
echo FTT_DIR=$FTT_DIR >> /tmp/enstore-setup
echo export FTT_DIR >> /tmp/enstore-setup

echo PATH="$"PYTHON_DIR/bin:"$"PATH >> /tmp/enstore-setup
. /tmp/enstore-setup
#chown -R enstore.enstore /home/enstore
export ENSTORE_DIR=$RPM_BUILD_ROOT/%{prefix}

# copy qpid extras
cp -p /opt/enstore/etc/extra_python.pth $PYTHONLIB/site-packages
cp -rp /usr/lib/python2.7/site-packages/qpid $PYTHONLIB/site-packages
cp -rp /usr/lib/python2.7/site-packages/mllib $PYTHONLIB/site-packages

echo "Creating sudoers file"
echo "The original is saved into /etc/sudoers.enstore_save"
if [ ! -f /etc/sudoers.enstore_save ]; then
    cp /etc/sudoers /etc/sudoers.enstore_save
fi
# we do not want tty, but it may be set by default and preserve PATH
sed -e /requiretty/{d} -e /secure_path/{d} /etc/sudoers.enstore_save > /etc/sudoers.e
chmod 740 /etc/sudoers.e
# Need to add env_keep because in RH5 the sudoers was modified to
#reset all environment
echo 'Defaults env_keep +=	"PATH PYTHON_DIR PYTHONPATH PYTHONINC PYTHONLIB \' >> /etc/sudoers.e
echo '                        	ENSTORE_CONFIG_HOST ENSTORE_CONFIG_PORT ENSTORE_DIR ENSTORE_MAIL \' >> /etc/sudoers.e
echo '                        	FTT_DIR	KRBTKFILE"' >> /etc/sudoers.e
echo "Cmnd_Alias      PYTHON  = ${PYTHON_DIR}/bin/python" >> /etc/sudoers.e
echo "Cmnd_Alias      PIDKILL = ${ENSTORE_DIR}/bin/pidkill, ${ENSTORE_DIR}/bin/pidkill_s, /bin/kill" >> /etc/sudoers.e
echo "Cmnd_Alias      MOVER = ${ENSTORE_DIR}/sbin/mover" >> /etc/sudoers.e
echo "Cmnd_Alias      MIGRATOR = ${ENSTORE_DIR}/sbin/migrator" >> /etc/sudoers.e
echo "enstore ALL=NOPASSWD:PYTHON, NOPASSWD:PIDKILL, NOPASSWD:MOVER, NOPASSWD:MIGRATOR" >> /etc/sudoers.e
rm -f /etc/sudoers
cp /etc/sudoers.e /etc/sudoers
chmod 440 /etc/sudoers

echo "Copying $ENSTORE_DIR/bin/enstore-boot to /etc/rc.d/init.d"
cp -f $ENSTORE_DIR/bin/enstore-boot /etc/rc.d/init.d
echo "Configuring the system to start enstore on boot"
/etc/rc.d/init.d/enstore-boot install
echo "Copying $ENSTORE_DIR/bin/monitor_server-boot to /etc/rc.d/init.d"
cp -f $ENSTORE_DIR/bin/monitor_server-boot /etc/rc.d/init.d
echo "Configuring the system to start monitor server on boot"
/etc/rc.d/init.d/monitor_server-boot install
echo "Saving /etc/rc.d/rc.local to /etc/rc.d/rc.local.enstore_save"
cp -pf /etc/rc.d/rc.local /etc/rc.d/rc.local.enstore_save
echo "Copying $ENSTORE_DIR/sbin/rc.local to /etc/rc.d"
cp -f $ENSTORE_DIR/sbin/rc.local /etc/rc.d
echo "Updating symbolic links"
$ENSTORE_DIR/external_distr/update_sym_links.sh
if [ ! -d ~enstore/config ]; then
   echo "Creating default output directory: /var/log/enstore"
   mkdir -p /var/log/enstore
   chown enstore.enstore /var/log/enstore
fi
rm -f $ENSTORE_DIR/debugfiles.list
rm -f $ENSTORE_DIR/debugsources.list
rm /tmp/enstore-setup
echo "Enstore installed. Please read README file"

%preun
echo "PRE UNINSTALL"
$RPM_BUILD_ROOT/%{prefix}/external_distr/rpm_uninstall.sh $1
%clean
rm -rf $RPM_BUILD_ROOT/*

%files
%defattr(-,enstore,enstore,-)
%doc
/%{prefix}
%config /%{prefix}/etc/enstore_configuration
%config /%{prefix}/etc/sam.conf
%config /%{prefix}/etc/stk.conf
%config /usr/local/etc/setups.sh
#/etc/rc.d/init.d/enstore-boot
#/etc/sudoers
#/home/enstore/debugfiles.list
#/home/enstore/debugsources.list
%changelog
* Mon Mar 30 2015  <moibenko@fnal.gov> -
- new release 4.2.2, release 6.
* Thu Mar 26 2015  <moibenko@fnal.gov> -
- new release 4.2.2, release 5.
* Tue Mar 10 2015  <moibenko@fnal.gov> -
- new release 4.2.2, release 4.
* Mon Dec 22 2014  <moibenko@fnal.gov> -
- new release 4.2.2, release 3
* Thu Oct 30 2014  <moibenko@fnal.gov> -
- new release 4.2.2, release 2
* Thu Oct 09 2014  <moibenko@fnal.gov> -
- new version 4.2.2, release 0
- added SCSI Media Verify for T10000C and higher drives
* Wed Oct 01 2014  <moibenko@fnal.gov> -
- new release 4.2.1, release 1
* Mon Sep 08 2014  <moibenko@fnal.gov> -
- new release 4.2.1, release 0
* Thu Aug 14 2014  <moibenko@fnal.gov> -
- new version 4.2.0 release 0 ftt now is part of enstore rpm
* Tue May 06 2014  <moibenko@fnal.gov> -
- new release 4.1.2, release 1
* Thu Apr 10 2014  <moibenko@fnal.gov> -
- new version 4.1.2-0
* Mon Feb 10 2014  <moibenko@fnal.gov> -
- new version 4.1.1-0
* Tue Jan 14 2014  <moibenko@fnal.gov> -
- started using python 2.7.6 rpm
* Thu Dec 12 2013  <moibenko@fnal.gov> -
- new version 4.0.0-4
* Mon Aug 19 2013 <moibenko@fnal.gov> -
- new version 3.4.1-0
* Mon Aug 12 2013 <moibenko@fnal.gov> -
- new version 3.4.0-0
* Mon Feb 11 2013 <moibenko@fnal.gov> -
- new version 3.2.2-0
* Fri Oct 26 2012  <moibenko@fnal.gov> -
- new version 3.0.1-3
- use Python-enstore2.7-3.0.0-0.x86_64
* Fri Oct 26 2012  <moibenko@fnal.gov> -
- new version 3.0.0-3
* Mon Sep 10 2012  <moibenko@fnal.gov> -
- new version 3.0.0-2
* Thu Jul 26 2012  <moibenko@fnal.gov> -
- new version 3.0.0-1
* Thu Jun 07 2012  <moibenko@fnal.gov> -
- Added Small Files Aggregation
- new version 3.0.0-0
* Wed Jul 13 2011  <moibenko@fnal.gov> -
- new release 2.2.2-3
* Fri May 06 2011  <moibenko@fnal.gov> -
- new release 2.2.2-2
* Tue Feb 15 2011  <moibenko@fnal.gov> -
- new release 2.1.2-1
* Tue Feb 01 2011  <moibenko@fnal.gov> -
- new release
* Mon Jan 24 2011  <moibenko@fnal.gov> -
- new release
* Wed Nov 24 2010  <moibenko@fnal.gov> -
- new release
* Fri Oct 01 2010  <moibenko@fnal.gov> -
- new release
* Mon Jul 19 2010  <moibenko@fnal.gov> -
- new release
* Tue Jun 08 2010  <moibenko@fnal.gov> -
- using python with enabled ssl
* Tue Mar 23 2010  <moibenko@fnal.gov> -
- added dependency on postgresql-libs, because there now are many clients that require it
- added a message: Enstore installed. Please read README file
- changed Version to 2.0.1 and Release to 0
* Mon Nov 05 2007  <moibenko@fnal.gov> -
- added configuration files
* Fri Aug 17 2007  <moibenko@fnal.gov> -
- Copy enstore-setup file from config host if it exists there
- If "server" is specified, install additional rpms
* Thu Aug 16 2007  <moibenko@fnal.gov> -
- Moved creation of system files from create_enstore_environment.sh here
- Added enstore_monitor-boot
* Wed Feb 21 2007  <moibenko@fnal.gov> -
- Initial build.
