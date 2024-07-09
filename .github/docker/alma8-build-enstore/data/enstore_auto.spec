Summary: Enstore: Mass Storage System
Name: enstore
Version: 6.3.4
Release: 20.7.el8
License: GPL
Group: System Environment/Base
Source: enstore.tgz
BuildDir: rpmbuild/BUILD
BuildRoot: rpmbuild/BUILDROOT
AutoReqProv: no
AutoProv: no
AutoReq: no
Prefix: opt/enstore
Requires: mt-st,sg3_utils

%define __os_install_post %{nil}
%global __strip /bin/true
%global _missing_build_ids_terminate_build 0
%undefine __brp_mangle_shebangs
%define __brp_strip /bin/true
%define _build_id_links none
##%%define debug_package %{nil}
%global __arch_install_post %{nil}
# disable python_byte_compile
#to exit and show all macro expansions
##%%dump
#exit 1
%global _enable_debug_packages 1
%global _include_muinidebuginfo 1
%global _include_gdb_index 1
%global _build_id_links alldebug

%global __os_install_post %(echo '%{__os_install_post}' | sed -e 's!/usr/lib[^[:space:]]*/brp-python-bytecompile[[:space:]].*$!!g')
%description
Enstore Distributed Mass Storage System.
The main storage media it uses is magnetic tape, although the new media can be added.
Beginning with version 3.X File Aggregation Feature is added.
For the postinstallation and configuration instructions please see enstore/README

%prep
# check if all supporting rpms are installed
rpm -q Python-enstore-2.7.18-1.x86_64
if [ $? -ne 0 ]; then
	echo "Python-enstore2.7.18 is not installed"
	exit 1
fi

rpm -q swig
if [ $? -ne 0 ]; then
	echo "swig is not installed"
	exit 1
fi
mkdir -p $RPM_BUILD_ROOT/%{prefix}
cd $RPM_BUILD_DIR
echo "BUILD  $RPM_BUILD_DIR "
rm -rf enstore-setup

%setup -q -c -n %{prefix}
# copy all supporting products
cp -rp * $RPM_BUILD_ROOT/%{prefix}
pydir=/opt/python/python-enstore-2.7.18
#pydir=`rpm -ql Python-enstore2.7.18* | head -1`
PYTHON_DIR=$RPM_BUILD_ROOT/%{prefix}/Python
mkdir -p $PYTHON_DIR
mkdir -p Python
cp -rp $pydir/* $PYTHON_DIR
cp -rp $pydir/* Python
rm -rf $PYTHON_DIR/*.tgz
rm -rf Python/*.tgz
#echo "debug pwd=`pwd` ls=`ls`"
#exit 1
ftt_dir=$RPM_BUILD_DIR/opt/enstore/ftt
FTT_DIR=$RPM_BUILD_ROOT/%{prefix}/ftt
mkdir -p $FTT_DIR
cp -rp $ftt_dir/* $FTT_DIR
swigdir=`swig -swiglib`
SWIG_DIR=$RPM_BUILD_ROOT/%{prefix}/SWIG
mkdir -p $SWIG_DIR/swig_lib
mkdir -p SWIG/swig_lib
cp -rp $swigdir/* $SWIG_DIR/swig_lib
cp -rp $swigdir/* SWIG/swig_lib
cp /usr/bin/swig $SWIG_DIR
cp /usr/bin/swig SWIG
#cp -rp /usr/share/swig/3.0.12/* $SWIG_DIR/swig_lib
#cp -rp /usr/share/swig/3.0.12/* SWIG/swig_lib

# create a tepmorary setup file
#+++++++++++
echo PYTHON_DIR=`pwd`/opt/enstore/Python> /tmp/enstore-setup
echo export PYTHON_DIR >> /tmp/enstore-setup
echo PYTHONINC=`pwd`/opt/enstore/Python/include/python2.7 >> /tmp/enstore-setup
echo export PYTHONINC >> /tmp/enstore-setup
echo PYTHONLIB=`pwd`/opt/enstore/Python/lib/python2.7 >> /tmp/enstore-setup
echo LD_LIBRARY_PATH=`pwd`/opt/enstore/Python/lib >> /tmp/enstore-setup
echo export LD_LIBRARY_PATH >> /tmp/enstore-setup
echo export PYTHONLIB >> /tmp/enstore-setup
echo FTT_DIR=$ftt_dir >> /tmp/enstore-setup
echo export FTT_DIR >> /tmp/enstore-setup
echo ftt_dir=$ftt_dir >> /tmp/enstore-setup
echo export ftt_dir >> /tmp/enstore-setup
echo ENSTORE_DIR=`pwd`/opt/enstore >> /tmp/enstore-setup
echo export ENSTORE_DIR >> /tmp/enstore-setup
echo SWIG_DIR=`pwd`/opt/enstore/SWIG >> /tmp/enstore-setup
echo export SWIG_DIR >> /tmp/enstore-setup
echo SWIG_LIB=$SWIG_DIR/swig_lib >> /tmp/enstore-setup
echo export SWIG_LIB >> /tmp/enstore-setup
echo PATH="$"SWIG_DIR:"$"PYTHON_DIR/bin:/usr/pgsql-15/bin:"$"PATH >> /tmp/enstore-setup
%build
. /tmp/enstore-setup
echo "BUILD RPM"
cd $ftt_dir
echo now in `pwd`
make clean
cd ftt_lib
echo now in `pwd`
make all
cd ../ftt_test
echo now in `pwd`
make all 
cd $RPM_BUILD_DIR/opt/enstore
echo now in `pwd`
make clean
make all
##%%exit 1
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
if [ ! -f $RPM_BUILD_ROOT/etc/ld.so.conf.d/enstore.conf ];then
    echo /opt/enstore/ftt/lib >  $RPM_BUILD_ROOT/etc/ld.so.conf.d/enstore.conf
fi

mkdir -p $RPM_BUILD_ROOT/usr/local/etc/
mkdir -p $RPM_BUILD_ROOT/etc
echo INSTALL DONE
%pre
PATH=/usr/sbin:$PATH

echo 'Checking if group "enstore" exists'
getent group enstore >/dev/null || groupadd -g 6209 enstore
echo 'Checking if user "enstore" exists'
getent passwd enstore >/dev/null || useradd -u 5744 -g enstore enstore;chmod 775 ~enstore

# save existing enstore distribution
d=`date "+%%F-%T"`
if [ -d $RPM_BUILD_ROOT/%{prefix} ]; then
   echo "moving $RPM_BUILD_ROOT/%{prefix} to /tmp/enstore_backup.$d"
   mv $RPM_BUILD_ROOT/%{prefix} /tmp/enstore_backup.$d
fi

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
echo PATH="$"PYTHON_DIR/bin:/usr/pgsql-12/bin:"$"PATH >> /tmp/enstore-setup
echo export PATH  >> /tmp/enstore-setup
. /tmp/enstore-setup

if [ ! -e $ENSTORE_DIR/FTT ]; then
    ln -s $ENSTORE_DIR/ftt $ENSTORE_DIR/FTT
fi

#export ENSTORE_DIR=$RPM_BUILD_ROOT/%{prefix}

# copy qpid extras
cp -p /opt/enstore/etc/extra_python.pth $PYTHONLIB/site-packages

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
echo '                          ENSTORE_CONFIG_HOST ENSTORE_CONFIG_PORT ENSTORE_DIR ENSTORE_MAIL \' >> /etc/sudoers.e
echo '                          FTT_DIR KRBTKFILE ENSTORE_OUT ENSSH ENSCP"' >> /etc/sudoers.e
echo "Cmnd_Alias      PYTHON  = ${PYTHON_DIR}/bin/python" >> /etc/sudoers.e
echo "Cmnd_Alias      PIDKILL = ${ENSTORE_DIR}/bin/pidkill, ${ENSTORE_DIR}/bin/pidkill_s, /bin/kill" >> /etc/sudoers.e
echo "Cmnd_Alias      MOVER = ${ENSTORE_DIR}/sbin/mover" >> /etc/sudoers.e
echo "Cmnd_Alias      MIGRATOR = ${ENSTORE_DIR}/sbin/migrator" >> /etc/sudoers.e
echo "enstore ALL=NOPASSWD:PYTHON, NOPASSWD:PIDKILL, NOPASSWD:MOVER, NOPASSWD:MIGRATOR" >> /etc/sudoers.e
rm -f /etc/sudoers
cp /etc/sudoers.e /etc/sudoers
chmod 440 /etc/sudoers

echo "Copying $ENSTORE_DIR/bin/enstore-boot.service.SLF7 to /usr/lib/systemd/system/enstore.service"
cp -f $ENSTORE_DIR/etc/enstore.service /usr/lib/systemd/system/enstore.service
echo "Configuring the system to start enstore on boot"
systemctl is-enabled enstore.service
if [ $? -ne 0 ]; then
    systemctl enable enstore.service
fi
/etc/rc.d/init.d/enstore-boot install
echo "Copying $ENSTORE_DIR/bin/monitor-boot.service.SLF7 to /usr/lib/systemd/system/enstore-monitor.service"
cp -f $ENSTORE_DIR/etc/enstore-monitor.service /usr/lib/systemd/system/enstore-monitor.service
echo "Configuring the system to start monitor server on boot"
systemctl is-enabled enstore-monitor.service
if [ $? -ne 0 ]; then
    systemctl enable enstore-monitor.service
fi
cp -f $ENSTORE_DIR/sbin/rc.local /etc/rc.d
chmod +x /etc/rc.d/rc.local
echo "Updating symbolic links"
$ENSTORE_DIR/external_distr/update_sym_links.sh
if [ ! -d ~enstore/config ]; then
   echo "Creating default output directory: /var/log/enstore"
   mkdir -p /var/log/enstore
   chown enstore.enstore /var/log/enstore
fi
#rm -f $ENSTORE_DIR/debugfiles.list
#rm -f $ENSTORE_DIR/debugsources.list
#rm /tmp/enstore-setup
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

%changelog
* Wed Feb 02 2022  <moibenko@fnal.gov> -
- v 6.3.4 release 15. Accumulative changes since 6.3.4-14.
* Thu Oct 07 2021  <moibenko@fnal.gov> -
- v 6.3.4 release 14. Accumulative changes since 6.3.4-11.
* Tue Aug 31 2021  <moibenko@fnal.gov> -
- v 6.3.4 release 11. Spectra Logic media changer included.
* Thu Aug 05 2021  <moibenko@fnal.gov> -
- v 6.3.4 release 10. Accumulative changes since 6.3.4-9
* Fri May 14 2021  <moibenko@fnal.gov> -
- v 6.3.4 release 9. Accumulative changes since 6.3.4-8
* Mon Apr 26 2021  <moibenko@fnal.gov> -
- v 6.3.4 release 8. Accumulative changes since 6.3.4-7
* Thu Feb 18 2021  <moibenko@fnal.gov> -
- v 6.3.4 release 7. Accumulative changes since 6.3.4-6
* Thu Jan 07 2021  <moibenko@fnal.gov> -
- v 6.3.4 release 6. Accumulative changes since 6.3.4-5
* Thu Dec 17 2020  <moibenko@fnal.gov> -
- v 6.3.4 release 5. Accumulative changes since 6.3.4-4
* Tue Oct 27 2020  <moibenko@fnal.gov> -
- v 6.3.4 release 4. Accumulative changes since 6.3.4-3
* Tue Aug 18 2020  <moibenko@fnal.gov> -
- v 6.3.4 release 3. Accumulative changes since 6.3.4-2
* Tue Apr 07 2020  <moibenko@fnal.gov> -
- v 6.3.4 release 2. Accumulative changes since 6.3.4-1
* Thu Mar 12 2020  <moibenko@fnal.gov> -
- v 6.3.4 release 1. Accumulative changes since 6.3.4-0
* Mon Feb 03 2020  <moibenko@fnal.gov> -
- v 6.3.4 release 0. Accumulative changes since 6.3.3-2
* Tue Dec 17 2019  <moibenko@fnal.gov> -
- v 6.3.3 release 2. Unofficial release with fixes for communincation of dual stak IPs with IPv4 only
* Wed Dec 04 2019  <moibenko@fnal.gov> -
- v 6.3.3 release 0. Same as 6.3.2.2, but with new python
* Wed Nov 13 2019  <moibenko@fnal.gov> -
- v 6.3.2 release 1. Modified media changer to use mtx unit_test calls
* Fri Nov 08 2019  <moibenko@fnal.gov> -
- v 6.3.2 release 0. Same as  v 6.3.1 release 17 but with new mtx
* Wed Nov 6 2019  <moibenko@fnal.gov> -
- v 6.3.1 release 17. Accumulative changes since 6.3.1-16
* Mon Sep 30 2019  <moibenko@fnal.gov> -
- v 6.3.1 release 16. Accumulative changes since 6.3.1-12
* Fri May 24 2019  <moibenko@fnal.gov> -
- v 6.3.1 release 12. Increase dismout delay
* Fri May 10 2019  <moibenko@fnal.gov> -
- v 6.3.1 release 11. Accumulative changes since 6.3.1-10
* Wed May 1 2019  <moibenko@fnal.gov> -
- v 6.3.1 release 10. Accumulative changes since 6.3.1-5
* Thu Apr 25 2019  <moibenko@fnal.gov> -
- v 6.3.1 release 9. Changed mover to send retrialble error to encp for CRC error in write_client
* Wed Mar 27 2019  <moibenko@fnal.gov> -
- v 6.3.1 release 7. Migration code changes.
* Tue Mar 26 2019  <moibenko@fnal.gov> -
- v 6.3.1 release 6. Migration code changes.
* Wed Mar 13 2019  <moibenko@fnal.gov> -
- v 6.3.1 release 5. Accumulative changes.
* Wed Jan 23 2019  <moibenko@fnal.gov> -
- v 6.3.1 release 0. Uses new mtx rpm which updates elemets structure every time status is called.
* Mon Jan 14 2019  <moibenko@fnal.gov> -
- v 6.3.0 release 13. Fixed listSlots to not fail on keyerror.
* Tue Jan 8 2019  <moibenko@fnal.gov> -
- v 6.3.0 release 12. Added listClean and listVolumes to MTXN media changer
* Tue Jan 8 2019  <moibenko@fnal.gov> -
- v 6.3.0 release 11. Restored MC lost changes.
* Fri Jan 4 2019  <moibenko@fnal.gov> -
- v 6.3.0 release 10. LM fair share change. Process DNS resolution errors.
* Thu Dec 20 2018  <moibenko@fnal.gov> -
- v 6.3.0 release 9. Create log dir if it does not exist
* Wed Dec 05 2018  <moibenko@fnal.gov> -
- v 6.3.0 release 8. Added more retry cases
* Mon Nov 05 2018  <moibenko@fnal.gov> -
- v 6.3.0 release 7. Ignore media type in mtx mount/dismount
* Thu Nov 01 2018  <moibenko@fnal.gov> -
- v 6.3.0 release 6. Fixed bugs in media changer mount / dismount retries. Noe enstore_display for entv without lookup calls.
* Tue Oct 30 2018  <moibenko@fnal.gov> -
- v 6.3.0 release 5. Fixed mover.py. Disk mover had exception due to missing loc_mover var.
* Mon Oct 29 2018  <moibenko@fnal.gov> -
- v 6.3.0 release 4. Added retries on mount / dismount failures with error processing.
* Thu Oct 25 2018  <moibenko@fnal.gov> -
- v 6.3.0 release 3. included LTO8 into ftt_tables and enabled compression. MC mods
* Tue Oct 23 2018  <moibenko@fnal.gov> -
- v 6.3.0 release 2. MC: adopt new ACSSA CLI version, reload loaction info on retry
* Thu Sep 27 2018  <moibenko@fnal.gov> -
- v 6.3.0 release 1. bug fix in mover to not stop media changer on exit
* Fri Sep 7 2018  <moibenko@fnal.gov> -
- v 6.3.0 release 0. added new mtx support for TS4500 robotic library (stacker actually)
* Mon Jul 16 2018  <moibenko@fnal.gov> -
- v 6.2.0 release 0. - last release, which does not include new code for TS4500
* Mon Apr 16 2018  <moibenko@fnal.gov> -
- v 6.1.0 release 6.
* Tue Oct 3 2017  <moibenko@fnal.gov> -
- v 6.1.0 release 5.
* Tue Aug 29 2017  <moibenko@fnal.gov> -
- v 6.1.0 release 4.
* Fri Jul 7 2017  <moibenko@fnal.gov> -
- v 6.1.0 release 3.
* Thu May 4 2017  <moibenko@fnal.gov> -
- v 6.1.0 release 2.
* Tue Mar 28 2017  <moibenko@fnal.gov> -
- v 6.1.0 release 1.
* Wed Dec 28 2016  <moibenko@fnal.gov> -
- v 6.1.0 release 0.
* Mon Dec 12 2016  <moibenko@fnal.gov> -
- v 6.0.0 release 15. edb.py patch
* Thu Dec 1 2016  <moibenko@fnal.gov> -
- v 6.0.0 release 12. Fixed udp_client.py
* Tue Nov 29 2016  <moibenko@fnal.gov> -
- v 6.0.0 release 11. Fixes for IPV6 cleint to IPV4 server communications
* Wed Nov 16 2016  <moibenko@fnal.gov> -
- v 6.0.0 release 10. Forgot to include mover.py
* Fri Nov 11 2016  <moibenko@fnal.gov> -
- v 6.0.0 release 9. More fixes for IPV4 <-> IPV6
* Thu Nov 10 2016  <moibenko@fnal.gov> -
- v 6.0.0 release 8. More fixes for IPV4 <-> IPV6
* Wed Nov 9 2016  <moibenko@fnal.gov> -
- v 6.0.0 release 7. More fixes for IPV4 <-> IPV6
* Tue Nov 8 2016  <moibenko@fnal.gov> -
- v 6.0.0 release 6. Fixes for IPV4 <-> IPV6
* Tue Nov 1 2016  <moibenko@fnal.gov> -
- v 6.0.0 release 5.
* Tue Oct 25 2016  <moibenko@fnal.gov> -
- v 6.0.0 release 3.
* Mon Oct 3 2016  <moibenko@fnal.gov> -
- v 6.0.0 release 2.
* Fri Aug 26 2016  <moibenko@fnal.gov> -
- udp_common.py change was missing
* Wed Aug 3 2016  <moibenko@fnal.gov> -
- First official release with IPV6 support v 6.0.0, release 0.
* Wed Jul 13 2016  <moibenko@fnal.gov> -
- minor change v 5.1.3, release 5.
* Wed Jul 13 2016  <moibenko@fnal.gov> -
- do not remove /opt/enstore v 5.1.3, release 4.
* Wed Jul 13 2016  <moibenko@fnal.gov> -
- do not remove /opt/enstore v 5.1.3, release 3.
* Wed Jul 13 2016  <moibenko@fnal.gov> -
- minor change v 5.1.3, release 2.
* Tue Jul 12 2016  <moibenko@fnal.gov> -
- preliminary release with IPV6 support v 5.1.3, release 1.
* Tue May 10 2016  <moibenko@fnal.gov> -
- new release 5.1.3, release 0.
* Wed Feb 10 2016  <moibenko@fnal.gov> -
- new release 5.1.2, release 0.
* Mon Nov 09 2015  <moibenko@fnal.gov> -
- new release 5.1.1, release 1.
* Mon Nov 09 2015  <moibenko@fnal.gov> -
- new release 5.1.1, release 0.
* Fri Sep 25 2015  <moibenko@fnal.gov> -
- new release 5.1.0, release 3.
* Fri Jun 05 2015  <moibenko@fnal.gov> -
- new release 5.1.0, release 1.
* Tue May 12 2015  <moibenko@fnal.gov> -
- started using python 2.7.9 rpm
- new version 5.0.0, release 0.
