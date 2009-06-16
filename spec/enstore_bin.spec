Summary: Enstore: Mass Storage System
Name: enstore
Version: 1.b.1
Release: 8
Copyright: GPL
Group: System Environment/Base
Source: enstore.tgz
BuildRoot: /usr/src/redhat/BUILD
AutoReqProv: no
AutoProv: no
AutoReq: no
Prefix: opt/enstore_bin
Requires: Python-enstore, ftt

%description
Standalone Enstore. Enstore is a Distributed Mass Storage System. 
The main storage media it uses is magnetic tape, although the new media can be added.
This rpm has more functionality such as ADIC robot interface.
For the postinstallation and configuration instructions please see enstore/README

%prep
# create a tepmorary setup file
#+++++++++++
cd $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT/%{prefix}
rm -rf enstore-setup
PYTHON_DIR=`rpm -ql Python-enstore | head -1`
echo PYTHON_DIR=`rpm -ql Python-enstore | head -1`> /tmp/enstore-setup
echo export PYTHON_DIR >> /tmp/enstore-setup
echo PYTHONINC=`ls -d $PYTHON_DIR/include/python*`>> /tmp/enstore-setup
echo export PYTHONINC >> /tmp/enstore-setup

#pm1=`ls -d $PYTHON_DIR/lib/python*`
#pm2=$RPM_BUILD_ROOT/%{prefix}/modules
#echo PYTHONLIB=$pm2:$pm1 >> /tmp/enstore-setup
echo PYTHONLIB=`ls -d $PYTHON_DIR/lib/python*` >> /tmp/enstore-setup
echo export PYTHONLIB >> /tmp/enstore-setup
echo PYTHONPATH=$RPM_BUILD_ROOT/%{prefix}:$RPM_BUILD_ROOT/%{prefix}/src:$RPM_BUILD_ROOT/%{prefix}/modules:$RPM_BUILD_ROOT/%{prefix}/HTMLgen >> /tmp/enstore-setup
echo export PYTHONPATH >> /tmp/enstore-setup
echo FTT_DIR=`rpm -ql ftt | head -1` >> /tmp/enstore-setup
echo export FTT_DIR >> /tmp/enstore-setup
echo ENSTORE_DIR=$RPM_BUILD_ROOT/%{prefix} >> /tmp/enstore-setup
echo export ENSTORE_DIR >> /tmp/enstore-setup

rpm -q swig-enstore > /dev/null
if [ $? -eq 0 ]; then
	swigdir=`rpm -ql swig-enstore | head -1`
	echo SWIG_DIR=$swigdir >> /tmp/enstore-setup
	echo export SWIG_DIR >> /tmp/enstore-setup
	echo SWIG_LIB=$swigdir/swig_lib >> /tmp/enstore-setup
	echo export SWIG_LIB >> /tmp/enstore-setup
else
	echo SWIG_DIR=/home/moibenko/enstore_products/swig/swig1.1-883/SWIG1.1-883 >> /tmp/enstore-setup
	echo export SWIG_DIR
	echo SWIG_LIB=/home/moibenko/enstore_products/swig/swig1.1-883/SWIG1.1-883/swig_lib >> /tmp/enstore-setup
	echo export SWIG_LIB
fi
echo PATH="$"SWIG_DIR:"$"PYTHON_DIR/bin:$RPM_BUILD_ROOT/%{prefix}/bin:$RPM_BUILD_ROOT/%{prefix}/sbin:"$"PATH >> /tmp/enstore-setup

#rpm -q aci > /dev/null
#if [ $? -eq 0 ]; then
#	echo ACI_DIR=`rpm -ql aci | head -1` >> /tmp/enstore-setup
#	echo export ACI_DIR >> /tmp/enstore-setup
#	echo PATH="$"ACI_DIR:"$"PATH >> /tmp/enstore-setup
#fi
#++++++++++++

%setup -q -c -n %{prefix}
#find . -name "CVS" | xargs rm -rf

%build
. /tmp/enstore-setup
echo "BUILD"
cd $RPM_BUILD_ROOT/%{prefix}/src
make enstore
make entv
for f in `ls -1 ENTV_BIN`; do
	rm -f $f.py
done
for f in `ls -1 ENSTORE_BIN/bin`; do
	rm -f $f.py
done
for f in `ls -1 ENSTORE_BIN/sbin`; do
	if [ $f != "configuration_server" ]; then
		rm -f $f.py
	fi
done


%install
cp -p $RPM_BUILD_ROOT/%{prefix}/src/ENSTORE_BIN/bin/* $RPM_BUILD_ROOT/%{prefix}/bin
cp -p $RPM_BUILD_ROOT/%{prefix}/src/ENSTORE_BIN/sbin/* $RPM_BUILD_ROOT/%{prefix}/sbin
cp -p $RPM_BUILD_ROOT/%{prefix}/src/ENTV_BIN/entv $RPM_BUILD_ROOT/%{prefix}/bin


if [ ! -d $RPM_BUILD_ROOT/usr/local/etc ]; then
	mkdir -p $RPM_BUILD_ROOT/usr/local/etc
fi
cp -r $RPM_BUILD_ROOT/%{prefix}/external_distr/setups.sh $RPM_BUILD_ROOT/usr/local/etc/setups.sh

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
rm -rf /tmp/enstore-setup
PYTHON_DIR=`rpm -ql Python-enstore | head -1`
echo PYTHON_DIR=`rpm -ql Python-enstore | head -1`> /tmp/enstore-setup
echo export PYTHON_DIR >> /tmp/enstore-setup
echo PYTHONINC=`ls -d $PYTHON_DIR/include/python*`>> /tmp/enstore-setup
echo export PYTHONINC >> /tmp/enstore-setup
echo PYTHONLIB=`ls -d $PYTHON_DIR/lib/python*` >> /tmp/enstore-setup
echo export PYTHONLIB >> /tmp/enstore-setup
echo FTT_DIR=`rpm -ql ftt | head -1` >> /tmp/enstore-setup
echo export FTT_DIR >> /tmp/enstore-setup


echo PATH="$"PYTHON_DIR/bin:"$"PATH >> /tmp/enstore-setup
. /tmp/enstore-setup
#chown -R enstore.enstore /home/enstore
export ENSTORE_DIR=$RPM_BUILD_ROOT/%{prefix}
echo "Creating sudoers file"
echo "The original is saved into /etc/sudoers.enstore_save"
if [ ! -f /etc/sudoers.enstore_save ]; then
    cp /etc/sudoers /etc/sudoers.enstore_save
fi
cp /etc/sudoers.enstore_save /etc/sudoers.e
chmod 740 /etc/sudoers.e


echo "Cmnd_Alias      PYTHON  = ${PYTHON_DIR}/bin/python" >> /etc/sudoers.e
echo "Cmnd_Alias      PIDKILL = ${ENSTORE_DIR}/bin/pidkill, ${ENSTORE_DIR}/bin/pidkill_s, /bin/kill" >> /etc/sudoers.e
echo "Cmnd_Alias      MOVER = ${ENSTORE_DIR}/sbin/mover" >> /etc/sudoers.e
echo "enstore ALL=NOPASSWD:PYTHON, NOPASSWD:PIDKILL, NOPASSWD:MOVER" >> /etc/sudoers.e
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
rm -f $ENSTORE_DIR/debugfiles.list
rm -f $ENSTORE_DIR/debugsources.list
rm /tmp/enstore-setup

%preun
$RPM_BUILD_ROOT/%{prefix}/external_distr/rpm_uninstall.sh
%clean
rm -rf $RPM_BUILD_ROOT/*

%files
%defattr(-,enstore,enstore,-)
%doc
/%{prefix}
%config /%{prefix}/etc/enstore_configuration
%config /%{prefix}/etc/sam.conf
%config /%{prefix}/etc/stk.conf
%config /%{prefix}/etc/d0en_sde_test.conf
%config /usr/local/etc/setups.sh
#/etc/rc.d/init.d/enstore-boot
#/etc/sudoers
#/home/enstore/debugfiles.list
#/home/enstore/debugsources.list
%changelog
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
