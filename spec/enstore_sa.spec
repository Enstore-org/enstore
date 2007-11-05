Summary: Enstore: Mass Storage System
Name: enstore_sa
Version: 1.0.1
Release: 9
Copyright: GPL
Group: System Environment/Base
Source: enstore_sa.tgz
BuildRoot: /usr/src/redhat/BUILD
AutoReqProv: no
AutoProv: no
AutoReq: no
Prefix: opt/enstore
Requires: Python-enstore, ftt

%description
Standalone Enstore. Enstore is a Distributed Mass Storage System. 
The main storage media it uses is magnetic tape, although the new media can be added.
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
echo PYTHONLIB=`ls -d $PYTHON_DIR/lib/python*` >> /tmp/enstore-setup
echo export PYTHONLIB >> /tmp/enstore-setup
echo FTT_DIR=`rpm -ql ftt | head -1` >> /tmp/enstore-setup
echo export FTT_DIR >> /tmp/enstore-setup

echo PATH="$"PYTHON_DIR/bin:"$"PATH >> /tmp/enstore-setup
#++++++++++++

%setup -q -c -n %{prefix}
#find . -name "CVS" | xargs rm -rf

%build
. /tmp/enstore-setup
echo "BUILD"
cd $RPM_BUILD_ROOT/%{prefix}/modules
make clean
make

%install
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
	chmod 755 ~enstore
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
