SHELL=/bin/sh

# need a robust way of finding freeze, especially for non-kits version of python
FREEZE=`find $(PYTHON_DIR) -name freeze.py -print` 
#FREEZE=`ls $(PYTHON_DIR)/Python-*/Tools/freeze/freeze.py`
#PYMODULES=`find $(PYTHON_DIR) -name Modules -print`
PYMODULES=$(PYTHON_DIR)/Python-*/Modules

# No static version for SunOS for now
LDFLAGS2=`if [ "_SunOS" = _$(MACH_OS) ]; then echo "-dy"; else echo "-static"; fi`

OSV=`uname -s r`

# just for testing
setenvs:
	echo "uname =" `uname`
	echo "LDFLAGS2 =" $(LDFLAGS2)

all:
	@echo nothing to build

clean:
	@ $(ENSTORE_DIR)/bin/enstoreClean
	rm -f *.pyc encp
	- rm -rf ENCPBIN_TEMP
	- rm -rf ENCPBIN
	- rm -rf ENSTORE_USER_BIN_TEMP

check:
	$(ENSTORE_DIR)/tools/mylint.py ./*.py
	$(ENSTORE_DIR)/tools/check_pct.py -w ./*.py

encp:
	(cd $(ENSTORE_DIR)/modules; make all)
	- rm -rf ENCPBIN_TEMP
	mkdir ENCPBIN_TEMP
	- rm -rf ENCPBIN
	mkdir ENCPBIN
	python $(FREEZE) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o ENCPBIN_TEMP encp_user.py
	(cd ENCPBIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp ENCPBIN_TEMP/encp_user	ENCPBIN/encp
	cp ../modules/enroute2	ENCPBIN
	rm -rf ENCPBIN_TEMP

	- rm -rf ENSTORE_USER_BIN_TEMP
	mkdir ENSTORE_USER_BIN_TEMP
	python $(FREEZE) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o ENSTORE_USER_BIN_TEMP enstore_user.py
	(cd ENSTORE_USER_BIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp ENSTORE_USER_BIN_TEMP/enstore_user      ENCPBIN/enstore
	rm -rf ENSTORE_USER_BIN_TEMP

	- rm -rf ENSYNC_BIN_TEMP
	mkdir ENSYNC_BIN_TEMP
	python $(FREEZE) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o ENSYNC_BIN_TEMP ensync.py
	(cd ENSYNC_BIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp ENSYNC_BIN_TEMP/ensync      ENCPBIN
	rm -rf ENSYNC_BIN_TEMP

fileinfo:
	- rm -rf FILEINFO_TEMP
	mkdir FILEINFO_TEMP
	- rm -rf FILEINFOBIN
	mkdir FILEINFOBIN
	python $(FREEZE) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o FILEINFO_TEMP fileinfo.py
	(cd FILEINFO_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp FILEINFO_TEMP/fileinfo FILEINFOBIN
	cp $(ENSTORE_DIR)/ups/fileinfo.table FILEINFOBIN
	- rm -rf FILEINFO_TEMP


#	- rm -rf MONITOR_CLIENT_BIN_TEMP
#	mkdir MONITOR_CLIENT_BIN_TEMP
#	python $(FREEZE) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o MONITOR_CLIENT_BIN_TEMP monitor_client.py
#	(cd MONITOR_CLIENT_BIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
#	cp MONITOR_CLIENT_BIN_TEMP/monitor_client      ENCPBIN/enmonitor
#	rm -rf MONITOR_CLIENT_BIN_TEMP

install: encp enstore_user
	cp encp $ENSTORE_DIR/bin
	cp enstore_user $ENSTORE_DIR/bin

