SHELL=/bin/sh

# need a robust way of finding freeze, especially for non-kits version of python
FREEZE=`find $(PYTHON_DIR) -name freeze.py -print` 
#FREEZE=`ls $(PYTHON_DIR)/Python-*/Tools/freeze/freeze.py`
#PYMODULES=`find $(PYTHON_DIR) -name Modules -print`
PYMODULES=$(PYTHON_DIR)/Python-*/Modules

# No static version for SunOS for now
# No static version for any one any more ... since v3_0
# LDFLAGS2=`if [ "_SunOS" = _$(MACH_OS) ]; then echo "-dy"; else echo "-static"; fi`
LDFLAGS2=

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
	cp ../modules/ddencp	ENCPBIN
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

get:
	(cd $(ENSTORE_DIR)/modules; make all)
	- rm -rf GET_TEMP
	mkdir GET_TEMP
	- rm -rf GETBIN
	mkdir GETBIN
	python $(FREEZE) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o GET_TEMP get.py
	(cd GET_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp GET_TEMP/get ../ups/chooseConfig GETBIN
	sed -e 's/encp/get/g' $(ENSTORE_DIR)/ups/encp.table > GETBIN/get.table
	- rm -rf GET_TEMP

entv:
	(cd $(ENSTORE_DIR)/modules; make all mouse_move autologinenstore)
	- rm -rf ENTV_TEMP
	mkdir ENTV_TEMP
	- rm -rf ENTVBIN
	mkdir ENTVBIN
	python $(FREEZE) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o ENTV_TEMP entv.py
	(cd ENTV_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp ENTV_TEMP/entv ../ups/chooseConfig ENTVBIN
	cp -r $(ENSTORE_DIR)/etc/TclTk/tcl8.3  $(ENSTORE_DIR)/etc/TclTk/tk8.3 $(ENSTORE_DIR)/etc/Images ENTVBIN
	cp $(ENSTORE_DIR)/modules/autologinenstore ENTVBIN
	cp $(ENSTORE_DIR)/modules/mouse_move ENTVBIN
	cp $(ENSTORE_DIR)/etc/entvrc ENTVBIN
	sed -e 's/encp/entv/g' $(ENSTORE_DIR)/ups/encp.table > ENTVBIN/entv.table
	- rm -rf ENTV_TEMP


#	- rm -rf MONITOR_CLIENT_BIN_TEMP
#	mkdir MONITOR_CLIENT_BIN_TEMP
#	python $(FREEZE) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o MONITOR_CLIENT_BIN_TEMP monitor_client.py
#	(cd MONITOR_CLIENT_BIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
#	cp MONITOR_CLIENT_BIN_TEMP/monitor_client      ENCPBIN/enmonitor
#	rm -rf MONITOR_CLIENT_BIN_TEMP

# This install never works!
install: encp enstore_user
	cp encp $ENSTORE_DIR/bin
	cp enstore_user $ENSTORE_DIR/bin

