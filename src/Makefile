SHELL=/bin/sh

# need a robust way of finding freeze, especially for non-kits version of python
FREEZE=`find $(PYTHON_DIR) -name freeze.py -print`
#FREEZE=`ls $(PYTHON_DIR)/Python-*/Tools/freeze/freeze.py`
#PYMODULES=`find $(PYTHON_DIR) -name Modules -print`
PYMODULES=$(PYTHON_DIR)/Python-*/Modules
PYLIB=$(PYTHON_DIR)/lib/python2.*/lib-dynload

# Need to build python with the freeze -E option (new with python 2.2.3).
# This option forces all modules to be frozen.  This forces frozen python 
# executables to keep from looking for external *.py and *.pyc files.
# However, there are always exceptions.  Those are the -x options.  Those
# modules listed as -x are to be excluded from the frozen binary (though if
# they do exist they will be included).  These mostly have to do with non-UNIX
# OSes that won't have modules be there when running on UNIX.
FREEZE_SKIP_OS=-x ce -x dos -x mac -x nt -x os2 -x riscos -x riscosenviron -x riscospath
FREEZE_SKIP_HTML=-x GimpGradientFile -x GimpPaletteFile -x Image -x ImageTk -x PaletteFile -x _imaging -x jarray
FREEZE_SKIP_MISC=-x MacOS -X sitecustomize -x _winreg -x ic -x rourl2path -x EasyDialogs -x msvcrt -x MACFS -x macfs -x SOCKS -x org.python.core -X org -X xreadlines -x Carbon.File -x Carbon.Folder -x Carbon.Folders -x os.path -x _emx_link

FREEZE_ENCP_OPTIONS=-E $(FREEZE_SKIP_OS) $(FREEZE_SKIP_MISC)
FREEZE_ENSTORE_OPTIONS=-E $(FREEZE_SKIP_OS) $(FREEZE_SKIP_MISC) $(FREEZE_SKIP_HTML)

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
	- rm -rf GET_BIN
	- rm -rf GET_TEMP
	- rm -rf ENTV_BIN
	- rm -rf ENTV_TEMP
	- rm -rf ENMV_BIN_TEMP
	- rm -rf ENSYNC_BIN_TEMP

check:
	$(ENSTORE_DIR)/tools/mylint.py ./*.py
	$(ENSTORE_DIR)/tools/check_pct.py -w ./*.py

### The following groupings for for building the client side programs
### intended for release to the general public.
### 
### The two commands of interest are "make encp" and "make encp_for_dcache".

pre_encp_cmd:
	(cd $(ENSTORE_DIR)/modules; make all)
	- rm -rf ENCPBIN_TEMP
	mkdir ENCPBIN_TEMP
	- rm -rf ENCPBIN
	mkdir ENCPBIN
	- rm -rf ENSTORE_USER_BIN_TEMP
	mkdir ENSTORE_USER_BIN_TEMP

encp_cmd:
	python $(FREEZE) $(FREEZE_ENCP_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o ENCPBIN_TEMP encp_user.py
	(cd ENCPBIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp ENCPBIN_TEMP/encp_user	ENCPBIN/encp

encp_cmd_for_dcache:
	python $(FREEZE) $(FREEZE_ENCP_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o ENCPBIN_TEMP encp_user2.py
	(cd ENCPBIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp ENCPBIN_TEMP/encp_user2	ENCPBIN/encp

post_encp_cmd:
	rm -rf ENCPBIN_TEMP
	rm -rf ENSTORE_USER_BIN_TEMP

	cp ../modules/enroute2	ENCPBIN
	cp ../modules/ddencp	ENCPBIN
	cp ../modules/ecrc	ENCPBIN
	cp ../bin/en_check	ENCPBIN

enstore_cmd:
	python $(FREEZE) $(FREEZE_ENSTORE_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o ENSTORE_USER_BIN_TEMP enstore_user.py
	(cd ENSTORE_USER_BIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp ENSTORE_USER_BIN_TEMP/enstore_user      ENCPBIN/enstore

enstore_cmd_for_dcache:
	python $(FREEZE) $(FREEZE_ENSTORE_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o ENSTORE_USER_BIN_TEMP enstore_user2.py
	(cd ENSTORE_USER_BIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp ENSTORE_USER_BIN_TEMP/enstore_user2      ENCPBIN/enstore

ensync_cmd:
	- rm -rf ENSYNC_BIN_TEMP
	mkdir ENSYNC_BIN_TEMP
	python $(FREEZE) $(FREEZE_ENCP_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o ENSYNC_BIN_TEMP ensync_user.py
	(cd ENSYNC_BIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp ENSYNC_BIN_TEMP/ensync_user      ENCPBIN/ensync
	rm -rf ENSYNC_BIN_TEMP

encp: pre_encp_cmd encp_cmd enstore_cmd ensync_cmd post_encp_cmd

encp_for_dcache: pre_encp_cmd encp_cmd_for_dcache enstore_cmd_for_dcache ensync_cmd post_encp_cmd

### The following groupings for for building the server side programs
### intended for use by the Enstore administrators.
### 
### The command of interest is "make enstore".

pre_enstore:
	@ if [ "$$FTT_DIR"x = x ]; then echo FTT_DIR must be set to build enstore.; exit 1; fi
	@ if [ "$$ACI_DIR"x = x ]; then echo ACI_DIR must be set to build enstore.; exit 1; fi
	(cd $(ENSTORE_DIR)/modules; make all)
	- rm -rf ENSTORE_BIN
	mkdir ENSTORE_BIN

post_enstore:
	mv ENSTORE_BIN/encp_admin ENSTORE_BIN/encp
	mv ENSTORE_BIN/enstore_admin ENSTORE_BIN/enstore
	mv ENSTORE_BIN/ensync_admin ENSTORE_BIN/ensync

	cp ../modules/enroute2	ENSTORE_BIN
	cp ../modules/ddencp	ENSTORE_BIN
	cp ../modules/ecrc	ENSTORE_BIN
	cp ../bin/en_check	ENSTORE_BIN

SERVER_LIST=configuration_server log_server event_relay library_manager
SERVER_HTMLGEN_LIST=alarm_server inquisitor ratekeeper monitor_server
SERVER_SQL_LIST=file_clerk volume_clerk info_server accounting_server drivestat_server
SERVER_LIB_LIST=media_changer mover

FULL_SERVER_LIST=$(SERVER_LIST) $(SERVER_HTMLGEN_LIST) $(SERVER_SQL_LIST) $(SERVER_LIB_LIST)
FULL_CLIENT_LIST=encp_admin enstore_admin ensync_admin

$(FULL_SERVER_LIST): $$@.py
	- rm -rf SERVER_BIN_TEMP 
	mkdir SERVER_BIN_TEMP
	python $(FREEZE) $(FREEZE_ENSTORE_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o SERVER_BIN_TEMP $@.py
	(cd SERVER_BIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp SERVER_BIN_TEMP/$@      ENSTORE_BIN/$@
	rm -rf SERVER_BIN_TEMP

$(FULL_CLIENT_LIST): $$@.py
	- rm -rf CLIENT_BIN_TEMP
	mkdir CLIENT_BIN_TEMP
	python $(FREEZE) $(FREEZE_ENSTORE_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o CLIENT_BIN_TEMP $@.py
	(cd CLIENT_BIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp CLIENT_BIN_TEMP/$@      ENSTORE_BIN/$@
	rm -rf CLIENT_BIN_TEMP

enstore: pre_enstore $(FULL_CLIENT_LIST) $(FULL_SERVER_LIST) post_enstore


#- rm -rf ENMV_BIN_TEMP
#mkdir ENMV_BIN_TEMP
#python $(FREEZE) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o ENMV_BIN_TEMP enmv.py
#(cd ENMV_BIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
#cp ENMV_BIN_TEMP/enmv      ENCPBIN
#rm -rf ENMV_BIN_TEMP

fileinfo:
	- rm -rf FILEINFO_TEMP
	mkdir FILEINFO_TEMP
	- rm -rf FILEINFOBIN
	mkdir FILEINFOBIN
	python $(FREEZE) $(FREEZE_ENCP_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o FILEINFO_TEMP fileinfo.py
	(cd FILEINFO_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp FILEINFO_TEMP/fileinfo FILEINFOBIN
	cp $(ENSTORE_DIR)/ups/fileinfo.table FILEINFOBIN
	- rm -rf FILEINFO_TEMP

get:
	(cd $(ENSTORE_DIR)/modules; make all)
	- rm -rf GET_TEMP
	mkdir GET_TEMP
	- rm -rf GET_BIN
	mkdir GET_BIN
	python $(FREEZE) $(FREEZE_ENCP_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o GET_TEMP get.py
	(cd GET_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)

	cp GET_TEMP/get GET_BIN
	cp ../ups/chooseConfig GET_BIN
	cp ../modules/enroute2 GET_BIN
	sed -e 's/encp/get/g' $(ENSTORE_DIR)/ups/encp.table > GET_BIN/get.table
	- rm -rf GET_TEMP

entv:
	(cd $(ENSTORE_DIR)/modules; make all mouse_move autologinenstore)
	- rm -rf ENTV_TEMP
	mkdir ENTV_TEMP
	- rm -rf ENTV_BIN
	mkdir ENTV_BIN
	python $(FREEZE) $(FREEZE_ENCP_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o ENTV_TEMP entv.py
	(cd ENTV_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp ENTV_TEMP/entv ../ups/chooseConfig ENTV_BIN
	if [ -n "$(ENSTORE_DEVEL_DIR)" ]; then \
		tcltk_dirs=`ls -ld $(ENSTORE_DEVEL_DIR)/tcltk/lib/tcl* $(ENSTORE_DEVEL_DIR)/tcltk/lib/tk* | egrep "^d" | awk '{print $$9}'` && \
		cp -r $$tcltk_dirs ENTV_BIN ; \
		cp -r $(ENSTORE_DIR)/etc/Images ENTV_BIN ; \
	else \
		cp -r $(ENSTORE_DIR)/etc/TclTk/tcl8.3  $(ENSTORE_DIR)/etc/TclTk/tk8.3 $(ENSTORE_DIR)/etc/Images ENTV_BIN ; \
	fi
	cp $(ENSTORE_DIR)/modules/autologinenstore ENTV_BIN
	cp $(ENSTORE_DIR)/modules/mouse_move ENTV_BIN
	cp $(ENSTORE_DIR)/etc/entvrc ENTV_BIN
	cp ../modules/enroute2 ENTV_BIN
	sed -e 's/encp/entv/g' $(ENSTORE_DIR)/ups/encp.table > ENTV_BIN/entv.table
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

