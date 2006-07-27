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
FREEZE_SKIP_HTML=-x GimpGradientFile -x GimpPaletteFile -x Image -x ImageTk -x PaletteFile -x _imaging -x jarray -x 
FREEZE_SKIP_MISC=-x MacOS -X sitecustomize -x _winreg -x ic -x rourl2path -x EasyDialogs -x msvcrt -x MACFS -x macfs -x SOCKS -x org.python.core -X org -X xreadlines 

FREEZE_ENCP_OPTIONS=-E $(FREEZE_SKIP_OS) $(FREEZE_SKIP_MISC)
FREEAE_ENSTORE_OPTIONS=-E $(FREEZE_SKIP_OS) $(FREEZE_SKIP_MISC) $(FREEZE_SKIP_HTML)

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

pre_encp:
	(cd $(ENSTORE_DIR)/modules; make all)
	- rm -rf ENCPBIN_TEMP
	mkdir ENCPBIN_TEMP
	- rm -rf ENCPBIN
	mkdir ENCPBIN

encp_cmd_build:
	python $(FREEZE) $(FREEZE_ENCP_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o ENCPBIN_TEMP encp_user.py
	(cd ENCPBIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp ENCPBIN_TEMP/encp_user	ENCPBIN/encp

encp_cmd_build_for_dcache:
	python $(FREEZE) $(FREEZE_ENCP_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o ENCPBIN_TEMP encp_user2.py
	(cd ENCPBIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp ENCPBIN_TEMP/encp_user2	ENCPBIN/encp

post_encp:
	rm -rf ENCPBIN_TEMP

	cp ../modules/enroute2	ENCPBIN
	cp ../modules/ddencp	ENCPBIN
	cp ../modules/ecrc	ENCPBIN
	cp ../bin/en_check	ENCPBIN

	- rm -rf ENSTORE_USER_BIN_TEMP
	mkdir ENSTORE_USER_BIN_TEMP
	python $(FREEZE) $(FREEZE_ENSTORE_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o ENSTORE_USER_BIN_TEMP enstore_user.py
	(cd ENSTORE_USER_BIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp ENSTORE_USER_BIN_TEMP/enstore_user      ENCPBIN/enstore
	rm -rf ENSTORE_USER_BIN_TEMP

	- rm -rf ENSYNC_BIN_TEMP
	mkdir ENSYNC_BIN_TEMP
	python $(FREEZE) $(FREEZE_ENCP_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o ENSYNC_BIN_TEMP ensync.py
	(cd ENSYNC_BIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp ENSYNC_BIN_TEMP/ensync      ENCPBIN
	rm -rf ENSYNC_BIN_TEMP

encp: pre_encp encp_cmd_build post_encp

encp_for_dcache: pre_encp encp_cmd_build_for_dcache post_encp

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

