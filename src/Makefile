SHELL=/bin/sh

CP=cp -p
MV=mv
RM=rm -rf

# need a robust way of finding freeze, especially for non-kits version of python
FREEZE=`find $(PYTHON_DIR) -name freeze.py -print`
PYMODULES=$(PYTHON_DIR)/Python-*/Modules
PYLIB=$(PYTHON_DIR)/lib/python2.*/lib-dynload

# Need to build python with the freeze -E option (new with python 2.2.3).
# This option forces all modules to be frozen.  This forces frozen python
# executables to keep from looking for external *.py and *.pyc files.
# However, there are always exceptions.  Those are the -x options.  Those
# modules listed as -x are to be excluded from the frozen binary (though if
# they do exist they will be included).  These mostly have to do with non-UNIX
# OSes that won't have modules be there when running on UNIX.
FREEZE_SKIP_OS=-x ce -x dos -x mac -x nt -x os2 -x riscos -x riscosenviron -x riscospath -x MacOS -x _winreg -x msvcrt -x MACFS -x macfs -x SOCKS -x Carbon.File -x Carbon.Folder -x Carbon.Folders
FREEZE_SKIP_HTML=-x GimpGradientFile -x GimpPaletteFile -x Image -x ImageTk -x PaletteFile -x _imaging -x jarray
FREEZE_SKIP_MISC=-x sitecustomize -x ic -x rourl2path -x EasyDialogs -x org.python.core -x org -x xreadlines -x os.path -x _emx_link -x _dummy_threading -x _subprocess -x email.Generator -x email.Iterators -x usercustomize -x _md5 -x _sha -x _sha256 -x _sha512 -x _xmlplus -x _xmlrpclib -x dummy.Process -x sgmlop -x pg -x whrandom -x _scproxy -x _warnings -x init_warnings -x DBUtils -x psycopg2 -x psycopg2.extras -x DBUtils.PooledDB

FREEZE_SKIP_POSTGRES=`if [ "_Linux" != _$(MACH_OS) ]; then echo "-x pg"; fi`

FREEZE_ENCP_OPTIONS=-E $(FREEZE_SKIP_OS) $(FREEZE_SKIP_MISC)
FREEZE_ENSTORE_OPTIONS=-E $(FREEZE_SKIP_OS) $(FREEZE_SKIP_MISC) $(FREEZE_SKIP_HTML) $(FREEZE_SKIP_POSTGRES)

# No static version for SunOS for now
# No static version for any one any more ... since v3_0
# LDFLAGS2=`if [ "_SunOS" = _$(MACH_OS) ]; then echo "-dy"; else echo "-static"; fi`
LDFLAGS2=

ENCP_TOP_DIR=$$ENSTORE_DIR/src/ENCPBIN
ENCP_BIN_DIR=$(ENCP_TOP_DIR)/opt/encp
ENSTORE_TOP_DIR=$$ENSTORE_DIR/src/ENSTORE_BIN
ENSTORE_BIN_DIR=$(ENSTORE_TOP_DIR)/opt/enstore

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
	(cd $(ENSTORE_DIR)/volume_import; make all)
	- rm -rf ENCPBIN_TEMP
	mkdir ENCPBIN_TEMP
	- rm -rf ENCPBIN
	mkdir -p $(ENCP_BIN_DIR)
	- rm -rf ENSTORE_USER_BIN_TEMP
	mkdir ENSTORE_USER_BIN_TEMP

encp_cmd:

	python $(FREEZE) $(FREEZE_ENCP_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o ENCPBIN_TEMP encp_user.py
	(cd ENCPBIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	$(CP) ENCPBIN_TEMP/encp_user	$(ENCP_BIN_DIR)/encp

encp_cmd_for_dcache:
	python $(FREEZE) $(FREEZE_ENCP_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o ENCPBIN_TEMP encp_user2.py
	(cd ENCPBIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	$(CP) ENCPBIN_TEMP/encp_user2	$(ENCP_BIN_DIR)/encp

post_encp_cmd:
	$(RM) ENCPBIN_TEMP
	$(RM) ENSTORE_USER_BIN_TEMP

	$(CP) $(ENSTORE_DIR)/bin/en_check		$(ENCP_BIN_DIR)/
	$(CP) $(ENSTORE_DIR)/modules/ddencp		$(ENCP_BIN_DIR)/
	$(CP) $(ENSTORE_DIR)/modules/ecrc		$(ENCP_BIN_DIR)/
	$(CP) $(ENSTORE_DIR)/modules/enroute2		$(ENCP_BIN_DIR)/
# So far, make setuid installation manual
#	chmod u+s $INSTALLDIR/enroute2

	mkdir -p -m 0775 $(ENCP_BIN_DIR)/volume_import/
	$(CP) $(ENSTORE_DIR)/volume_import/enstore_tape  $(ENCP_BIN_DIR)/volume_import/

	$(CP) $(ENSTORE_DIR)/sbin/EPS                    $(ENCP_BIN_DIR)/EPS
	$(CP) $(ENSTORE_DIR)/ups/encp.table              $(ENCP_BIN_DIR)/encp.table
	$(CP) $(ENSTORE_DIR)/ups/encp_mni_install.sh     $(ENCP_BIN_DIR)/encp_mni_install.sh
	$(CP) $(ENSTORE_DIR)/ups/chooseConfig            $(ENCP_BIN_DIR)/chooseConfig
	$(CP) $(ENSTORE_DIR)/src/e_errors.py             $(ENCP_BIN_DIR)/e_errors.py
#Make all files and directories user and group readable and writable.
	chmod -R ug+rw $(ENCP_BIN_DIR)/

enstore_cmd:
	python $(FREEZE) $(FREEZE_ENSTORE_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o ENSTORE_USER_BIN_TEMP enstore_user.py
	(cd ENSTORE_USER_BIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp ENSTORE_USER_BIN_TEMP/enstore_user      $(ENCP_BIN_DIR)/enstore

enstore_cmd_for_dcache:
	python $(FREEZE) $(FREEZE_ENSTORE_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o ENSTORE_USER_BIN_TEMP enstore_user2.py
	(cd ENSTORE_USER_BIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp ENSTORE_USER_BIN_TEMP/enstore_user2      $(ENCP_BIN_DIR)/enstore

ensync_cmd:
	- rm -rf ENSYNC_BIN_TEMP
	mkdir ENSYNC_BIN_TEMP
	python $(FREEZE) $(FREEZE_ENCP_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o ENSYNC_BIN_TEMP ensync_user.py
	(cd ENSYNC_BIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp ENSYNC_BIN_TEMP/ensync_user      $(ENCP_BIN_DIR)/ensync
	rm -rf ENSYNC_BIN_TEMP

enmv_cmd:
	- rm -rf ENMV_BIN_TEMP
	mkdir ENMV_BIN_TEMP
	python $(FREEZE)  $(FREEZE_ENCP_OPTIONS)  -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o ENMV_BIN_TEMP enmv_user.py
	(cd ENMV_BIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp ENMV_BIN_TEMP/enmv_user      $(ENCP_BIN_DIR)/enmv
	rm -rf ENMV_BIN_TEMP

encp: pre_encp_cmd encp_cmd enstore_cmd ensync_cmd enmv_cmd post_encp_cmd

encp_for_dcache: pre_encp_cmd encp_cmd_for_dcache enstore_cmd_for_dcache ensync_cmd enmv_cmd post_encp_cmd

### The following groupings for for building the server side programs
### intended for use by the Enstore administrators.
###
### The command of interest is "make enstore".

pre_enstore:
	 #if [ "$$FTT_DIR"x = x ]; then echo FTT_DIR must be set to build enstore.; exit 1; fi
	 #if [ "$$ACI_DIR"x = x ]; then mv aml2.py aml2.py.sav; cp aml2_dummy.py aml2.py; fi
	(cd $(ENSTORE_DIR)/modules; make all)
	- $(RM) ENSTORE_BIN
	mkdir -p $(ENSTORE_BIN_DIR)
	mkdir $(ENSTORE_BIN_DIR)/bin
	mkdir $(ENSTORE_BIN_DIR)/sbin
	mkdir $(ENSTORE_BIN_DIR)/etc
	mkdir $(ENSTORE_BIN_DIR)/www
	mkdir $(ENSTORE_BIN_DIR)/crontabs

post_enstore: install_enstore
	$(MV) $(ENSTORE_BIN_DIR)/bin/encp_admin   $(ENSTORE_BIN_DIR)/bin/encp
	$(MV) $(ENSTORE_BIN_DIR)/bin/enstore_admin $(ENSTORE_BIN_DIR)/bin/enstore
	$(MV) $(ENSTORE_BIN_DIR)/bin/ensync_admin $(ENSTORE_BIN_DIR)/bin/ensync
	$(MV) $(ENSTORE_BIN_DIR)/bin/enmv_admin   $(ENSTORE_BIN_DIR)/bin/enmv

	$(CP) $(ENSTORE_DIR)/modules/enroute2		$(ENSTORE_BIN_DIR)/bin/
	$(CP) $(ENSTORE_DIR)/modules/ddencp		$(ENSTORE_BIN_DIR)/bin/
	$(CP) $(ENSTORE_DIR)/modules/ecrc		$(ENSTORE_BIN_DIR)/bin/

install_enstore:
	$(CP) $(ENSTORE_DIR)/bin/en_check		$(ENSTORE_BIN_DIR)/bin/

	$(CP) $(ENSTORE_DIR)/etc/*.html 		$(ENSTORE_BIN_DIR)/www/
	$(CP) $(ENSTORE_DIR)/etc/*.gif 			$(ENSTORE_BIN_DIR)/www/
	$(CP) $(ENSTORE_DIR)/etc/*.jpg			$(ENSTORE_BIN_DIR)/www/
	$(CP) $(ENSTORE_DIR)/etc/*.kil			$(ENSTORE_BIN_DIR)/www/
	$(CP) $(ENSTORE_DIR)/etc/*.fig			$(ENSTORE_BIN_DIR)/www/
	$(CP) $(ENSTORE_DIR)/etc/*.xpm			$(ENSTORE_BIN_DIR)/www/
	$(CP) $(ENSTORE_DIR)/etc/*.xcf			$(ENSTORE_BIN_DIR)/www/
	$(CP) $(ENSTORE_DIR)/www/*.html 		$(ENSTORE_BIN_DIR)/www/
	$(CP) $(ENSTORE_DIR)/www/*.gif 			$(ENSTORE_BIN_DIR)/www/
	$(CP) $(ENSTORE_DIR)/www/*.jpg			$(ENSTORE_BIN_DIR)/www/
	$(CP) $(ENSTORE_DIR)/www/*.kil			$(ENSTORE_BIN_DIR)/www/
	$(CP) $(ENSTORE_DIR)/www/*.fig			$(ENSTORE_BIN_DIR)/www/
	$(CP) $(ENSTORE_DIR)/www/*.xpm			$(ENSTORE_BIN_DIR)/www/
	$(CP) $(ENSTORE_DIR)/www/*.xcf			$(ENSTORE_BIN_DIR)/www/
	$(CP) $(ENSTORE_DIR)/etc/sample.conf		$(ENSTORE_BIN_DIR)/etc/
	$(CP) `ls -1dp $(ENSTORE_DIR)/crontabs/* | egrep -v "CVS"` 	$(ENSTORE_BIN_DIR)/crontabs/

	$(CP) $(ENSTORE_DIR)/sbin/Vols    		$(ENSTORE_BIN_DIR)/sbin/
	$(CP) $(ENSTORE_DIR)/sbin/noaccess-tapes	$(ENSTORE_BIN_DIR)/sbin/
	$(CP) $(ENSTORE_DIR)/sbin/cleaning_report	$(ENSTORE_BIN_DIR)/sbin/
	$(CP) $(ENSTORE_DIR)/sbin/quota_alert		$(ENSTORE_BIN_DIR)/sbin/

	$(CP) $(ENSTORE_DIR)/sbin/log-stash		$(ENSTORE_BIN_DIR)/sbin/
	$(CP) $(ENSTORE_DIR)/sbin/check_for_traceback	$(ENSTORE_BIN_DIR)/sbin/

	$(CP) $(ENSTORE_DIR)/sbin/copy_ran_file		$(ENSTORE_BIN_DIR)/sbin/

#Why are these two in $ENSTORE_DIR/bin?
	$(CP) $(ENSTORE_DIR)/bin/enstore-boot		$(ENSTORE_BIN_DIR)/bin/
	$(CP) $(ENSTORE_DIR)/bin/monitor_server-boot	$(ENSTORE_BIN_DIR)/bin/

        #if [ "$$ACI_DIR"x = x -a -f aml2.py.sav ]; then mv aml2.py.sav aml2.py; fi

#These are the list breakdowns.
SERVER_LIST=configuration_server log_server event_relay library_manager
SERVER_HTMLGEN_LIST=alarm_server inquisitor ratekeeper monitor_server
SERVER_SQL_LIST=file_clerk volume_clerk info_server accounting_server drivestat_server
SERVER_LIB_LIST=
#mover media_changer
NON_SERVER_LIST=plotter_main

#This .SECONDEXPANSION necessary to enable $$@ to work on newer versions of GNU
# Make.  This became necessary with the version of GNU Make that shipped with
# SLF5.  SLF4 and ealier had $$@ enabled by default.
#
# The third edition of "Managing Progjects with GNU Make" does not mention
# $$@ or .SECONDEXPANSION.  The first edition described $$@ as a Sys V
# extension.
.SECONDEXPANSION:

#These are the full lists.
FULL_SERVER_LIST=$(SERVER_LIST) $(SERVER_HTMLGEN_LIST) $(SERVER_SQL_LIST) $(SERVER_LIB_LIST) $(NON_SERVER_LIST)
FULL_CLIENT_LIST=enstore_admin encp_admin ensync_admin enmv_admin

$(FULL_SERVER_LIST): $$@.py
	- rm -rf SERVER_BIN_TEMP
	mkdir SERVER_BIN_TEMP
	python $(FREEZE) $(FREEZE_ENSTORE_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o SERVER_BIN_TEMP $@.py
	(cd SERVER_BIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp SERVER_BIN_TEMP/$@ $(ENSTORE_BIN_DIR)/sbin/$@
	rm -rf SERVER_BIN_TEMP

$(FULL_CLIENT_LIST): $$@.py
	- rm -rf CLIENT_BIN_TEMP
	mkdir CLIENT_BIN_TEMP
	python $(FREEZE) $(FREEZE_ENSTORE_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o CLIENT_BIN_TEMP $@.py
	(cd CLIENT_BIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp CLIENT_BIN_TEMP/$@ $(ENSTORE_BIN_DIR)/bin/$@
	rm -rf CLIENT_BIN_TEMP

make_enstore_system: ../sbin/$$@.py
	- rm -rf SERVER_BIN_TEMP
	mkdir SERVER_BIN_TEMP
	python $(FREEZE) $(FREEZE_ENSTORE_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o SERVER_BIN_TEMP ../sbin/$@.py
	(cd SERVER_BIN_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	cp SERVER_BIN_TEMP/$@ $(ENSTORE_BIN_DIR)/sbin/$@
	rm -rf SERVER_BIN_TEMP

enstore: pre_enstore $(FULL_CLIENT_LIST) $(FULL_SERVER_LIST) post_enstore

meadia_changer:
	pre_enstore
	(cd $(ENSTORE_DIR)/modules; make all)
	- rm -rf GET_TEMP
	mkdir GET_TEMP
	- rm -rf GET_BIN
	mkdir GET_BIN
	python $(FREEZE) $(FREEZE_ENCP_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o GET_TEMP media_changer.py
	(cd GET_TEMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)
	@ if [ "$$ACI_DIR"x = x -a -f aml2.py.sav ]; then mv aml2.py.sav aml2.py; fi

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
	if [ -n "$(ENSTORE_DEVEL_DIR)" -a -f $(ENSTORE_DEVEL_DIR)/tcltk/ ]; then \
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

source: pre_source install_enstore

pre_source:
	(cd $(ENSTORE_DIR)/modules; make clean)
	- $(RM) ENSTORE_BIN
	mkdir -p $(ENSTORE_BIN_DIR)
	mkdir $(ENSTORE_BIN_DIR)/bin
	mkdir $(ENSTORE_BIN_DIR)/sbin
	mkdir $(ENSTORE_BIN_DIR)/etc
	mkdir $(ENSTORE_BIN_DIR)/www
	mkdir $(ENSTORE_BIN_DIR)/crontabs
	mkdir $(ENSTORE_BIN_DIR)/src
	mkdir $(ENSTORE_BIN_DIR)/modules

	$(CP) $(ENSTORE_DIR)/src/*.py 		$(ENSTORE_BIN_DIR)/src
	$(CP) -r $(ENSTORE_DIR)/modules/* 	$(ENSTORE_BIN_DIR)/modules

	ln -s ENSTORE_BIN/opt/enstore/src/mover.py $(ENSTORE_BIN_DIR)/sbin/mover

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

