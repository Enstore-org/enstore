SHELL=/bin/sh

CP=cp -p
MV=mv
RM=rm -rf

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
FREEZE_SKIP_MISC=-x MacOS -X sitecustomize -x _winreg -x ic -x rourl2path -x EasyDialogs -x msvcrt -x MACFS -x macfs -x SOCKS -x org.python.core -X org -X xreadlines -x Carbon.File -x Carbon.Folder -x Carbon.Folders -x os.path -x _emx_link -x _dummy_threading
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

make_pnfs_dir:
	- rm -rf TMP
	mkdir TMP
	python $(FREEZE) $(FREEZE_ENSTORE_OPTIONS) -e $(ENSTORE_DIR)/modules -e $(PYMODULES) -o TMP make_pnfs_dir.py
	(cd TMP; LDFLAGS=$(LDFLAGS2); export LDFLAGS; make -e;)

