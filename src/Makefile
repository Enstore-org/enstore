SHELL=/bin/sh

# need a robust way of finding freeze, especially for non-kits version of python
#FREEZE=`find $PYTHON_DIR -name freeze.py -print` 
FREEZE=`ls $(PYTHON_DIR)/Python-*/Tools/freeze/freeze.py`

all:
	@echo nothing to build

clean:
	@ $(ENSTORE_DIR)/bin/enstoreClean
	rm -f *.pyc encp
	- rm -rf ENCPBIN_TEMP
	- rm -rf ENCPBIN
	- rm -rf ENSTORE_USER_BIN_TEMP
	- rm -rf ENSTORE_USER_BIN

encp:
	(cd $(ENSTORE_DIR)/modules; make all)
	- rm -rf ENCPBIN_TEMP
	mkdir ENCPBIN_TEMP
	- rm -rf ENCPBIN
	mkdir ENCPBIN
	python $(FREEZE) -e $(ENSTORE_DIR)/modules -o ENCPBIN_TEMP encp.py
	(cd ENCPBIN_TEMP; LDFLAGS=-static; export LDFLAGS; make -e;)
	cp ENCPBIN_TEMP/encp      ENCPBIN
	cp ../bin/pcmd            ENCPBIN
	cp ../ups/encp.table      ENCPBIN
	cp ../ups/pcfarm.table    ENCPBIN
	cp ../ups/rip.table       ENCPBIN
	cp ../ups/sam.table       ENCPBIN
	rm -rf ENCPBIN_TEMP


install: encp enstore_user
	cp encp $ENSTORE_DIR/bin
	cp enstore_user $ENSTORE_DIR/bin

enstore_user:
	(cd $(ENSTORE_DIR)/modules; make all)
	- rm -rf ENSTORE_USER_BIN_TEMP
	mkdir ENSTORE_USER_BIN_TEMP
	- rm -rf ENSTORE_USER_BIN
	mkdir ENSTORE_USER_BIN
	python $(FREEZE) -e $(ENSTORE_DIR)/modules -o ENSTORE_USER_BIN_TEMP enstore_user.py
	(cd ENSTORE_USER_BIN_TEMP; LDFLAGS=-static; export LDFLAGS; make -e;)
	cp ENSTORE_USER_BIN_TEMP/enstore_user      ENSTORE_USER_BIN
	cp ../bin/pcmd            ENSTORE_USER_BIN
	cp ../ups/encp.table      ENSTORE_USER_BIN
	cp ../ups/pcfarm.table    ENSTORE_USER_BIN
	cp ../ups/rip.table       ENSTORE_USER_BIN
	cp ../ups/sam.table       ENSTORE_USER_BIN
	rm -rf ENSTORE_USER_BIN_TEMP
