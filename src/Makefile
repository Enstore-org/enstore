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


install: encp
	cp encp $ENSTORE_DIR/bin

