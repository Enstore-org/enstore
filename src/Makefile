SHELL=/bin/sh

all:
	@echo nothing to build

clean:
	@ $(ENSTORE_DIR)/bin/enstoreClean
	rm -f *.pyc encp

encp:	encp.py $(ENSTORE_DIR)/modules/Devcodes.o $(ENSTORE_DIR)/modules/ETape.o $(ENSTORE_DIR)/modules/EXfer.o $(ENSTORE_DIR)/modules/Trace.o
	- rm -rf ENCPBIN_TEMP
	mkdir ENCPBIN_TEMP
	- rm -rf ENCPBIN
	mkdir ENCPBIN
	python $(PYTHON_DIR)/Python-*/Tools/freeze/freeze.py -e $(ENSTORE_DIR)/modules -o ENCPBIN_TEMP encp.py
	(cd ENCPBIN_TEMP; make;)
	cp ENCPBIN_TEMP/encp ENCPBIN
	cp ../ups/encp.table ENCPBIN
	rm -rf ENCPBIN_TEMP


install: encp
	cp encp $ENSTORE_DIR/bin

$(ENSTORE_DIR)/modules/Devcodes.o:
	(cd  $(ENSTORE_DIR)/modules; make Devcodes)

$(ENSTORE_DIR)/modules/ETape.o:
	(cd  $(ENSTORE_DIR)/modules; make ETape)

$(ENSTORE_DIR)/modules/EXfer.o:
	(cd  $(ENSTORE_DIR)/modules; make EXfer)

$(ENSTORE_DIR)/modules/Trace.o:
	(cd  $(ENSTORE_DIR)/modules; make Trace)
