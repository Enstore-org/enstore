#############################################################
#
#  $Id$
#
all:	 adler32.a Makefile.modules Modules tape_copy enroute2 m2probe

# The target Makefile.modules generates a file Makefile.modules
# which builds dynamic load modules (.so files) for each of the .c files
# listed in the Setup file.   The file Makefile.pre.in knows how to make
# dll files on the platform and "make -f Makefile.pre.in boot" generates
# an appropriate Makefile (in a subdirectory) .   We copy it to
# ../Makefile.modules for the Modules: target
# The STK and ACI/EMASS products are only made if they are setup

Makefile.modules: Setup 
	@echo "MAKING MODULE Makefile  ================================"
	if [ ! -d mksetup ] ; then  mkdir mksetup; fi
	cd mksetup/   ; \
	cp ../Setup . ; \
	if  [ "$(ACI_DIR)"x = x ];  then grep -v ACI Setup > x; mv x Setup; fi ;\
	if  [ "$(STK_DIR)"x = x ];  then grep -v STK Setup > x; mv x Setup; fi ;\
	if  [ "$(DRIVESTAT_CLIENT_DIR)"x = x ];  then grep -v DRIVESTAT_CLIENT Setup > x; mv x Setup; fi ;\
	cp $(PYTHONLIB)/config/Makefile.pre.in . ; \
	make -f Makefile.pre.in boot                    ; \
	cp Makefile ../Makefile.modules

Modules:
	@echo "MAKING MODULES ================================"
	cd SWIG && make 
	@echo "in $$PWD"
	make -f Makefile.modules

adler32.a: adler32.c
	$(CC) -c adler32.c
	ar cr adler32.a adler32.o

tape_copy: tape_copy.c
	$(CC) -I. -I$(FTT_DIR)/ftt_lib -o tape_copy tape_copy.c \
	-L. -L$(FTT_DIR)/lib -L$(FTT_DIR)/ftt_lib -lftt_test -lftt $(LIBDS)
tape_write: tape_write.c
	$(CC) -I. -I$(FTT_DIR)/ftt_lib -o tape_write tape_write.c \
	-L. -L$(FTT_DIR)/lib -L$(FTT_DIR)/ftt_lib -lftt_test -lftt $(LIBDS)

m2probe: m2probe.c
	$(CC) -I. -I$(FTT_DIR)/include -o m2probe m2probe.c -L$(FTT_DIR)/lib -lftt $(LIBDS)
	cp m2probe ../sbin

enroute2: enroute2.c
	$(CC) enroute2.c -o enroute2 $(LIBSOCKETNSL)

installpyc:
	for m in $(PYMODULES) the-end; do \
	  if [ "$$m" != the-end ]; then \
	    python -c "import $$m"; \
	    cp $$m.pyc $(installdir)/lib/python$(VERSION)/; \
	  fi; \
	done

clean:
	-rm Makefile.modules
	-rm -f *.o *.so *~ *# so_locations sedscript enroute2 m2probe
	-rm -rf mksetup
	-rm -rf tape_copy
	@if [ ! -d ../sbin ];then mkdir ../sbin; fi
	cd SWIG && make clean
