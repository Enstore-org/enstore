#############################################################
#
#  $Id$
#
CC=`grep '^CC=' $(PYTHONLIB)/config/Makefile | sed -e 's/CC=[       ]*//'`
all:	 Trace adler32.a Makefile.modules Modules tape_copy  

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
	cp $(PYTHONLIB)/config/Makefile.pre.in . ; \
	make -f Makefile.pre.in boot                    ; \
        cp Makefile ../Makefile.modules

Modules:
	@echo "MAKING MODULES ================================"
	cd SWIG && make 
	@echo "in $$PWD"
	make -f Makefile.modules

Trace:	
	@echo "MAKING TRACE =================================="
	make -f trace.mk trace.a all 
	@if [ ! -d ../sbin ];then mkdir ../sbin; fi
	@mv traceShow trace_delta ../sbin
	ln -fs traceShow ../sbin/traceInfo
	ln -fs traceShow ../sbin/traceReset
	ln -fs traceShow ../sbin/traceMode
	ln -fs traceShow ../sbin/traceOn
	ln -fs traceShow ../sbin/traceOff

# Normally we don't install .py files:
install: installpyc

# But sometimes we may want to:
installpy: install
	for m in $(PYMODULES) the-end; do \
	  if [ "$$m" != the-end ]; then \
	    cp $$m.py $(installdir)/lib/python$(VERSION)/; \
	  fi; \
	done

adler32.a: adler32.c
	$(CC) -c adler32.c
	ar cr adler32.a adler32.o

tape_copy: tape_copy.c
	$(CC) -I. -I$(FTT_DIR)/ftt_lib -o tape_copy tape_copy.c \
	-L. -L$(FTT_DIR)/lib -L$(FTT_DIR)/ftt_lib -lftt_test -lftt $(LIBDS)
installpyc:
	for m in $(PYMODULES) the-end; do \
	  if [ "$$m" != the-end ]; then \
	    python -c "import $$m"; \
	    cp $$m.pyc $(installdir)/lib/python$(VERSION)/; \
	  fi; \
	done

clean:
	-rm Makefile.modules
	-rm -f *.o *.so *~ *# so_locations sedscript
	-rm -rf mksetup
	-rm -rf tape_copy
	-make -f trace.mk clean
	@if [ ! -d ../sbin ];then mkdir ../sbin; fi
	-rm -f  ../sbin/traceShow ../sbin/trace_delta
	-rm -f  ../sbin/traceInfo
	-rm -f  ../sbin/traceReset
	-rm -f  ../sbin/traceMode
	-rm -f  ../sbin/traceOn
	-rm -f  ../sbin/traceOff
	cd SWIG && make clean
