all:	 Trace Makefile.modules Modules 

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
	cp $(PYTHON_DIR)/Python*/Misc/Makefile.pre.in . ; \
	make -f Makefile.pre.in boot                    ; \
        cp Makefile ../Makefile.modules

Modules:
	@echo "MAKING MODULES ================================"
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
	@cd ../modules

# Normally we don't install .py files:
install: installpyc

# But sometimes we may want to:
installpy: install
	for m in $(PYMODULES) the-end; do \
	  if [ "$$m" != the-end ]; then \
	    cp $$m.py $(installdir)/lib/python$(VERSION)/; \
	  fi; \
	done

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
	-make -f trace.mk clean
	@if [ ! -d ../sbin ];then mkdir ../sbin; fi
	-rm -f  ../sbin/traceShow ../sbin/trace_delta
	-rm -f  ../sbin/traceInfo
	-rm -f  ../sbin/traceReset
	-rm -f  ../sbin/traceMode
	-rm -f  ../sbin/traceOn
	-rm -f  ../sbin/traceOff
