
all:	 Trace Makefile.modules Modules 

# The target Makefile.modules generates a file Makefile.modules
# which builds dynamic load modules (.so files) for each of the .c files
# listed in the Setup file.   The file Makefile.pre.in knows how to make
# dll files on the platform and "make -f Makefile.pre.in boot" generates
# an appropriate Makefile (in a subdirectory) .   We copy it to
# ../Makefile.modules for the Modules: target

Makefile.modules: Setup 
	@echo "MAKING MODULE Makefile  ================================"
	if [ ! -d mksetup ] ; then  mkdir mksetup; fi
	cd mksetup/   ; \
	cp ../Setup . ; \
	cp $(PYTHON_DIR)/Python*/Misc/Makefile.pre.in . ; \
	make -f Makefile.pre.in boot ; \
        cp Makefile ../Makefile.modules

Modules:
	@echo "MAKING MODULES ================================"
	make -f Makefile.modules

Trace:	
	@echo "MAKING TRACE =================================="
	make -f trace.mk trace.a all 
	@if [ ! -d ../sbin ];then mkdir ../sbin; fi
	@mv traceShow trace_delta ../sbin
	@cd ../sbin;\
	ln -fs traceShow traceInfo;\
	ln -fs traceShow traceReset;\
	ln -fs traceShow traceMode;\
	ln -fs traceShow traceOn;\
	ln -fs traceShow traceOff;

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
