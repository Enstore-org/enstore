SHELL=/bin/sh

all:
	for d in bin src test etc doc ups ; do (cd $$d; make all) ; done


clean:
	for d in bin src test etc doc ups ; do (cd $$d; make clean) ; done
	@ $(ENSTORE_DIR)/bin/enstoreClean
