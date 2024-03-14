
NEWDIRS=lib include bin
SUBDIRS=ftt_lib ftt_test $(NEWDIRS)

all: 
	for d in $(SUBDIRS); do (cd $$d; $(MAKE) install); done

memdebug: 
	for d in $(SUBDIRS); do (cd $$d; $(MAKE) clean install WARN=-DDEBUGMALLOC EXTRAOBJ=dmalloc/malloc.o ); done


clean:
	for d in $(SUBDIRS); do (cd $$d; $(MAKE) $@); done


