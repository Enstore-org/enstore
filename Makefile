SHELL=/bin/sh
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

#  $Id$

#------------------------------------------------------------------

ENDBG=

all:
	for d in modules src volume_import doc; do \
		(cd $$d; $(MAKE) $(MFLAGS) all $(ENDBG);) \
	done 

clean:
	for d in modules src volume_import doc; do \
		(cd $$d; $(MAKE) $(MFLAGS) clean $(ENDBG);) \
	done 
