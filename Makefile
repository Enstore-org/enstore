SHELL=/bin/sh
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

#  $Id$

#------------------------------------------------------------------

ENDBG=

docs: 
	cd doc ; $(MAKE) $(MFLAGS) all $(ENDBG)

all:
	for d in modules src volume_import PyGreSQL psycopg2 xml2ddl; do \
		(cd $$d; $(MAKE) $(MFLAGS) all $(ENDBG);) \
	done

clean:
	for d in modules src volume_import PyGreSQL xml2ddl doc; do \
		(cd $$d; $(MAKE) $(MFLAGS) clean $(ENDBG);) \
	done 
