SHELL=/bin/sh
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

#  $Id$

#------------------------------------------------------------------

ENDBG=

docs: 
	cd doc ; $(MAKE) $(MFLAGS) all $(ENDBG)

all:    FTT
	for d in ftt modules src volume_import PyGreSQL psycopg2 xml2ddl; do \
		(cd $$d; $(MAKE) $(MFLAGS) all $(ENDBG);) \
	done
FTT:
	ln -s ftt FTT

clean:
	for d in ftt modules src volume_import PyGreSQL psycopg2 xml2ddl doc; do \
		(cd $$d; $(MAKE) $(MFLAGS) clean $(ENDBG);) \
	done 
