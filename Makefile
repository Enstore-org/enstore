SHELL=/bin/sh
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

#  $Id$

#------------------------------------------------------------------

ENDBG=

all:
	# for BerkeleyDB
	if [ -d BerkeleyDB ] ; then \
		(cd BerkeleyDB; $(MAKE) $(MFLAGS) all $(ENDBG);) \
	fi
	for d in modules src volume_import doc; do \
		(cd $$d; $(MAKE) $(MFLAGS) all $(ENDBG);) \
	done

clean:
	# for BerkeleyDB
	if [ -d BerkeleyDB ] ; then \
		(cd BerkeleyDB; $(MAKE) $(MFLAGS) clean $(ENDBG);) \
	fi
	for d in modules src volume_import doc; do \
		(cd $$d; $(MAKE) $(MFLAGS) clean $(ENDBG);) \
	done 
