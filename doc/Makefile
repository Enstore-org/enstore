# doc/Makefile $Revision$
SHELL = /bin/sh
#
# There are three targets:
#	all:  to make the html files
#	clean: to get rid of extra files (core, *~, and so on)
#	install: copy selected parts to $ENSTORE_DIR
#
#
# TARGETS
#

all: 
	@ (cd other ; $(MAKE) $(MFLAGS) all $(ENDBG) )
	@ (cd WWW   ; $(MAKE) $(MFLAGS) all $(ENDBG) )

#
# Move into the distribution directory.
install :
	@ rm -rf           $(ENSTORE_INSTALL_DIR)/doc
	@ mkdir		  $(ENSTORE_INSTALL_DIR)/doc
	@ cp `justfiles`    $(ENSTORE_INSTALL_DIR)/doc
	@ (cd other ; $(MAKE) $(MFLAGS) install $(ENDBG) )
	@ (cd WWW   ; $(MAKE) $(MFLAGS) install $(ENDBG) )

#
# Scrub down to what would have been had from the source repository 
clean :
	@ $(ENSTORE_DIR)/bin/enstoreClean
	@ rm -f *.ps *.gif
	@ (cd other ; $(MAKE) $(MFLAGS) clean $(ENDBG) )
	@ (cd WWW   ; $(MAKE) $(MFLAGS) clean $(ENDBG) )


