SHELL = /bin/sh
#
# There are three targets:
#	all:  to make the html files
#	clean: to get rid of extra files (core, *~, and so on)
#	install: copy selected parts to $ENSTORE_DIR
#

.IGNORE:
.SUFFIXES: .ps .html

WWW  = ./www/
WWW_NEW = ./www_new/
BIN  = ../bin/
SRC  = ../src/
SRCWWW = $(WWW)

ENSTORE_HOME = /usr/hppc_home/www/enstore

HTML_FILES = architecture.html assumptions.html db.html detail_req.html \
	error.html implementation.html overview.html prim_req.html \
	scope.html system_architecture.html testing.html

HOME_FILE = index.html

PS_FILES = enstore.ps read.ps test-stand.ps write.ps

WWW_NEW_FILES = $(HTML_FILES) $(PS_FILES) $(HOME_FILE)

#
# TARGETS
#
all: www_new obj.ps other

#
www_new: $(WWW_NEW_FILES)
	@echo Begin www_new
	@- rm -rf $(WWW_NEW)
	mkdir $(WWW_NEW)
	./moveAndAddButton.pl $(WWW) $(WWW_NEW) $(HTML_FILES)
	@cp $(WWW)/$(HOME_FILE) $(WWW_NEW)
	@cp $(WWW)*.ps $(WWW_NEW)
	@cp $(WWW)*.gif $(WWW_NEW)

obj.ps:
	./mk_inherit

other:
	./class4module

#
# Move into the distribution directory.
install :
	@rm -rf           $(ENSTORE_INSTALL_DIR)/doc
	@mkdir		 $(ENSTORE_INSTALL_DIR)/doc
	cp Makefile mk_inherit class4module $(ENSTORE_INSTALL_DIR)/doc
	@mkdir		 $(ENSTORE_INSTALL_DIR)/doc/www
	cp www_new/*.html  $(ENSTORE_INSTALL_DIR)/doc/www
	cp www_new/*.gif   $(ENSTORE_INSTALL_DIR)/doc/www
	cp www_new/*.ps    $(ENSTORE_INSTALL_DIR)/doc/www
	if [ -d "$(ENSTORE_HOME)" ]; then \
		cp www_new/*.html  $(ENSTORE_HOME) \
		cp www_new/*.gif   $(ENSTORE_HOME) \
		cp www_new/*.ps    $(ENSTORE_HOME) \
	fi;

#
#
# Remove likely artifacts of building, but not the desirata 
tidy:
	rm -f *.log *.toc *.aux
	rm -f core *~ *.jou *.bak MAIN.0 MAIN.0.* *.orig *.old .'#'* '#'*'#'

#
# Scrub down to what would have been had from the source repository 
clean :
	@ echo in .
	@ $(ENSTORE_DIR)/bin/enstoreClean
	@rm -f [1-9].out *.dot *.ps classes.known
	@rm -rf www_new

