SHELL = /bin/sh
#
# There are three targets:
#	all:  to make the html files
#	clean: to get rid of extra files (core, *~, and so on)
#	install: copy selected parts to $ENSTORE_DIR
#

.IGNORE:
.SUFFIXES: .ps .html

WWW  = ./www
WWW_NEW = ./www_new
BIN  = ../bin/
SRC  = ../src/
SRCWWW = $(WWW)

ENSTORE_HOME = /usr/hppc_home/www/enstore

HTML_FILES = $(WWW)/design.html $(WWW)/assumptions.html \
	$(WWW)/detail_req.html $(WWW)/implementation.html \
	$(WWW)/overview.html $(WWW)/prim_req.html $(WWW)/scope.html \
	$(WWW)/system_architecture.html $(WWW)/testing.html

HOME_FILE = $(WWW)/index.html

PS_FILES = $(WWW)/enstore.ps $(WWW)/read.ps $(WWW)/test-stand.ps \
	$(WWW)/write.ps

GIF_FILES = $(WWW)/bookshelf.gif $(WWW)/read.gif $(WWW)/write.gif \
	$(WWW)/enstore.gif

LOG_FILES = $(WWW)/redwoodfiber.log $(WWW)/redwoodlocal.log

WWW_NEW_FILES = $(HTML_FILES) $(PS_FILES) $(GIF_FILES) $(LOG_FILES) \
	$(HOME_FILE)

#
# TARGETS
#
all: www_new obj.ps other

#
www_new: $(WWW_NEW_FILES)
	@echo Begin www_new
	@- rm -rf $(WWW_NEW)
	mkdir $(WWW_NEW)
	./moveAndAddButton.pl $(WWW_NEW) $(HTML_FILES)
	@cp $(HOME_FILE) $(WWW_NEW)
	@cp $(PS_FILES) $(WWW_NEW)
	@cp $(GIF_FILES) $(WWW_NEW)
	@cp $(LOG_FILES) $(WWW_NEW)

obj.ps:
	./mk_inherit

other:
	./class4module

#
# Move into the distribution directory.
install : install_home
	@rm -rf           $(ENSTORE_INSTALL_DIR)/doc
	@mkdir		 $(ENSTORE_INSTALL_DIR)/doc
	cp Makefile mk_inherit class4module $(ENSTORE_INSTALL_DIR)/doc
	@mkdir		 $(ENSTORE_INSTALL_DIR)/doc/www
	cp $(WWW_NEW)/*.*   $(ENSTORE_INSTALL_DIR)/doc/www

install_home :
	@-if [ -d "$(ENSTORE_HOME)" -a -w "$(ENSTORE_HOME)" ]; then \
		cp $(WWW_NEW)/*.*  $(ENSTORE_HOME); \
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

