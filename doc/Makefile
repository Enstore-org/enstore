SHELL = /bin/sh
#
# There are three targets:
#	all:  to make the html files
#	clean: to get rid of extra files (core, *~, and so on)
#	install: copy selected parts to $ENSTORE_DIR
#

.IGNORE:
.SUFFIXES: .ps .gif .html .obj 

.obj.ps:
	@tgif -print -ps -color $*.obj

.ps.gif:
	@convert $*.ps $@

WWW  = ./www/
WWW_NEW = ./www_new/
BIN  = ../bin/
SRC  = ../src/
SRCWWW = $(WWW)

ENSTORE_HOME = /usr/hppc_home/www/enstore

HTML_FILES = $(WWW)/design.html $(WWW)/assumptions.html \
	$(WWW)/detail_req.html $(WWW)/implementation.html $(WWW)/wbs2.html \
	$(WWW)/overview.html $(WWW)/prim_req.html $(WWW)/scope.html \
	$(WWW)/system_architecture.html $(WWW)/testing.html

HOME_FILE = $(WWW)/index.html

PS_FILES = $(WWW)/enstore.ps $(WWW)/read.ps $(WWW)/test-stand.ps \
	$(WWW)/write.ps $(WWW)enstoreSimple.ps $(WWW)/vonruden-698-enstore.ps

GIF_FILES = $(WWW)/bookshelf.gif $(WWW)/read.gif $(WWW)/write.gif \
	$(WWW)/enstore.gif $(WWW)/test-stand.gif $(WWW)/enstoreSimple.gif

LOG_FILES = $(WWW)/redwoodfiber.log $(WWW)/redwoodlocal.log

PIC_FILES = \
	Server-card.ps Server-card.gif \
	christmas.ps christmas.gif \
	enstore.ps enstore.gif \
	hardware.ps hardware.gif \
	library_manager.ps library_manager.gif \
	mover_write.ps mover_write.gif \
	pnfs.ps pnfs.gif \
	superficial_overviewobj.ps superficial_overviewobj.gif \
	test-stand.ps test-stand.gif \
	$(WWW)/desy.ps $(WWW)/desy.gif \
	$(WWW)/enstore.ps $(WWW)/enstore.gif \
	$(WWW)/enstoreRead.ps $(WWW)/enstoreRead.gif \
	$(WWW)/enstoreSimple.ps $(WWW)/enstoreSimple.gif \
	$(WWW)/enstore_at_fnal.ps $(WWW)/enstore_at_fnal.gif \
	$(WWW)/enstore_at_fnal2.ps $(WWW)/enstore_at_fnal2.gif \
	$(WWW)/read.ps $(WWW)/read.gif \
	$(WWW)/test_nodes.ps $(WWW)/test_nodes.gif \
	$(WWW)/test-stand.ps $(WWW)/test-stand.gif \
	$(WWW)/write.ps $(WWW)/write.gif

WWW_NEW_FILES = $(HTML_FILES) $(PS_FILES) $(GIF_FILES) $(LOG_FILES) \
	$(HOME_FILE)

#
# TARGETS
#
all: www_new obj.ps other pics

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

pics:	$(PIC_FILES)

#
# Move into the distribution directory.
install : install_home
	@rm -rf           $(ENSTORE_INSTALL_DIR)/doc
	@mkdir		 $(ENSTORE_INSTALL_DIR)/doc
	cp Makefile mk_inherit class4module $(ENSTORE_INSTALL_DIR)/doc
	@mkdir		 $(ENSTORE_INSTALL_DIR)/doc/www
	cp $(WWW_NEW)/*.*   $(ENSTORE_INSTALL_DIR)/doc/www

install_home : www_new
	@-if [ -d "$(ENSTORE_HOME)" -a -w "$(ENSTORE_HOME)" ]; then \
		cp $(WWW_NEW)/*.*  $(ENSTORE_HOME); \
	else \
		echo "ERROR: No permissions to write to $(ENSTORE_HOME)"; \
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
	@rm -rf $(PIC_FILES)

