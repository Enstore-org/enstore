# doc/WWW/Makefile $Revision$
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
	$(TGIF) -print -ps -color $*.obj

.ps.gif:
	$(CONVERT) $*.ps $@


# Policy: Make files will succeed even if document procceeors are not
# present on the system.
# Mechanism: The funny macros below which either evaluate to the
# document processor or to /bin/true, which is effectively a no-op.
 
TGIF=`if hash tgif 2>/dev/null ; then echo tgif ; else echo true ; fi`
DOT=`if hash dot 2>/dev/null ; then echo dot ; else echo true ; fi`
CONVERT=`if hash convert 2>/dev/null ; then echo convert ; else echo true ; fi`

WWW  = .
WWW_NEW = ../www_new
BIN  = ../../bin
SRC  = ../../src
SRCWWW = $(WWW)

ENSTORE_HOME = /usr/hppc_home/www/enstore

HTML_FILES = $(WWW)/design.html $(WWW)/assumptions.html \
	$(WWW)/detail_req.html $(WWW)/implementation.html $(WWW)/wbs2.html \
	$(WWW)/overview.html $(WWW)/prim_req.html $(WWW)/scope.html \
	$(WWW)/system_architecture.html $(WWW)/testing.html

HOME_FILE = $(WWW)/index.html

PS_FILES = $(WWW)/enstore.ps $(WWW)/read.ps $(WWW)/test-stand.ps \
	$(WWW)/write.ps $(WWW)/enstoreSimple.ps $(WWW)/vonruden-698-enstore.ps

GIF_FILES = $(WWW)/bookshelf.gif $(WWW)/read.gif $(WWW)/write.gif \
	$(WWW)/enstore.gif $(WWW)/test-stand.gif $(WWW)/enstoreSimple.gif

LOG_FILES = $(WWW)/redwoodfiber.log $(WWW)/redwoodlocal.log

# Define .ps and .gif files that are generated. Not all .ps and 
# .gif files in this directory are generated
PIC_GIF_FILES = \
	./desy.gif \
	./enstore.gif \
	./enstoreRead.gif \
	./enstoreSimple.gif \
	./enstore_at_fnal.gif \
	./enstore_at_fnal2.gif \
	./read.gif \
	./test_nodes.gif \
	./test-stand.gif \
	./write.gif
PIC_PS_FILES = \
	./desy.ps \
	./enstore.ps \
	./enstoreRead.ps \
	./enstoreSimple.ps \
	./enstore_at_fnal.ps \
	./enstore_at_fnal2.ps \
	./read.ps \
	./test_nodes.ps \
	./test-stand.ps \
	./write.ps 

WWW_NEW_FILES = $(HTML_FILES) $(PS_FILES) $(GIF_FILES) $(LOG_FILES) \
	$(HOME_FILE)

#
# TARGETS
#
all: pics warn

pics: PS_pics GIF_pics

PS_pics: $(PIC_PS_FILES)

gifs: $(PIC_GIF_FILES)

GIF_pics: 
	make gifs

#
# The template defines things at the top of HTML files which are
# important, for example, a stanrdar button to go back to the index.
# check all the HTML for the standard style.
#
# The test that is implemented is to look for "bookshelf.gif" at the 
# top of the file.
#

warn:
	for h in *.html ; do if head -30 $$h | fgrep bookshelf.gif > /dev/null ; \
	then true ; else \
	echo warning: $$h does not have a standard header. Get one from \
	the templates directory ; fi ; \
	done 
#
# Move into the distribution directory.
#

install : install_home
	@rm -rf           $(ENSTORE_INSTALL_DIR)/doc
	@mkdir		  $(ENSTORE_INSTALL_DIR)/doc/WWW
	cp `justfiles`    $(ENSTORE_INSTALL_DIR)/doc/WWW

install_home : 
	@-if [ -d "$(ENSTORE_HOME)" -a -w "$(ENSTORE_HOME)" ]; then \
		cp `justfiles`  $(ENSTORE_HOME); \
		chmod g+w $(ENSTORE_HOME)/*; \
	else \
		echo "ERROR: No permissions to write to $(ENSTORE_HOME)"; \
	fi;

#
#
# Remove likely artifacts of building, but not the desirata 
tidy:
	@ $(BIN)/enstoreClean
#
# Scrub down to what would have been had from the source repository 
clean :
	@ $(BIN)/enstoreClean
	@ rm -rf $(PIC_PS_FILES) $(PIC_GIF_FILES)




