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
WWW_NEW = ../WWW_new
BIN  = ../../bin
SRC  = ../../src
SRCWWW = $(WWW)

ENSTORE_HOME = /usr/hppc_home/www/enstore

# Define .ps and .gif files that are generated. Not all .ps and 
# .gif files in this directory are generated
PIC_GIF_FILES = \
	./desy.gif \
	./enstore.gif \
	./enstoreSimple.gif \
	./enstore_at_fnal.gif \
	./enstore_at_fnal2.gif \
	./read.gif \
	./test_nodes.gif \
	./test-stand.gif \
	./write.gif \
	./test-configuration.gif \
	./test-network.gif
PIC_PS_FILES = \
	./desy.ps \
	./enstore.ps \
	./enstoreSimple.ps \
	./enstore_at_fnal.ps \
	./enstore_at_fnal2.ps \
	./read.ps \
	./test_nodes.ps \
	./test-stand.ps \
	./write.ps \
	./test-configuration.ps \
	./test-network.ps

#
# TARGETS
#
all: pics www_new

pics: PS_pics GIF_pics

www_new: pics
	@- rm -rf $(WWW_NEW)
	mkdir $(WWW_NEW)
	@- cp ../operations/d0en/consoles.html ./d0en_consoles.html
	@- cp ../operations/d0en/operations.html ./d0en_operations.html
	../moveAndAddButton.pl $(WWW_NEW) `justfiles`

PS_pics: $(PIC_PS_FILES)

gifs: $(PIC_GIF_FILES)

GIF_pics: 
	make gifs

#
# Move into the distribution directory.
#

install : install_home
	@rm -rf           $(ENSTORE_INSTALL_DIR)/doc/www
	@mkdir -p	  $(ENSTORE_INSTALL_DIR)/doc/WWW
	cp $(WWW_NEW)/*.* $(ENSTORE_INSTALL_DIR)/doc/WWW

install_home : www_new
	@-if [ -d "$(ENSTORE_HOME)" -a -w "$(ENSTORE_HOME)" ]; then \
		cp $(WWW_NEW)/*.*  $(ENSTORE_HOME); \
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
	@ rm -rf $(WWW_NEW)




