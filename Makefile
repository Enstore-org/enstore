# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#  CVS/Build standard makefile template
#  $Id$
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

#---------------------------------------------------------------------------
# Things folks may need to change
#---------------------------------------------------------------------------

# Product name for ups
PROD=ftt

# version for ups
VERS=v2_0
VERSIONFILES=Makefile README ups/INSTALL_NOTE ftt_lib/ftt_version.c

# dependency flags for declares
DEPEND=

# Chain for declares/addproduct
CHAIN=development

# Extended Flavor of product
FLAVOR=$(OS)$(CUST)$(QUALS)

# OS for declare/addproduct
OS=`funame -s`

# Qualifiers for declares, etc. (e.g. +debug+mips3)
QUALS=

# Customization for addproduct (os release major)
CUST=+`funame -r | sed -e 's|\..*||'`

# addproduct host
ADDPRODUCT_HOST=dcdsv0

# Directories to add whole hog, files to add by find rule
# empty directories to include
ADDDIRS =.
ADDFILES=-name '*.xxx' ! -name '*.yyy'
ADDEMPTY=

# destination for html install targets
DOCROOT=/afs/fnal/files/docs/products/$(PROD)

# destination for local installs
LOCAL=/usr/products/$(OS)$(CUST)/$(VERS)$(QUALS)

# --prefix=$(PREFIX) for gnu configure tools
PREFIX=/usr/local/products/ftt/$(VERS)




SUBDIRS=ftt_lib ftt_test
all: proddir
	for d in $(SUBDIRS); do (cd $$d; $(MAKE) install); done

clean:
	for d in $(SUBDIRS); do (cd $$d; $(MAKE) $@); done


#
# - - - - - - - - - - - - - cut here - - - - - - - - - - - - -
#
#---------------------------------------------------------------------------
# Standard Product Distribution/Declaration Targets
#---------------------------------------------------------------------------
#

# distribution -- currently makes tarfiles
# make an empty file .header
# make an initial tarfile by creating containing .header
# then go through $(LISTALL) output and add files to the tarfile a few dozen
# 	at a time with "xargs tar..."
# Finally echo the filename and do a table of contents
#
distribution: clean ups/declare.dat .manifest
	@: > .header
	@tar cf $(DISTRIBUTIONFILE) .header
	@$(LISTALL) | xargs tar uf $(DISTRIBUTIONFILE)
	@echo $(DISTRIBUTIONFILE):
	@tar tvf $(DISTRIBUTIONFILE)

kits: addproduct

unkits: delproduct

addproduct: distribution dproducts
	$(ADDPRODUCT)
	rm $(DISTRIBUTIONFILE)

# local --  Make a local copy of the product directly
# we do this by running $(LISTALL) and having cpio make a direct copy
# then we cd over there and do a check_manifest to make sure the copy 
#      worked okay.
#
local: clean $(UPS_SUBDIR)/declare.dat .manifest
	$(LISTALL) | cpio -dumpv $(LOCAL)
	cd $(LOCAL); make check_manifest

install: local

undeclare: dproducts $(UPS_SUBDIR)/Version 
	$(UPS_UNDECLARE)

# declare -- declares or redeclares the product; first we check
#        if its already declared and if so remove the existing declaration
#	Finally we declare it, and do a ups list so you can see the
#	declaration.
#
declare: dproducts $(UPS_SUBDIR)/Version
	@($(UPS_EXIST) && $(UPS_UNDECLARE)) || true
	@$(UPS_DECLARE)
	@$(UPS_LIST)

test: clean
	sh test/TestScript

delproduct:
	$(DELPRODUCT)

build_n_test:
	set +e					;\
	. /usr/local/etc/setups.sh		;\
	PRODUCTS="$(DPRODUCTS) $$PRODUCTS" 	;\
	export PRODUCTS				;\
	make FLAVOR=$(FLAVOR) declare		;\
	setup -b -f $(FLAVOR) $(PROD) $(VERS)||true	;\
	make all test

#
#---------------------------------------------------------------------------
# utility targets; check for variables, test file list generation
#---------------------------------------------------------------------------
dproducts:
	@if test "x$(DPRODUCTS)" != "x" ; then true; \
	 else echo "DPRODUCTS must be set for this target."; false; fi

proddir:
	@if test "x$$$(PRODUCT_DIR)" != "x"; then true; \
	 else echo "$(PRODUCT_DIR) must be set for this target.";false;fi

listall:
	$(LISTALL)

#---------------------------------------------------------------------------
# Standard ups files...
#---------------------------------------------------------------------------
#
$(UPS_SUBDIR)/declare.dat: FORCE
	$(UPS_LIST) > $@

$(UPS_SUBDIR)/Version:
	echo $(VERS) > ups/Version

$(UPS_SUBDIR)/upd_files.dat:
	$(LISTALL) > $@

#---------------------------------------------------------------------------
# .manifest file support
#---------------------------------------------------------------------------
#
MANIFEST = $(LISTALL) | 				\
		grep -v .manifest |			\
		xargs sum -r | 				\
		sed -e 's/[ 	].*[ 	]/	/' | 	\
		sort +1

.manifest: FORCE
	$(MANIFEST) > $@

check_manifest:
	$(MANIFEST) > /tmp/check$$$$ 	;\
	diff /tmp/check$$$$ .manifest	;\
	rm /tmp/check$$$$

#---------------------------------------------------------------------------
# Version change support
#---------------------------------------------------------------------------
setversion:
	@echo "New version? \c"; read newvers; set -x;			\
	perl -pi.bak -e "s/$(VERS)/$$newvers/go;" $(VERSIONFILES) ;	\
	cvs commit -m "marked as $$newvers";				\
	cvs tag -F $$newvers  .

#
#---------------------------------------------------------------------------
# Standard Documentation Targets
#---------------------------------------------------------------------------
#
# you probably don't need this for local products, but third party
# software tends to stuff unformatted man pages in $PREFIX/man...
#
$(UPS_SUBDIR)/toman:
	mkdir $(UPS_SUBDIR)/toman
	mkdir $(UPS_SUBDIR)/toman/man
	mkdir $(UPS_SUBDIR)/toman/catman
	. /usr/local/etc/setups.sh                                      ;\
	setup groff                                                     ;\
	cd man                                                          ;\
	for d in man?                                                   ;\
	do                                                               \
		(cd $$d                                                 ;\
		for f in *                                              ;\
		do                                                       \
			echo $$d/$$f                                    ;\
			cp $$f ../../$(UPS_SUBDIR)/toman/man                      ;\
			nroff -man $$f > ../../$(UPS_SUBDIR)/toman/catman/$$f     ;\
		done)                                                   ;\
	done


#
# targets to install html manpages,etc.  in afs docs/products area
#
html: html-man html-texi html-html

html-man: $(UPS_SUBDIR)/toman
	. /usr/local/etc/setups.sh					;\
	setup conv2html							;\
	if [ -d $(UPS_SUBDIR)/toman/catman ]; then			;\
	    src=$(UPS_SUBDIR)/toman/catman				;\
	else								;\
	    src=$(UPS_SUBDIR)/toman					;\
	fi								;\
	dest=$(DOCROOT)/man						;\
	mkdir -p $$dest	|| true						;\
	(cd $$src; find . -print) |				 	 \
	    while read f; do 						 \
		if [ -d $$src/$$f ]					;\
		then							;\
		    mkdir -p $$dest/$$f || true				;\
		else							;\
		    man2html < $$src/$$f > $$dest/$$f			;\
		fi							;\
	    done

html-texi:
	. /usr/local/etc/setups.sh					;\
	setup conv2html							;\
	dest=$(DOCROOT)/texi						;\
	mkdir -p $$dest	|| true						;\
	cd $$dest							;\
	(cd $$src; find . -name *.texi -print) |		 	 \
	    while read f; do						 \
		texi2html -split_chapter $$src/$$f 			;\
	    done

html-html:
	dest=$(DOCROOT)/html						;\
	find . -name '*.html' -print |					 \
	    cpio -dumpv $$dest
#
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#  Gory Details section
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

#---------------------------------------------------------------------------
# List all files in to be included in distributions
# this is tricky 'cause if you list a directory tar does it *and* its
# contents...
#---------------------------------------------------------------------------
#
PRUNECVS =  '(' -name CVS -prune ')' -o ! -name .manifest ! -name .header
LISTALL =  ( \
    test -z "$(ADDDIRS)" || find $(ADDDIRS) $(PRUNECVS) ! -type d -print; \
    test -z "$(ADDFILES)" || find . $(PRUNECVS) $(ADDFILES) ! -type d -print; \
    test -z "$(ADDCMD)" || $(ADDCMD); \
    for d in $(ADDEMPTY) .manifest; do echo $$d; done )

#---------------------------------------------------------------------------
# Ugly Definitions for ups
#---------------------------------------------------------------------------
#

UPS_EXIST= \
	PRODUCTS=$$DPRODUCTS \
	$(UPS_DIR)/bin/ups_exist \
		-f $(FLAVOR) \
		$(PROD) $(VERS)
UPS_UNDECLARE= \
	PRODUCTS=$$DPRODUCTS \
	$(UPS_DIR)/bin/ups_undeclare \
		-f $(FLAVOR) \
		$(PROD) $(VERS)

UPS_DECLARE= \
	PRODUCTS=$$DPRODUCTS \
	$(UPS_DIR)/bin/ups_declare \
		$(DEPEND) \
		-U $(UPS_SUBDIR) \
		-r $(DIR) \
		-f $(FLAVOR) \
		$(PROD) $(VERS)

UPS_LIST= \
	PRODUCTS=$$DPRODUCTS \
	$(UPS_DIR)/bin/ups_list \
		-la \
		-f $(FLAVOR) \
		$(PROD) $(VERS)

ADDPRODUCT = \
    rsh $(ADDPRODUCT_HOST) /bin/sh -c "'\
	. /usr/local/etc/setpath.sh ; \
	. /usr/local/etc/setups.sh ; \
	cmd addproduct \
		-t $(DISTRIBUTIONFILE) \
		-o $(OS) \
		-c $(CUST)$(QUALS) \
		-m $(CUST)$(QUALS) \
		-p $(PROD)  \
		-C $(CHAIN) \
		-v $(VERS) \
		-y '"

DELPRODUCT = \
    rsh $(ADDPRODUCT_HOST) /bin/sh -c "'\
	. /usr/local/etc/setpath.sh ; \
	. /usr/local/etc/setups.sh ; \
	cmd delproduct \
		-o $(OS) \
		-c $(CUST)$(QUALS) \
		-m $(CUST)$(QUALS) \
		-p $(PROD)  \
		-v $(VERS) \
		-y '"

FORCE:
