#!/usr/bin/make -f

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
VERS=v1_0

# dependency flags for declares
DEPEND=-b "< gcc current"

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
#---------------------------------------------------------------------------
# Standard Product Distribution/Declaration Targets
#---------------------------------------------------------------------------
#

tarfile: clean ups/declare.dat .manifest
	@: > .header
	@tar cf $(TARFILE) .header
	@$(LISTALL) | xargs tar uf $(TARFILE)
	@echo $(TARFILE):
	@tar tvf $(TARFILE)

kits: addproduct

unkits: delproduct

addproduct: tarfile dproducts
	$(ADDPRODUCT)
	rm $(TARFILE)

local: clean ups/declare.dat .manifest
	$(LISTALL) | cpio -dumpv $(LOCAL)
	cd $(LOCAL); make check_manifest declare

undeclare: dproducts ups/Version 
	$(UPS_UNDECLARE)

declare: dproducts ups/Version
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

#---------------------------------------------------------------------------
# utility targets; check for variables, test file list generation
#---------------------------------------------------------------------------
dproducts:
	@if test "x$(DPRODUCTS)" != "x" ; then true; \
	 else echo "DPRODUCTS must be set for this target."; false; fi

proddir:
	@if test "x$(FTT_DIR)" != "x"; then true; \
	 else echo "FTT_DIR must be set for this target.";false;fi

listall:
	$(LISTALL)

#---------------------------------------------------------------------------
# Standard ups files...
#---------------------------------------------------------------------------
#
ups/declare.dat: FORCE
	$(UPS_LIST) > $@

ups/Version:
	echo $(VERS) > ups/Version

ups/upd_files.dat:
	$(LISTALL) > $@
#
#---------------------------------------------------------------------------
# Standard Documentation Targets
#---------------------------------------------------------------------------
#
# you probably don't need this for local products, but third party
# software tends to stuff unformatted man pages in $PREFIX/man...
#
ups/toman:
	mkdir ups/toman
	mkdir ups/toman/man
	mkdir ups/toman/catman
	. /usr/local/etc/setups.sh                                      ;\
	setup groff                                                     ;\
	cd man                                                          ;\
	for d in man?                                                   ;\
	do                                                               \
		(cd $$d                                                 ;\
		for f in *                                              ;\
		do                                                       \
			echo $$d/$$f                                    ;\
			cp $$f ../../ups/toman/man                      ;\
			nroff -man $$f > ../../ups/toman/catman/$$f     ;\
		done)                                                   ;\
	done


#
# targets to install html manpages,etc.  in afs docs/products area
#
html: html-man html-texi html-html

html-man: ups/toman
	. /usr/local/etc/setups.sh					;\
	setup conv2html							;\
	if [ -d ups/toman/catman ]; then				;\
	    src=ups/toman/catman					;\
	else								;\
	    src=ups/toman						;\
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
    for d in $(ADDEMPTY) .manifest; do echo $$d; done )

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
# Ugly Definitions for ups
#---------------------------------------------------------------------------
#
DIR    =`pwd | sed -e  's|/tmp_mnt||'`#	# Declare directory for ups -- here
TARFILE="$(DIR)/../$(FLAVOR).tar"#	# tarfile name to use

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
		-t $(TARFILE) \
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
