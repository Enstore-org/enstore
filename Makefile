# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#  CVS/Build standard makefile template
#  $Id$
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# run commands with bourne shell!
SHELL=/bin/sh

#------------------------------------------------------------------
# DIR is a "proper" name for the current directory 
#(e.g. the name visible  all over the cluster)  
DIR=$(DEFAULT_DIR)

#------------------------------------------------------------------
# Variables for ups declaration and setup of product
#
            PROD=ftt
     PRODUCT_DIR=FTT_DIR
            VERS=v2_3
       UPS_STYLE=new
          DEPEND=
  TABLE_FILE_DIR=ups
      TABLE_FILE=$(PROD).table
           CHAIN=test
      UPS_SUBDIR=ups
 ADDPRODUCT_HOST=fnkits.fnal.gov
OLD_ADDPRODUCT_HOST=dcdsv0
DISTRIBUTIONFILE=$(DEFAULT_DISTRIBFILE)

#   QUALS is added qualifiers, like: "QUALS=mips3:debug"
#
# for Flavored products
         FLAVOR=$(DEFAULT_FLAVOR)
 	      OS=$(DEFAULT_OS)
          QUALS=
 	    CUST=$(DEFAULT_CUST)
#------------------------------------------------------------------
# Files to include in Distribution
#
# ADDDIRS is dirs to run "find" in to get a complete list
# ADDFILE is files to pick out by pattern anywhere in the product
# ADDEMPTY is empty directories to include in distribution which would
#	   otherwise be missed
# ADDCMD is a command to run to generate a list of files to include 
#	(one per line) (e.g. 'cd fnal/module; make distriblist')
# LOCAL is destination for local: and install: target
# DOCROOT is destination for html documentation targets
ADDDIRS =.
ADDFILES=
ADDEMPTY=
  ADDCMD=
   LOCAL=/fnal/ups/prd/$(PROD)/$(VERS)/$(OS)$(CUST)$(QUALS)
 DOCROOT=/afs/fnal/files/docs/products/$(PROD)

PREFIX=$(DEFAULT_PREFIX)

VERSIONFILES=Makefile README $(UPS_SUBDIR)/INSTALL_NOTE $(UPS_SUBDIR)/Version

#------------------------------------------------------------------


SUBDIRS=ftt_lib ftt_test
all: proddir_is_set
	for d in $(SUBDIRS); do (cd $$d; $(MAKE) install); done

cvswebtags: FORCE
	find ftt_lib ftt_test  -name '*.[ch]' -exec ctags -txw {} \; | \
		grep -v '^if ' > cvswebtags

clean:
	for d in $(SUBDIRS); do (cd $$d; $(MAKE) $@); done

test:
	sh test/TestScript

spotless: clean
	rm -rf bin lib
	rm -f ftt_lib/local

# we indirect this a level so we can customize it for bundle products

declare: $(UPS_STYLE)_declare
undeclare: $(UPS_STYLE)_undeclare
addproduct: $(UPS_STYLE)_addproduct
delproduct: $(UPS_STYLE)_delproduct
autokits: $(UPS_STYLE)_autokits

#---------------------------------------------------------------------------
# 		*** Do not change anything below this line: ***
#     it will go away if you # update the makefile from the templates.
# - - - - - - - - - - - - - - cut here - - - - - - - - - - - - - - - - - -
#
#---------------------------------------------------------------------------
# Standard Product Distribution/Declaration Targets
#    $Id$
#---------------------------------------------------------------------------
#

# distribution -- currently makes tarfiles
#
# The invocation looks weird here; we need to make a tarfile from a (long)
# file list on stdin, but something obvious like:
#     $(LISTALL) | xargs -n25 tar cvf - 
# doesn't work on all the platforms; tar thinks its done when it hits
# the end of the first one, and "tar rvf" doesn't work right on OSF1, so:
# * make an empty file .header
# * make an initial tarfile by adding .header
# * then go through $(LISTALL) output and add files to the tarfile a few dozen
# 	at a time with "xargs tar uf..."
# * Finally echo the filename and do a table of contents to show it worked
#
distribution: clean .manifest.$(PROD)
	@echo "creating $(DISTRIBUTIONFILE)..."
	@: > .header
	@tar cf $(DISTRIBUTIONFILE) .header
	@$(LISTALL) | xargs -n25 tar uf $(DISTRIBUTIONFILE)
	@echo $(DISTRIBUTIONFILE):
	@tar tvf $(DISTRIBUTIONFILE)

kits: addproduct
unkits: delproduct

# local --  Make a local copy of the product directly
# we do this by running $(LISTALL) and having cpio make a direct copy
# then we cd over there and do a check_manifest to make sure the copy 
#      worked okay.
#
local: clean .manifest.$(PROD)
	test -d $(LOCAL) || mkdir -p $(LOCAL)
	$(LISTALL) | cpio -dumpv $(LOCAL)
	cd $(LOCAL); make check_manifest

autolocal: dproducts_is_set distribution
	@/bin/echo "Press enter to update database? \c"
	@read line
	test -d $(LOCAL) || mkdir -p $(LOCAL)
	@$(LISTALL) | cpio -dumpv $(LOCAL)
	cd $(LOCAL); make check_manifest

install: local

#====================== OLD UPS COMMANDS =====================
# addproduct -- add the distribution file to a distribution platform, 
# and clean out the local copy
#
old_addproduct: dproducts_is_set distribution
	@$(OLD_ADDPRODUCT)
	@echo rm $(DISTRIBUTIONFILE);rm $(DISTRIBUTIONFILE)

old_autokits: dproducts_is_set distribution
	@/bin/echo "Press enter to update database? \c"
	@read line
	@$(OLD_ADDPRODUCT)
	@echo rm $(DISTRIBUTIONFILE);rm $(DISTRIBUTIONFILE)

# declare -- declares or redeclares the product; first we check
#        if its already declared and if so remove the existing declaration
#	Finally we declare it, and do a ups list so you can see the
#	declaration.
#
old_declare: dproducts_is_set $(UPS_SUBDIR)/Version
	@($(OLD_UPS_EXIST) && $(OLD_UPS_UNDECLARE)) || true
	@$(OLD_UPS_DECLARE)
	@$(OLD_UPS_LIST)

old_undeclare: dproducts_is_set $(UPS_SUBDIR)/Version 
	@$(OLD_UPS_UNDECLARE)

old_delproduct:
	@$(OLD_DELPRODUCT)

#====================== NEW UPS COMMANDS =====================
# addproduct -- add the distribution file to a distribution platform, 
# and clean out the local copy
#
new_addproduct: dproducts_is_set distribution
	$(NEW_ADDPRODUCT)
	rm $(DISTRIBUTIONFILE)

new_autokits: dproducts_is_set distribution
	@/bin/echo "Press enter to update database? \c"
	@read line
	@$(NEW_ADDPRODUCT)
	@echo rm $(DISTRIBUTIONFILE) ; rm $(DISTRIBUTIONFILE) 

# declare -- declares or redeclares the product; first we check
#        if its already declared and if so remove the existing declaration
#	Finally we declare it, and do a ups list so you can see the
#	declaration.
#  	For new ups, we need to do one declare with build, and
#	one without.  Easiest with recursive calls...
#
new_declare: dproducts_is_set $(UPS_SUBDIR)/Version new_declare_one
	@make UPS_STYLE=$(UPS_STYLE) "QUALS=build:$(QUALS)" new_declare_one

new_declare_one:
	@($(NEW_UPS_EXIST) && $(NEW_UPS_UNDECLARE)) || true
	@$(NEW_UPS_DECLARE) || true
	@$(NEW_UPS_LIST)

new_undeclare: dproducts_is_set $(UPS_SUBDIR)/Version  new_undeclare_one
	@make UPS_STYLE=$(UPS_STYLE) "QUALS=build:$(QUALS)" new_undeclare_one

new_undeclare_one:
	@$(NEW_UPS_UNDECLARE)

new_delproduct:
	@$(NEW_DELPRODUCT)

#====================== TRANSITION COMMANDS =====================

both_addproduct: dproducts_is_set distribution
	@$(NEW_ADDPRODUCT)
	@$(OLD_ADDPRODUCT)
	@echo rm $(DISTRIBUTIONFILE); rm $(DISTRIBUTIONFILE)

both_delproduct:
	@$(NEW_DELPRODUCT)
	@$(OLD_DELPRODUCT)

#=================================================================
# this is the usual target for manually rebuilding the software if it's just
# been checked out of the repository.  We declare it, set it up, and 
# build and regression test it.  Note that the make test will indirectly
# make clean (see above).
# 
build_n_test:
	set +e						;\
	UPS_SHELL=sh; export UPS_SHELL; . `ups setup ups` ;\
	setup -q build? -f $(FLAVOR) $(PROD) 		\
		-r $(DIR) -M $(TABLE_FILE_DIR) -m $(TABLE_FILE)	;\
	make all 					;\
	setup -f $(FLAVOR) $(PROD) 			\
		-r $(DIR) -M $(TABLE_FILE_DIR) -m $(TABLE_FILE)	;\
	make test

#
#---------------------------------------------------------------------------
# utility targets; check for variables, test file list generation
#---------------------------------------------------------------------------
CHECKIT_DEF= checkit() {\
	    test ! -z "$$1"||(echo "$$2 needs to be set";false)\
	}

dproducts_is_set: check_template_vars
	@$(CHECKIT_DEF); checkit "$(DPRODUCTS)" 'DPRODUCTS'

proddir_is_set: check_template_vars
	@$(CHECKIT_DEF); checkit "$$$(PRODUCT_DIR)" '$(PRODUCT_DIR)'

listall:
	$(LISTALL)

# check extra vars when doing new ups...

check_template_vars: check_common_vars check_$(UPS_STYLE)_vars

check_common_vars:
	@$(CHECKIT_DEF) ;\
	checkit "$(UPS_STYLE)" "UPS_STYLE"		;\
	checkit "$(ADDPRODUCT_HOST)" "ADDPRODUCT_HOST"	;\
	checkit "$(CHAIN)" "CHAIN"			;\
	checkit "$(DIR)" "DIR"				;\
	checkit "$(DISTRIBUTIONFILE)" "DISTRIBUTIONFILE";\
	checkit "$(DOCROOT)" "DOCROOT"			;\
	checkit "$(FLAVOR)" "FLAVOR"			;\
	checkit "$(LOCAL)" "LOCAL"			;\
	checkit "$(OS)" "OS"				;\
	checkit "$(PROD)" "PROD"			;\
	checkit "$(PRODUCT_DIR)" "PRODUCT_DIR"		;\
	checkit "$(SHELL)" "SHELL"			;\
	checkit "$(UPS_SUBDIR)" "UPS_SUBDIR"		;\
	checkit "$(VERS)" "VERS"			;\
	checkit "$(VERSIONFILES)" "VERSIONFILES"	

check_old_vars:

check_new_vars:
	@$(CHECKIT_DEF) ;\
	checkit "$(TABLE_FILE)" "TABLE_FILE"		;\
	checkit "$(TABLE_FILE)" "TABLE_FILE_DIR"	;\

#---------------------------------------------------------------------------
#

$(UPS_SUBDIR)/Version:
	echo $(VERS) > $(UPS_SUBDIR)/Version

$(UPS_SUBDIR)/upd_files.dat:
	@echo "creating $@..."
	@ $(LISTALL) > $@

#---------------------------------------------------------------------------
# .manifest file support
#---------------------------------------------------------------------------
#
MANIFEST = $(LISTALL) | 				\
		grep -v .manifest |			\
		xargs -n25 sum -r | 				\
		sed -e 's/[ 	].*[ 	]/	/' | 	\
		sort +1

.manifest.$(PROD): FORCE
	@echo "creating .manifest..."
	@ $(MANIFEST) > $@

check_manifest:
	$(MANIFEST) > /tmp/check$$$$ 	;\
	diff /tmp/check$$$$ .manifest.$(PROD)	;\
	rm /tmp/check$$$$

#---------------------------------------------------------------------------
# Version change support
#---------------------------------------------------------------------------
setversion:
	@/bin/echo "New version? \c"; read newvers; set -x;		\
	perl -pi.bak -e "s/$(VERS)/$$newvers/go;" $(VERSIONFILES) ;	\
	cvs commit -m "marked as $$newvers";				\
	cvs tag -F $$newvers  .

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

# find arguements to prevent CVS directories and manifest file stuff
# from being listed:
PRUNECVS =  '(' -name CVS -prune ')' -o ! -name '.manifest.*' ! -name .header

# make a list of all the files to be included in the product.  There are
# several make variables that could be set, so use all of them that aree
# to make the list.  We do the whole thing in a subshell (e.g. between
# parenthesis) so the whole thing can be piped to other programs, etc.
#
LISTALL =  ( \
    for d in  .manifest.$(PROD) $(ADDEMPTY); do echo $$d; done; \
    test -z "$(ADDDIRS)" || find $(ADDDIRS) $(PRUNECVS) ! -type d -print; \
    test -z "$(ADDFILES)" || find . $(PRUNECVS) $(ADDFILES) ! -type d -print; \
    test -z "$(ADDCMD)" || sh -c "$(ADDCMD)" \
    )

#---------------------------------------------------------------------------
# Ugly Definitions for ups
#---------------------------------------------------------------------------
#

# Default names for things
DEFAULT_DISTRIBFILE="$(DIR)/../$(PROD)$(FLAVOR)$(VERS).tar"
DEFAULT_OS=`uname -s | sed -e 's/IRIX64/IRIX/'`
DEFAULT_FLAVOR=$(OS)$(CUST)$(QUALSEP)$(QUALS)
DEFAULT_NULL_FLAVOR=NULL$(QUALSEP)$(QUALS)

# note the plus sign in DEFAULT_CUST is really part of the string, not
# appending to some other value or anything.
DEFAULT_CUST=+`((uname -s | grep AIX >/dev/null && uname -v)||uname -r) | \
		sed -e 's|\..*||'`

# We try to undo common automount name munging here,
# and correct afs paths to read-only volume paths, etc.   This way we get a
# name we can use in ups declares, and when rsh-ing to another node
# to do "cmd addproduct", etc.
DEFAULT_DIR=`pwd | sed	-e 's|^/tmp_mnt||' \
	   	-e 's|^/export||' \
	   	-e 's|^/afs/\.fnal\.gov|/afs/fnal.gov|' \
		-e 's|^/products|/usr&|'`

# ------ prefix support

DEFAULT_PREFIX=/tmp/build-$(PROD)-$(VERS)

build_prefix: proddir_is_set $(PREFIX)

$(PREFIX):
	ln -s $$$(PRODUCT_DIR) $(PREFIX)


# In old ups, we need "-f IRIX+6+qual1+qual2"
# in new ups, we need "-f IRIX+6 -q qual1+qual2"
# if no qualifiers, we want *nothing*
#
QUALSEP=`case $(UPS_STYLE)$(QUALS) in \
	 new) ;; \
	 old) ;; \
	 new*) echo ' -q ';; \
	 old*) echo '+';; \
	 esac`

# These are all basic ups commands with loads of options.

OLD_UPS_EXIST= \
        PRODUCTS="$(DPRODUCTS)"; export PRODUCTS; \
	echo $(UPS_DIR)/bin/ups_exist \
		-f $(FLAVOR) \
		$(PROD) $(VERS);\
	$(UPS_DIR)/bin/ups_exist \
		-f $(FLAVOR) \
		$(PROD) $(VERS)

NEW_UPS_EXIST=\
	echo $(UPS_DIR)/bin/ups exist \
		-z $(DPRODUCTS) \
		-f $(FLAVOR) \
		$(PROD) $(VERS);\
	$(UPS_DIR)/bin/ups exist \
		-z $(DPRODUCTS) \
		-f $(FLAVOR) \
		$(PROD) $(VERS)

OLD_UPS_UNDECLARE= \
        PRODUCTS="$(DPRODUCTS)"; export PRODUCTS; \
	echo $(UPS_DIR)/bin/ups_undeclare \
		-f $(FLAVOR) \
		$(PROD) $(VERS);\
	$(UPS_DIR)/bin/ups_undeclare \
		-f $(FLAVOR) \
		$(PROD) $(VERS)

NEW_UPS_UNDECLARE=\
	echo $(UPS_DIR)/bin/ups undeclare \
		-z $(DPRODUCTS) \
		-f $(FLAVOR) \
		$(PROD) $(VERS);\
	$(UPS_DIR)/bin/ups undeclare \
		-z $(DPRODUCTS) \
		-f $(FLAVOR) \
		$(PROD) $(VERS)

OLD_UPS_DECLARE= \
        PRODUCTS="$(DPRODUCTS)"; export PRODUCTS; \
	echo $(UPS_DIR)/bin/ups_declare \
		$(DEPEND) \
		-U $(DIR)/$(UPS_SUBDIR) \
		-r $(DIR) \
		-f $(FLAVOR) \
		$(PROD) $(VERS); \
	$(UPS_DIR)/bin/ups_declare \
		$(DEPEND) \
		-U $(DIR)/$(UPS_SUBDIR) \
		-r $(DIR) \
		-f $(FLAVOR) \
		$(PROD) $(VERS)

NEW_UPS_DECLARE= \
	echo $(UPS_DIR)/bin/ups declare \
		-M $(TABLE_FILE_DIR) \
		-m $(TABLE_FILE) \
		-z $(DPRODUCTS) \
		-U $(UPS_SUBDIR) \
		-r $(DIR) \
		-f $(FLAVOR) \
		$(PROD) $(VERS);\
	$(UPS_DIR)/bin/ups declare \
		-M $(TABLE_FILE_DIR) \
		-m $(TABLE_FILE) \
		-z $(DPRODUCTS) \
		-U $(UPS_SUBDIR) \
		-r $(DIR) \
		-f $(FLAVOR) \
		$(PROD) $(VERS)

# make the declare.dat here so we don't try to make it otherwise

OLD_UPS_LIST= \
        PRODUCTS="$(DPRODUCTS)"; export PRODUCTS; \
	echo $(UPS_DIR)/bin/ups_list \
		-l \
		-f $(FLAVOR) \
		$(PROD) $(VERS); \
	$(UPS_DIR)/bin/ups_list \
		-l \
		-f $(FLAVOR) \
		$(PROD) $(VERS) | \
			(test  -d $(UPS_SUBDIR) && \
				tee $(UPS_SUBDIR)/declare.dat || cat)

NEW_UPS_LIST = \
	echo $(UPS_DIR)/bin/ups list \
		-z $(DPRODUCTS) \
		-f $(FLAVOR) \
		$(PROD) $(VERS);\
	$(UPS_DIR)/bin/ups list \
		-z $(DPRODUCTS) \
		-f $(FLAVOR) \
		$(PROD) $(VERS) 

OLD_ADDPRODUCT = echo $(OLD_ADDPRODUCT_1); $(OLD_ADDPRODUCT_1)

OLD_ADDPRODUCT_1 = \
    rsh $(OLD_ADDPRODUCT_HOST) /bin/sh -c "'\
	. /usr/local/etc/setpath.sh ; \
	. /usr/local/etc/setupsI.sh ; \
	cmd addproduct \
		-t $(DISTRIBUTIONFILE) \
		-o $(OS) \
		-c $(CUST)$(QUALS) \
		-m $(CUST)$(QUALS) \
		-p $(PROD)  \
		-C $(CHAIN) \
		-v $(VERS) \
		-y '"

NEW_ADDPRODUCT = \
	(test ! -z "$(UPD_DIR)" || (echo upd must be setup!; false ));\
		echo $(NEW_ADDPRODUCT_1); $(NEW_ADDPRODUCT_1)

NEW_ADDPRODUCT_1 = \
	upd addproduct \
	        -h $(ADDPRODUCT_HOST) \
		-T $(DISTRIBUTIONFILE) \
		-M $(TABLE_FILE_DIR) \
		-m $(TABLE_FILE) \
		-U $(UPS_SUBDIR) \
		-f $(FLAVOR) \
		$(PROD) $(VERS)

NEW_DELPRODUCT = \
	(test ! -z "$(UPD_DIR)" || (echo upd must be setup!; false ));\
	     echo $(NEW_DELPRODUCT_1); $(NEW_DELPRODUCT_1)

NEW_DELPRODUCT_1 = \
	upd delproduct \
                -h $(ADDPRODUCT_HOST) \
		-f $(FLAVOR) \
		$(PROD) $(VERS)

OLD_DELPRODUCT = echo $(OLD_DELPRODUCT_1); $(OLD_DELPRODUCT_1)

OLD_DELPRODUCT_1 = \
    rsh $(OLD_ADDPRODUCT_HOST) /bin/sh -c "'\
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
#
#---------------------------------------------------------------------------
# Documentation Targets
#---------------------------------------------------------------------------
#
# you probably don't need this for local products, but third party
# software tends to stuff unformatted man pages in $PREFIX/man...
#
$(UPS_SUBDIR)/toman:
	mkdir $(UPS_SUBDIR)/toman
	mkdir $(UPS_SUBDIR)/toman/man
	mkdir $(UPS_SUBDIR)/toman/catman
	cd man                                                          ;\
	for d in man?                                                   ;\
	do                                                               \
	    (cd $$d                                                 	;\
	    for f in *                                              	;\
	    do                                                       	 \
		echo $$d/$$f                                    	;\
		cp $$f ../../$(UPS_SUBDIR)/toman/man                    ;\
		nroff -man $$f > ../../$(UPS_SUBDIR)/toman/catman/$$f   ;\
	    done)                                                   	;\
	done


#
# targets to install html manpages,etc.  in afs docs/products area
#
html: html-man html-texi html-html

html-man: $(UPS_SUBDIR)/toman
	UPS_SHELL=sh; export UPS_SHELL; . `ups setup ups`               ;\
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
	UPS_SHELL=sh; export UPS_SHELL; . `ups setup ups` ;\
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
