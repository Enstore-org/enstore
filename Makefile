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
# product name, PRODUCT_DIR environment variable, VERSion, 
# DEPENDency flags for ups, CHAIN name for ups, ups subdirectory name,
# Flavor for ups declares, which is built up from the Operating System,
# Qualifiers, and customization information, which is also used for 
# "cmd addproduct", and finally the distribution file name.
# this section may change in later ups|addproduct incarnations

            PROD=ftt
     PRODUCT_DIR=FTT_DIR
            VERS=v2_5
  TABLE_FILE_DIR=ups
      TABLE_FILE=$(PROD).table
           CHAIN=test
      UPS_SUBDIR=ups
 ADDPRODUCT_HOST=fnkits.fnal.gov
DISTRIBUTIONFILE=$(DEFAULT_DISTRIBFILE)

#   QUALS is added qualifiers, like: "QUALS=mips3:debug"
#
      UPS_SUBDIR=ups
# for Flavored products
         FLAVOR=$(DEFAULT_FLAVOR)
          QUALS=""
# for NULL products
#         FLAVOR=$(DEFAULT_NULL_FLAVOR)
#          QUALS=""
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
   LOCAL=/fnal/ups/$(PROD)/$(VERS)/$(FLAVOR)$(QUALS)
 DOCROOT=/afs/fnal/files/docs/products/$(PROD)
# - - - - - - - - - - - - -
# For example, you could use
# To take just the bin lib share and ups directories
#    ADDIRS = bin lib share ups	 
#   ADDFILES= 
#
# To take anything but *.out and *.bozo files
#    ADDIRS = 
#   ADDFILES= ! -name '*.out' ! -name '*.bozo'
#
# To take the whole nine yards
#    ADDIRS = . 
#   ADDFILES= 

#------------------------------------------------------------------
# --prefix=$(PREFIX) for gnu configure tools
# you should have build_prefix as a dependency of "all" if you use this
PREFIX=$(DEFAULT_PREFIX)

#------------------------------------------------------------------
# Files that have the version string in them
# this is used by "make setversion" to know what files need
# the version string replaced
VERSIONFILES=Makefile README $(UPS_SUBDIR)/INSTALL_NOTE

#------------------------------------------------------------------


SUBDIRS=ftt_lib ftt_test
all: proddir_is_set
	for d in $(SUBDIRS); do (cd $$d; $(MAKE) install); done

memdebug: proddir_is_set
	for d in $(SUBDIRS); do (cd $$d; $(MAKE) clean install WARN=-DDEBUGMALLOC EXTRAOBJ=dmalloc/malloc.o ); done

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


readable: 
	chmod -R a+r .
	chmod -R a+rx bin

# test: runs regression tests 
# regression: runs tests on "clean" product
#      some religious discussions have centered on whether we should
#      require a clean before testing; it is this way because we want
#      to be sure clean doesn't remove anything neccesary to operating
#      the product.
#---------------
# examples:
# this is an *example* of a mythical product with a GNU Configure
# based module "gnuproduct", an X11R6 imake based module "xproduct", 
# and a locally developed product that uses "premake"
# "all" should probably depend on the product directory being set.
#all: proddir_is_set
#	sh -c "cd imports/gnuproduct; \
#	       test -f config.status || ./configure --prefix=$(PREFIX)"
#	cd imports/diffutils ; make install
#	sh -c "cd imports/xproduct; \
#	       test -f Makefile || (xmkmf; make Makefiles)"
#	cd imports/xproduct ; make DEST=$$$(PRODUCT_DIR) install
#	cd fnal/premakedir ; premake -f $(FLAVOR) install
#	make $(UPS_SUBDIR)/toman
#
#clean: 				# clean up unneeded files	#
#	-cd imports/gnuproduct ; make clean; rm config.status
#	-cd imports/xproduct ; make clean; rm Makefile
#	-cd fnal/premakedir ; premake -f $(FLAVOR) clean
#
#spotless: clean
#	rm -rf bin lib 

#

# we indirect this a level so we can customize it for bundle products

declare: new_declare
undeclare: new_undeclare
addproduct: new_addproduct
delproduct: new_delproduct
autokits: new_autokits

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
# only new UPS style stuff is now supported

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

autolocal: distribution
	@/bin/echo "Press enter to update database? \c"
	@read line
	test -d $(LOCAL) || mkdir -p $(LOCAL)
	@$(LISTALL) | cpio -dumpv $(LOCAL)
	cd $(LOCAL); make check_manifest

install: local

#====================== NEW UPS COMMANDS =====================
# addproduct -- add the distribution file to a distribution platform, 
# and clean out the local copy
#
new_addproduct: distribution
	@$(NEW_ADDPRODUCT)
	rm $(DISTRIBUTIONFILE)

new_autokits: distribution
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
new_declare: dproducts_is_set new_declare_one
	@make "QUALS=build:$(QUALS)" new_declare_one

new_declare_one:
	@($(NEW_UPS_EXIST) && $(NEW_UPS_UNDECLARE)) || true
	@$(NEW_UPS_DECLARE) || true
	@$(NEW_UPS_LIST)

new_undeclare: dproducts_is_set  new_undeclare_one
	@make "QUALS=build:$(QUALS)" new_undeclare_one

new_undeclare_one:
	@$(NEW_UPS_UNDECLARE)

new_delproduct:
	@$(NEW_DELPRODUCT)

#=================================================================
# this is the usual target for manually rebuilding the software if it's just
# been checked out of the repository.  We declare it, set it up, and 
# build and regression test it.  Note that the make test will indirectly
# make clean (see above).
# 
SETUP_BUILD= echo $(SETUP_BUILD1); $(SETUP_BUILD1)
SETUP_BUILD1=setup -P -q \"build?:$(QUALS)\" -f $(FLAVOR) $(PROD) 	\
		-r $(DIR) -M $(TABLE_FILE_DIR) -m $(TABLE_FILE)	

SETUP_PLAIN= echo $(SETUP_PLAIN1); $(SETUP_PLAIN1)
SETUP_PLAIN1=setup -P -f $(FLAVOR) -q \"$(QUALS)\" $(PROD) 		\
		-r $(DIR) -M $(TABLE_FILE_DIR) -m $(TABLE_FILE)
build_n_test:
	@echo "\
	UPS_SHELL=sh; export UPS_SHELL; . `ups setup ups` ;\
	$(SETUP_BUILD)					;\
	echo make all; make all 			;\
	$(SETUP_PLAIN)					;\
	echo make regression; make regression " | /bin/sh

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

check_template_vars: check_common_vars check_new_vars

check_common_vars:
	@$(CHECKIT_DEF) ;\
	checkit "$(ADDPRODUCT_HOST)" "ADDPRODUCT_HOST"	;\
	checkit "$(CHAIN)" "CHAIN"			;\
	checkit "$(DIR)" "DIR"				;\
	checkit "$(DISTRIBUTIONFILE)" "DISTRIBUTIONFILE";\
	checkit "$(DOCROOT)" "DOCROOT"			;\
	checkit "$(FLAVOR)" "FLAVOR"			;\
	checkit "$(LOCAL)" "LOCAL"			;\
	checkit "$(PROD)" "PROD"			;\
	checkit "$(PRODUCT_DIR)" "PRODUCT_DIR"		;\
	checkit "$(SHELL)" "SHELL"			;\
	checkit "$(UPS_SUBDIR)" "UPS_SUBDIR"		;\
	checkit "$(VERS)" "VERS"			;\
	checkit "$(VERSIONFILES)" "VERSIONFILES"	

check_new_vars:
	@$(CHECKIT_DEF) ;\
	checkit "$(TABLE_FILE)" "TABLE_FILE"		;\
	checkit "$(TABLE_FILE)" "TABLE_FILE_DIR"	;\

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
DEFAULT_DISTRIBFILE="$(DIR)/../$(PROD)$(FLAVOR)$(QUALS)$(VERS).tar"
DEFAULT_FLAVOR=`ups flavor -2`
DEFAULT_NULL_FLAVOR=NULL

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

# These are all basic ups commands with loads of options.

NEW_UPS_EXIST=\
	echo $(UPS_DIR)/bin/ups exist \
		-z $(DPRODUCTS) \
		-f $(FLAVOR) \
		-q $(QUALS) \
		$(PROD) $(VERS);\
	$(UPS_DIR)/bin/ups exist \
		-z $(DPRODUCTS) \
		-f $(FLAVOR) \
		-q $(QUALS) \
		$(PROD) $(VERS)

NEW_UPS_UNDECLARE=\
	echo $(UPS_DIR)/bin/ups undeclare \
		-z $(DPRODUCTS) \
		-f $(FLAVOR) \
		-q $(QUALS) \
		$(PROD) $(VERS);\
	$(UPS_DIR)/bin/ups undeclare \
		-z $(DPRODUCTS) \
		-f $(FLAVOR) \
		-q $(QUALS) \
		$(PROD) $(VERS)

NEW_UPS_DECLARE= \
	echo $(UPS_DIR)/bin/ups declare \
		-M $(TABLE_FILE_DIR) \
		-m $(TABLE_FILE) \
		-z $(DPRODUCTS) \
		-U $(UPS_SUBDIR) \
		-r $(DIR) \
		-f $(FLAVOR) \
		-q $(QUALS) \
		$(PROD) $(VERS);\
	$(UPS_DIR)/bin/ups declare \
		-M $(TABLE_FILE_DIR) \
		-m $(TABLE_FILE) \
		-z $(DPRODUCTS) \
		-U $(UPS_SUBDIR) \
		-r $(DIR) \
		-f $(FLAVOR) \
		-q $(QUALS) \
		$(PROD) $(VERS)

NEW_UPS_LIST = \
	echo $(UPS_DIR)/bin/ups list \
		-z $(DPRODUCTS) \
		-f $(FLAVOR) \
		-q $(QUALS) \
		$(PROD) $(VERS);\
	$(UPS_DIR)/bin/ups list \
		-z $(DPRODUCTS) \
		-f $(FLAVOR) \
		-q $(QUALS) \
		$(PROD) $(VERS) 

NEW_ADDPRODUCT = \
	(test ! -z "$(UPD_DIR)" || (echo upd must be setup!; false ));\
		echo $(NEW_ADDPRODUCT_1); $(NEW_ADDPRODUCT_1)

NEW_ADDPRODUCT_1 = \
	upd addproduct \
	        -h $(ADDPRODUCT_HOST) \
		-T $(DISTRIBUTIONFILE) \
		-M $(TABLE_FILE_DIR) \
		-m $(TABLE_FILE) \
		`test -z '$(UPS_SUBDIR)'  || echo -U $(UPS_SUBDIR)` \
		-f $(FLAVOR) \
		-q $(QUALS) \
		$(PROD) $(VERS)

NEW_DELPRODUCT = \
	(test ! -z "$(UPD_DIR)" || (echo upd must be setup!; false ));\
	     echo $(NEW_DELPRODUCT_1); $(NEW_DELPRODUCT_1)

NEW_DELPRODUCT_1 = \
	upd delproduct \
                -h $(ADDPRODUCT_HOST) \
		-f $(FLAVOR) \
		-q $(QUALS) \
		$(PROD) $(VERS)


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
