# Generic Makefile for dynamically linked extension modules.
#
# Digital Creations, info@digicool.com
# Copied from the oracledb module and modified by kurt for ETape

# Uncomment and modify these lines if you want to fix the location of
# the PYTHON installation and the python version.  Otherwise, set the
# environment variables before using this Makefile.
PYTHONHOME= $(PYTHON_DIR)
PYTHONVERSION=	1.5

SHELL =         /bin/sh

# The following lines should be left as is:
VERSION=	$(PYTHONVERSION)
pyinstalldir=	$(PYTHONHOME)
installdir=	$(PYTHONHOME)
exec_installdir=$(pyinstalldir)
INCLUDEPY=	$(PYTHONINC) 
LIBP=		$(PYTHONLIB)
LIBPL=		$(LIBP)/config
SRCDIR=		$(PYTHONHOME)/Python-$(PYTHONVERSION)/Modules
PYMAKE=		make -f $(LIBPL)/Makefile

# LIBSO is the location of platform-dependent dynamically linked 
# extension libraries.  This can be handy when you need to build 
# shared libraries that are not extensions but want to store them
# with other extensions and need to know where they are.
# Leave this line as it is.
LIBSO=		`$(PYMAKE) -s echodestshared`

# Put the object files for your module here:

WARN=  `if [ \`uname\` = Linux ];then echo "-Wall"; fi`
CFLAGS=-g $(WARN) -I./ -I$(FTT_DIR)/include `cat $$(FTT_DIR)/lib/libs` 

MAKEIT=  $(PYMAKE)  INCLDIR=$(INCLUDEPY) CONFIGINCLDIR=$(LIBPL) \
                ASHAREDMODULE=$$MODULE \
                ASHAREDMODULESOBS=$$OBS \
                ASHAREDMODULESEXTRA="$$YOURLIBS" \
                'OPT=$(CFLAGS) -lds' \
                'srcdir=$(SRCDIR)' \
                asharedmodule 



# The libraries to use
YOURLIBS="$(OLIB)/libftt.a -lds"
YOURLIBS=$(OLIB)/libftt.a 

# If you have any Python modules, include them here, so that they
# can get installed.
PYMODULES=

all:	EXfer ETape Devcodes

ETape:
	MODULE=ETape; \
	YOURLIBS="$(FTT_DIR)/lib/libftt.a -lds "; \
	OBS=ETape.o ; \
	$(MAKEIT)

EXfer:
	MODULE=EXfer; \
	YOURLIBS="$(FTT_DIR)/lib/libftt.a -lds "; \
	OBS=EXfer.o ; \
	$(MAKEIT)

Devcodes:
	MODULE=Devcodes; \
	YOURLIBS=;\
	OBS=Devcodes.o; \
	$(MAKEIT)

# Normally we don't install .py files:
install: installso installpyc

# But sometimes we may want to:
installpy: install
	for m in $(PYMODULES) the-end; do \
	  if [ "$$m" != the-end ]; then \
	    cp $$m.py $(installdir)/lib/python$(VERSION)/; \
	  fi; \
	done


installso: build
	if [ "$(MODULE)" != your-module ]; then \
	  $(PYMAKE) exec_prefix=$(installdir) \
		ASHAREDMODULE=$(MODULE) asharedinstall; \
	fi

installpyc:
	for m in $(PYMODULES) the-end; do \
	  if [ "$$m" != the-end ]; then \
	    python -c "import $$m"; \
	    cp $$m.pyc $(installdir)/lib/python$(VERSION)/; \
	  fi; \
	done

clean::
	-rm -f *.o *.so *~ *# so_locations
