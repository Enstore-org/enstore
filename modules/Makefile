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

# Put your module name here:
MODULE=ETape

# Put the object files for your module here:
OBS=ETape.o

CFLAGS=-g  -I./ -I$(FTT_DIR)/include 

# Change this to the location of your libraries....
OLIB=$(FTT_DIR)/lib

# The libraries to use
#YOURLIBS=$(OLIB)/libftt.a /usr/lib/libds.a
YOURLIBS=$(OLIB)/libftt.a 

EXTRALD=$(YOURLIBS)

# If you have any Python modules, include them here, so that they
# can get installed.
PYMODULES=

build:
	if [ "$(MODULE)" != your-module ]; then \
	  $(PYMAKE) INCLDIR=$(INCLUDEPY) CONFIGINCLDIR=$(LIBPL) \
		ASHAREDMODULE=$(MODULE) \
		'ASHAREDMODULESOBS=$(OBS)' \
		'ASHAREDMODULESEXTRA=$(EXTRALD)' \
		'OPT=$(CFLAGS)' \
		'srcdir=$(SRCDIR)' \
		asharedmodule; \
	fi

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
