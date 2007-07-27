SHELL=/bin/sh
#############################################################
#
#  $Id$
#
#############################################################

ALL_TARGETS=tape_copy tape_write enroute2 m2probe t_rand t_read ecrc ddencp mouse_move autologinenstore

all: adler32.a Makefile.modules Modules $(ALL_TARGETS)

# The target Makefile.modules generates a file Makefile.modules
# which builds dynamic load modules (.so files) for each of the .c files
# listed in the Setup file.   The file Makefile.pre.in knows how to make
# dll files on the platform and "make -f Makefile.pre.in boot" generates
# an appropriate Makefile (in a subdirectory) .   We copy it to
# ../Makefile.modules for the Modules: target
# The ACI/EMASS products are only made if they are setup

POSIX4=`if [ "_SunOS" = _$(MACH_OS) ]; then echo "-lposix4"; else echo ""; fi`
RT=`if [ "_OSF1" = _$(MACH_OS) ]; then echo "-lrt"; else echo ""; fi`
LIBSOCKETNSL=`if [ "_SunOS" = _$(MACH_OS) ]; then echo "-lsocket -lnsl"; else echo ""; fi`
LIBGEN=`if [ "_IRIX" = _$(MACH_OS) ]; then echo "-lgen"; else echo ""; fi`
LIBDL=`if [ "_OSF1" = _$(MACH_OS) ]; then echo ""; else echo -ldl; fi`
PIC=`if [ "_x86_64" = _\`uname -p\` ]; then echo "-fPIC"; fi`
XINCLUDES=-I/usr/X11R6/include/X11 -I/usr/X11R6/include/ -I/usr/include/X11
XLIBS=-L/usr/X11R6/lib/X11 -L/usr/X11R6/lib64 -L/usr/X11R6/lib -L/usr/lib/X11 -lX11 -lXext -lXmu -lXt -lXi -lSM -lICE -lm
FTT_INCLUDES=-I. -I$(FTT_DIR)/ftt_lib -I$(FTT_DIR)/include
FTT_LIBS=-L. -L$(FTT_DIR)/lib -L$(FTT_DIR)/ftt_lib -lftt $(LIBDS)
FTTTEST_LIBS=$(FTT_LIBS) -lftt_test

Makefile.modules: Setup 
	@echo "MAKING MODULE Makefile  ================================"
	if [ ! -d mksetup ] ; then  mkdir mksetup; fi
	cd mksetup/ ; \
	cp ../Setup . ; \
	if [ "$(ACI_DIR)"x = x ]; then \
		grep -v ACI Setup > x; \
		mv x Setup; \
	fi ; \
	if [ "$(DRIVESTAT_CLIENT_DIR)"x = x ]; then \
		grep -v DRIVESTAT_CLIENT Setup > x; \
		mv x Setup; \
	fi ; \
	if [ "$(FTT_DIR)"x = x ]; then \
		grep -v FTT_DIR Setup > x; \
		mv x Setup; \
	fi ; \
	if [ "`basename $(PYTHON_DIR)`" = "v2_0_E_1" -o "`basename $(PYTHON_DIR)`" = "python" ]; then \
		cp $(PYTHONLIB)/config/Makefile.pre.in . ; \
		make -f Makefile.pre.in boot                    ; \
	else \
		cp $(PYTHON_DIR)/Python-*/Makefile.pre . ; \
		cp $(PYTHONLIB)/config/makesetup . ;\
		./makesetup -c - ; \
	fi; \
	cp Makefile ../Makefile.modules

Modules:
	@echo "MAKING MODULES ================================"
	cd SWIG && make 
	@echo "in $$PWD"
	if [ "`basename $(PYTHON_DIR)`" = "v2_0_E_1" -o "`basename $(PYTHON_DIR)`" = "python" ]; then \
		make -f Makefile.modules ; \
	else \
		make -f Makefile.modules oldsharedmods; \
	fi

adler32.a: adler32.c
	$(CC) -c $(PIC) adler32.c
	ar cr $@ adler32.o

tape_copy: tape_copy.c
	if [ "$(FTT_DIR)"x != x ]; then \
		$(CC) $(FTT_INCLUDES) -o $@ tape_copy.c $(FTTTEST_LIBS); \
	else \
		echo "Skipping $@.  FTT_DIR not found."; \
	fi;

tape_write: tape_write.c
	if [ "$(FTT_DIR)"x != x ]; then \
		$(CC) $(FTT_INCLUDES) -o $@ tape_write.c $(FTTTEST_LIBS); \
	else \
		echo "Skipping $@.  FTT_DIR not found."; \
	fi;

m2probe: m2probe.c
	if [ "$(FTT_DIR)"x != x ]; then \
		$(CC) $(FTT_INCLUDES) -o $@ m2probe.c $(FTT_LIBS); \
		cp m2probe ../sbin; \
	else \
		echo "Skipping $@.  FTT_DIR not found."; \
	fi;

enroute2: enroute2.c
	$(CC) enroute2.c -o $@ $(LIBSOCKETNSL)

t_rand: t_rand.c
	$(CC) t_rand.c -o $@

t_read: t_read.c
	$(CC) t_read.c -o $@

ecrc: ecrc.c
	$(CC) -I. -o $@ ecrc.c adler32.a $(LIBGEN)
	cp $@ ../sbin

ddencp: EXfer.c
	$(CC) -I. -DSTAND_ALONE -DDEBUG_REVERT -o $(@) EXfer.c adler32.a -lpthread $(POSIX4) $(RT) $(LIBGEN) $(LIBSOCKETNSL)
	cp $(@) ../bin

autologinenstore: autologinenstore.c
	- $(CC) -o $@ autologinenstore.c

mouse_move: mouse_move.c
	- $(CC) -o $@ $(XINCLUDES) mouse_move.c $(XLIBS)

installpyc:
	for m in $(PYMODULES) the-end; do \
	  if [ "$$m" != the-end ]; then \
	    python -c "import $$m"; \
	    cp $$m.pyc $(installdir)/lib/python$(VERSION)/; \
	  fi; \
	done

clean:
	-rm -rf Makefile.modules
	-rm -rf *.o *.so *.a *~ *.pyc *.py
	-rm -rf Makefile.pre
	-rm -rf mksetup makesetup
	-rm -rf $(ALL_TARGETS) autologinenstore mouse_move
	@if [ ! -d ../sbin ];then mkdir ../sbin; fi
	cd SWIG && make clean
