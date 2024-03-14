SHELL=/bin/sh
#############################################################
#
#  $Id$
#
#############################################################

ALL_TARGETS=tape_copy tape_write enroute2 m2probe t_rand t_read ecrc ddencp

all: adler32.a Makefile.modules Modules $(ALL_TARGETS)

# The target Makefile.modules generates a file Makefile.modules
# which builds dynamic load modules (.so files) for each of the .c files
# listed in the Setup file.   The file Makefile.pre.in knows how to make
# dll files on the platform and "make -f Makefile.pre.in boot" generates
# an appropriate Makefile (in a subdirectory) .   We copy it to
# ../Makefile.modules for the Modules: target
# The ACI/EMASS products are only made if they are setup

POSIX4=`if [ "_SunOS" = _\`uname\` ]; then echo "-lposix4"; else echo ""; fi`
RT=`if [ "_OSF1" = _\`uname\` ]; then echo "-lrt"; else echo ""; fi`
LIBSOCKETNSL=`if [ "_SunOS" = _\`uname\` ]; then echo "-lsocket -lnsl"; else echo ""; fi`
LIBGEN=`if [ "_IRIX" = _\`uname\` ]; then echo "-lgen"; else echo ""; fi`
LIBDL=`if [ "_OSF1" = _\`uname\` ]; then echo ""; else echo -ldl; fi`
PIC1=`if [ "_Linux" = _\`uname\` -a "_x86_64" = _\`uname -p\` ]; then echo "-fPIC"; fi`
PIC2=`if [ "_SunOS" = _\`uname\` -a "_i386" = _\`uname -p\` ]; then echo "-fPIC"; fi`
PIC=$(PIC1) $(PIC2)
BITS_FLAG=`if [ "_Linux" = _\`uname\` ]; then which_python=\`which python\`; bits=\`file -L $$which_python | cut -f 3 -d " " | cut -f 1 -d "-"\`; echo -m$$bits ; fi`
MACOS_FLAGS=`if [ "_Darwin" = _\`uname\` ]; then echo "-arch ppc -arch i386 -isysroot /Developer/SDKs/MacOSX10.4u.sdk"; fi`
XINCLUDES=-I/usr/X11R6/include/X11 -I/usr/X11R6/include/ -I/usr/include/X11
XLIBS=-L/usr/X11R6/lib/X11 -L/usr/X11R6/lib64 -L/usr/X11R6/lib -L/usr/lib/X11 -lX11 -lm
#-lXext -lXmu -lXt -lXi -lSM -lICE
FTT_INCLUDES=-I. -I$(FTT_DIR)/ftt_lib -I$(FTT_DIR)/include
FTT_LIBS=-L. -L$(FTT_DIR)/lib -L$(FTT_DIR)/ftt_lib -lftt $(LIBDS)
FTTTEST_LIBS=$(FTT_LIBS) -lftt_test
CFLAGS=$(BITS_FLAG) $(MACOS_FLAGS)

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
	cp $(PYTHON_DIR)/Python-*/Makefile.pre . ; \
	cp $(PYTHONLIB)/config/makesetup . ;\
	./makesetup -c - ; \
	cp Makefile ../Makefile.modules

Modules:
	@echo "MAKING MODULES ================================"
	cd SWIG && make
	@echo "in $$PWD"
	make BITS_FLAG=$(BITS_FLAG) -f Makefile.modules oldsharedmods

#Modules made using swig need to have two names; one for swig 1.1 and the
# other for swig 1.3.  Those with the letter c in the middle are for
# swig 1.1.
#	- cp aci_shadowcmodule.so _aci_shadowmodule.so
#	- cp derrnocmodule.so _derrnomodule.so
	cp checksumcmodule.so _checksummodule.so
	cp socket_extcmodule.so _socket_extmodule.so
	cp strbuffercmodule.so _strbuffermodule.so
	cp ftt2cmodule.so _ftt2module.so
	cp runoncmodule.so _runonmodule.so
	cp enroutecmodule.so _enroutemodule.so
	cp mtxcmodule.so _mtxmodule.so

adler32.o: adler32.c
	$(CC) $(CFLAGS) -c $(PIC) adler32.c

adler32.a: adler32.o
	ar cr $@ adler32.o
	ranlib $@

tape_copy: tape_copy.c
	if [ "$(FTT_DIR)"x != x ]; then \
		$(CC) $(CFLAGS) $(FTT_INCLUDES) -o $@ tape_copy.c $(FTTTEST_LIBS); \
	else \
		echo "Skipping $@.  FTT_DIR not found."; \
	fi;

tape_write: tape_write.c
	if [ "$(FTT_DIR)"x != x ]; then \
		$(CC) $(CFLAGS) $(FTT_INCLUDES) -o $@ tape_write.c $(FTTTEST_LIBS); \
	else \
		echo "Skipping $@.  FTT_DIR not found."; \
	fi;

m2probe: m2probe.c
	if [ "$(FTT_DIR)"x != x ]; then \
		$(CC) $(CFLAGS) $(FTT_INCLUDES) -o $@ m2probe.c $(FTT_LIBS); \
		cp m2probe ../sbin; \
	else \
		echo "Skipping $@.  FTT_DIR not found."; \
	fi;

enroute2: enroute2.c
	$(CC) $(CFLAGS) enroute2.c -o $@ $(LIBSOCKETNSL)

t_rand: t_rand.c
	$(CC) $(CFLAGS) t_rand.c -o $@

t_read: t_read.c
	$(CC) $(CFLAGS) t_read.c -o $@

ecrc: ecrc.c adler32.a
	$(CC) $(CFLAGS) -I. -o $@ ecrc.c adler32.a $(LIBGEN)
	cp $@ ../sbin

ddencp: EXfer.c adler32.a
	$(CC) $(CFLAGS) -I. -DSTAND_ALONE -DDEBUG_REVERT -o $(@) EXfer.c adler32.a -lpthread $(POSIX4) $(RT) $(LIBGEN) $(LIBSOCKETNSL)
	cp $(@) ../bin

autologinenstore: autologinenstore.c
	- $(CC) $(CFLAGS) -o $@ autologinenstore.c

mouse_move: mouse_move.c
	- $(CC) $(CFLAGS) -o $@ $(XINCLUDES) mouse_move.c $(XLIBS)

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
