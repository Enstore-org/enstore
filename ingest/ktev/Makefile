#
# $Id$
#

# Top Level Makfile

BIN_PATH = /afs/.desy.de/products/dcache

CP    = cp
RM    = rm -f
LINK  = ln -s
MV    = mv

TARGET = ktevV0 ktev_test 

all $(TARGET):

	@if  [ ! -f .depend ]; then \
		touch .depend;  \
		$(MAKE) depend; \
	fi

	@case `uname` in \
		SunOS) \
			$(MAKE) -f Makefile.sun $@ ; \
			;; \
		Linux) \
			$(MAKE) -f Makefile.linux $@ ; \
			;; \
		IRIX*) \
			$(MAKE) -f Makefile.sgi $@ ; \
			;; \
		AIX) \
			$(MAKE) -f Makefile.alpha $@ ; \
			;; \
		CYGWIN*) \
			$(MAKE) -f Makefile.w32 $@ ;\
			;; \
		*) \
			echo "Don't know your machine type" \
			;; \
	esac

release:
	@VER=`cat dcap_version.c | grep version`; \
	echo "Last version: $$VER"; \
	echo "Enter new version name:"; \
	read version; \
	$(RM) -f dcap_version.c; \
	echo "" > dcap_version.c; \
	echo "#define VER(a) #a \" \" __DATE__ \" \" __TIME__;" >> dcap_version.c; \
	echo "" >> dcap_version.c; \
	echo "const char *getDcapVersion()" >> dcap_version.c; \
	echo "{" >> dcap_version.c; \
	echo "	return VER( $$version );" >> dcap_version.c; \
	echo "}" >> dcap_version.c; \
	echo "" >> dcap_version.c; \
	cvs commit; \
	cvs tag -c -b $$version;

install: rebuild dirs
	@VER=`./version.sh`; \
	$(MV) libdcap.so libdcap$$VER.so; \
	$(RM) $(BIN_PATH)/lib/libdcap.so; \
	$(LINK) libdcap$$VER.so $(BIN_PATH)/lib/libdcap.so; \
	$(CP) libdcap$$VER.so $(BIN_PATH)/lib/; \
	$(MV) libpdcap.so libpdcap$$VER.so; \
	$(RM) $(BIN_PATH)/lib/libpdcap.so; \
	$(LINK) libpdcap$$VER.so $(BIN_PATH)/lib/libpdcap.so; \
	$(CP) libpdcap$$VER.so $(BIN_PATH)/lib/; \
	$(CP) dccp $(BIN_PATH)/bin/; \
	$(CP) dcap.h $(BIN_PATH)/include/; \
	$(CP) dcap_errno.h $(BIN_PATH)/include/; \
	$(CP) dc_hack.h $(BIN_PATH)/include/; \
	$(CP) dccp.c $(BIN_PATH)/sources/;

dirs:
	@./mkdirs.sh $(BIN_PATH)/bin/
	@./mkdirs.sh $(BIN_PATH)/lib
	@./mkdirs.sh $(BIN_PATH)/include/
	@./mkdirs.sh $(BIN_PATH)/sources/

rebuild: cleanall all
	@echo "Compilation Done."

clean:
	@rm -f *.o *.BAK core	

cleanall: clean	
	@rm -f $(TARGET) libdcap*.so libpdcap*.so dcap.h \
	.depend debug_map.h  debug_level.h mapfile
