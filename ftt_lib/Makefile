SHELL=/bin/sh

LIBS=`cat local/libs`

SYSOBJ= ftt_scsi.o ftt_machdep.o

SCSISRC= AIX/ftt_scsi.c IRIX+4/ftt_scsi.c IRIX+5/ftt_scsi.c OSF1/ftt_scsi.c \
	SunOS/ftt_scsi.c VMS/ftt_scsi.c Linux/ftt_scsi.c

MACHDEPSRC= AIX/ftt_machdep.c IRIX+4/ftt_machdep.c IRIX+5/ftt_machdep.c \
		OSF1/ftt_machdep.c SunOS/ftt_machdep.c Linux/ftt_machdep.c

SUIDSRC= ftt_suid.c 
SUIDOBJ= ftt_suid.o

SRC= ftt_open.c ftt_tables.c ftt_skip.c ftt_stats.c ftt_info.c \
	ftt_rdwr.c ftt_do_scsi.c ftt_error.c ftt_debug.c ftt_global.c \
	ftt_label.c ftt_higher.c ftt_async.c ftt_find.c\
	ftt_partition.c ftt_version.c \
	$(SCSISRC) $(MACHDEPSRC) 

HDR= ftt_private.h ftt_common.h ftt_macros.h local/ftt_mtio.h \
	ftt_scsi.h ftt_types.h ftt.h ftt_voidtypes.h ftt_defines.h

OBJ= $(SYSOBJ) ftt_do_scsi.o ftt_error.o ftt_debug.o ftt_global.o \
	ftt_open.o ftt_rdwr.o ftt_tables.o ftt_skip.o ftt_stats.o ftt_info.o \
	ftt_label.o ftt_higher.o ftt_async.o ftt_find.o \
	ftt_partition.o ftt_version.o

CFLAGS=-I. -Ilocal -g  $(WARN) `cat local/flags`

LIB=libftt.a

PROGS=ftt_suid asyncdemo ftt_checktable

all: local $(LIB) $(PROGS)

clean:
	rm -f $(LIB) $(PROGS) *.o local

PRINTEM=$(SRC) $(HDR) Makefile

print.ps: $(PRINTEM)
	a2ps -c -s2 -2 -n -l80 $(SRC) > print.ps
	a2ps -c -s2 -2 -n -l80 $(HDR) Makefile > header.ps

slides.ps: $(PRINTEM)
	a2ps -c -p -1 -n -l80 $(PRINTEM) > slides.ps

BINDIR=../bin
LIBDIR=../lib
INCDIR=../include

dirs: $(INCDIR) $(BINDIR) $(LIBDIR)

includes: \
	$(INCDIR)/ftt.h \
	$(INCDIR)/ftt_common.h \
	$(INCDIR)/ftt_scsi.h \
	$(INCDIR)/ftt_voidtypes.h \
	$(INCDIR)/ftt_defines.h

fttlib: $(LIBDIR)/libftt.a $(LIBDIR)/libs

install: all dirs includes fttlib $(BINDIR)/ftt_suid

$(LIBDIR)/libftt.a: libftt.a
	cp libftt.a $(LIBDIR)/

$(LIBDIR)/libs: local/libs
	cp local/libs $(LIBDIR)/

$(BINDIR)/ftt_suid: ftt_suid
	rm -f $@
	cp ftt_suid $@
	-chown root $@
	-chmod 4755 $@

$(INCDIR) $(BINDIR) $(LIBDIR):
	mkdir $@

$(INCDIR)/ftt.h : ftt.h
	cp ftt.h $(INCDIR)/

$(INCDIR)/ftt_common.h : ftt_common.h
	cp ftt_common.h $(INCDIR)/

$(INCDIR)/ftt_scsi.h : ftt_scsi.h 
	cp ftt_scsi.h $(INCDIR)/

$(INCDIR)/ftt_voidtypes.h :ftt_voidtypes.h 
	cp ftt_voidtypes.h  $(INCDIR)/

$(INCDIR)/ftt_defines.h : ftt_defines.h 
	cp ftt_defines.h  $(INCDIR)/


local:
	rm -f local ;\
	if [ -d `uname -s` ] ;\
	then \
		ln -s `uname -s` local;\
	elif [ -d "`uname -s | sed -e ' s/IRIX64/IRIX/' `+`uname -r | cut -c1`" ] ;\
	then \
		ln -s "`uname -s | sed -e ' s/IRIX64/IRIX/'`+`uname -r | cut -c1`" local ;\
	else \
		ln -s GENERIC local;\
	fi

testtables:
	rm -f ftt_error.o
	$(CC) $(CFLAGS) -DTESTTABLES ftt_error.c ftt_global.o -o testtables
	rm -f ftt_error.o

ftt_suid: $(SUIDOBJ) $(LIB)
	$(CC) -o $@ $(SUIDOBJ) $(LIB) $(LIBS)

asyncdemo: asyncdemo.c $(LIB)
	$(CC) $(CFLAGS) -o $@ asyncdemo.c $(LIB) $(LIBS)

listdemo: listdemo.c $(LIB)
	$(CC) $(CFLAGS) -o $@ listdemo.c $(LIB) $(LIBS)

ftt_checktable: ftt_checktable.c $(LIB)
	$(CC) $(CFLAGS) -o $@ ftt_checktable.c $(LIB) $(LIBS)


# this is shotgunning, crude but effective :-)
$(OBJ): $(HDR)

$(LIB): $(OBJ)
	rm -f $(LIB)
	if [ `uname` = Linux ];then opts=qvs;else opts=qv;fi; ar $$opts $(LIB) $(OBJ)

ftt_scsi.o: $(SCSISRC) 
	$(CC) $(CFLAGS) -c local/ftt_scsi.c

ftt_machdep.o: $(MACHDEPSRC)
	$(CC) $(CFLAGS) -c local/ftt_machdep.c

