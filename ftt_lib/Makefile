SHELL=/bin/sh

LIBS=`cat local/libs`

SYSOBJ= ftt_scsi.o ftt_machdep.o

SCSISRC= AIX/ftt_scsi.c IRIX+4/ftt_scsi.c IRIX+5/ftt_scsi.c OSF1/ftt_scsi.c \
	SunOS/ftt_scsi.c VMS/ftt_scsi.c

MACHDEPSRC= AIX/ftt_machdep.c IRIX+4/ftt_machdep.c IRIX+5/ftt_machdep.c \
		OSF1/ftt_machdep.c SunOS/ftt_machdep.c

SUIDSRC= ftt_suid.c 
SUIDOBJ= ftt_suid.o

SRC= ftt_open.c ftt_tables.c ftt_skip.c ftt_stats.c ftt_info.c \
	ftt_rdwr.c ftt_do_scsi.c ftt_error.c ftt_debug.c ftt_global.c \
	ftt_label.c ftt_higher.c ftt_async.c ftt_find.c\
	$(SCSISRC) $(MACHDEPSRC) 

HDR= ftt_private.h ftt_common.h ftt_macros.h local/ftt_mtio.h \
	ftt_scsi.h ftt_types.h ftt.h ftt_voidtypes.h ftt_defines.h

OBJ= $(SYSOBJ) ftt_do_scsi.o ftt_error.o ftt_debug.o ftt_global.o \
	ftt_open.o ftt_rdwr.o ftt_tables.o ftt_skip.o ftt_stats.o ftt_info.o \
	ftt_label.o ftt_higher.o ftt_async.o ftt_find.o

CFLAGS=-I. -Ilocal -g  $(WARN) `cat local/flags`

LIB=libftt.a

PROGS=ftt_suid asyncdemo ftt_checktable

all: local $(LIB) $(PROGS)

clean:
	rm -f $(LIB) $(PROGS) *.o

PRINTEM=$(SRC) $(HDR) Makefile

print.ps: $(PRINTEM)
	a2ps -c -s2 -2 -n -l80 $(SRC) > print.ps
	a2ps -c -s2 -2 -n -l80 $(HDR) Makefile > header.ps

slides.ps: $(PRINTEM)
	a2ps -c -p -1 -n -l80 $(PRINTEM) > slides.ps

install: all ../lib ../bin ../include
	cp libftt.a ../lib
	cp local/libs ../lib
	rm -f ../bin/ftt_suid
	cp ftt_suid ../bin
	-chown root ../bin/ftt_suid
	-chmod 4755 ../bin/ftt_suid
	cp ftt.h ftt_common.h ftt_scsi.h ftt_voidtypes.h ftt_defines.h \
		../include

../lib ../bin ../include:
	mkdir $@

local:
	rm -f local ;\
	if [ -d `uname -s` ] ;\
	then \
		ln -s `uname -s` local;\
	elif [ -d "`uname -s`+`uname -r | cut -c1`" ] ;\
	then \
		ln -s "`uname -s`+`uname -r | cut -c1`" local ;\
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
	ar qv $(LIB) $(OBJ)

ftt_scsi.o: $(SCSISRC) 
	$(CC) $(CFLAGS) -c local/ftt_scsi.c

ftt_machdep.o: $(MACHDEPSRC)
	$(CC) $(CFLAGS) -c local/ftt_machdep.c
