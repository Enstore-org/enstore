
SHELL =         /bin/sh

UNAME =`uname`

MODULES = ETape.so

all:
	for SRC in *.c ; do\
		MOD=`echo $SRC | sed -e s/\.c/.so/`;\
		${MAKE} -f makefile.$(UNAME) $(MODULES);\
	done

clean:
	rm *.o *.so
