# This Makefile invokes the a pltform specific makefile.`uname`
# which builds dynamically loadable, sharable libraries for 
# each .c file in this directory.  
# ALL .c FILES IN THE modules DIRECTORY MUST FOLLOW A SPECIFIC FORMAT!

SHELL =         /bin/sh

UNAME =`uname`


all:
	@if [ x = x$(PYTHONLIB) ] ; then\
		echo "REMEMBER TO SETUP PYTHON & ENSTORE"; exit 1;\
	fi;\
	for SRC in *.c ; do\
		MOD=`echo $$SRC | sed -e s/\.c/.so/`;\
		echo "   Making $$MOD";\
		${MAKE} -f makefile.$(UNAME) $$MOD;\
	done

clean:
	rm *.o *.so
