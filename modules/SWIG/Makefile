############################################################
#  
# $Id$
#

SWIG_GENERATED_C_FILES=../aci_shadow_wrap.c ../derrno_wrap.c ../checksum_wrap_thread.c \
	../socket_ext_wrap.c ../strbuffer_wrap_thread.c ../_ftt_wrap_thread.c \
	../runon_wrap.c ../enroute_wrap.c

SWIG_GENERATED_PY_FILES=../aci_shadow.py

all: $(SWIG_GENERATED_C_FILES)   
# SWIG_GENERATED_PY_FILES are automatically created 
#at the same time as the C files

clean:
	rm -f *wrap* *doc* *.pyc 
	rm -f $(SWIG_GENERATED_C_FILES)
	rm -f $(SWIG_GENERATED_PY_FILES)

../aci_shadow_wrap.c: aci_shadow.i aci_typemaps.i aci_typedefs.h
	if [ "$(ACI_DIR)_" != _ ] ; then \
	swig -shadow -python -D__STDC__ aci_shadow.i ;\
	mv aci_shadow_wrap.c .. ;\
	mv aci_shadow.py .. ;\
	fi


../derrno_wrap.c: derrno.i
	if [ "$(ACI_DIR)_" != _ ] ; then \
	swig -I$(ACI_DIR)/inc -python derrno.i ; \
	mv derrno_wrap.c .. ; \
	fi

../checksum_wrap_thread.c: checksum.i
	swig -python checksum.i 
	./threadit.py < checksum_wrap.c > ../checksum_wrap_thread.c
	rm checksum_wrap.c


../_ftt_wrap_thread.c: _ftt.i
	if [ "$(FTT_DIR)_" != _ ] ; then \
		swig -I$(FTT_DIR)/include -python _ftt.i ; \
		./threadit.py < _ftt_wrap.c > ../_ftt_wrap_thread.c; \
		rm _ftt_wrap.c ; \
	fi

../socket_ext_wrap.c: socket_ext.i
	swig -python socket_ext.i 
	mv socket_ext_wrap.c .. 

../strbuffer_wrap_thread.c: strbuffer.i
	swig -python strbuffer.i
	./threadit.py < strbuffer_wrap.c > ../strbuffer_wrap_thread.c ; \
	rm strbuffer_wrap.c ;

../enroute_wrap.c: enroute.i
	swig -python enroute.i
	mv enroute_wrap.c ..

../runon_wrap.c: runon.i
	swig -python runon.i
	mv runon_wrap.c ..

