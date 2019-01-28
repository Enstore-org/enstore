############################################################
#
# $Id$
#

SWIG_GENERATED_C_FILES=../aci_shadow_wrap.c ../derrno_wrap.c \
	../checksum_wrap_thread.c ../socket_ext_wrap.c \
	../strbuffer_wrap_thread.c ../ftt2_wrap_thread.c \
	../runon_wrap.c ../enroute_wrap.c ../mtx_wrap_thread.c

SWIG_GENERATED_PY_FILES=../aci_shadow.py ../derrno.py ../checksum.py \
	../ftt2.py ../socket_ext.py ../strbuffer.py ../enroute.py \
	../runon.py ../mtx.py

all: $(SWIG_GENERATED_PY_FILES) $(SWIG_GENERATED_C_FILES)
# SWIG_GENERATED_PY_FILES are automatically created
#at the same time as the C files

clean:
	rm -f *wrap* *doc* *.pyc *~
	rm -f $(SWIG_GENERATED_C_FILES)
	rm -f $(SWIG_GENERATED_PY_FILES)

../aci_shadow_wrap.c: aci_shadow.i aci_typemaps.i aci_typedefs.h
	if [ "$(ACI_DIR)_" != _ ] ; then \
	swig -shadow -python -D__STDC__ aci_shadow.i ;\
	mv aci_shadow_wrap.c .. ;\
	mv aci_shadow.py .. ;\
	fi

../aci_shadow.py: ../aci_shadow_wrap.c


../derrno_wrap.c: derrno.i
	if [ "$(ACI_DIR)_" != _ ] ; then \
	swig -I$(ACI_DIR)/inc -shadow -python derrno.i ; \
	mv derrno_wrap.c .. ; \
	mv derrno.py .. ; \
	fi

../derrno.py: ../derrno_wrap.c


../checksum_wrap_thread.c: checksum.i
	swig -shadow -python -D__STDC__ checksum.i
	./threadit.py < checksum_wrap.c > ../checksum_wrap_thread.c
	rm checksum_wrap.c
	mv checksum.py ..

../checksum.py: ../checksum_wrap_thread.c



../ftt2_wrap_thread.c: ftt2.i
	if [ "$(FTT_DIR)_" != _ ] ; then \
		swig -I$(FTT_DIR)/include -shadow -python ftt2.i ; \
		./threadit.py < ftt2_wrap.c > ../ftt2_wrap_thread.c; \
		rm ftt2_wrap.c ; \
		mv ftt2.py .. ; \
	fi

../ftt2.py: ../ftt2_wrap_thread.c


../socket_ext_wrap.c: socket_ext.i
	swig -shadow -python socket_ext.i
	mv socket_ext_wrap.c ..
	mv socket_ext.py ..

../socket_ext.py: ../socket_ext_wrap.c


../strbuffer_wrap_thread.c: strbuffer.i
	swig -shadow -python strbuffer.i
	./threadit.py < strbuffer_wrap.c > ../strbuffer_wrap_thread.c ; \
	rm strbuffer_wrap.c ;
	mv strbuffer.py ..

../strbuffer.py: ../strbuffer_wrap_thread.c


../enroute_wrap.c: enroute.i
	swig -shadow -python enroute.i
	mv enroute_wrap.c ..
	if [ -f enroute.py ]; then \
		mv enroute.py .. ;  \
	fi

../enroute.py: ../enroute_wrap.c


../runon_wrap.c: runon.i
	swig -shadow -python runon.i
	mv runon_wrap.c ..
	if [ -f runon.py ]; then \
		mv runon.py .. ;  \
	fi

../runon.py: ../runon_wrap.c

../mtx_wrap_thread.c: mtx.i
	swig -I$(ENSTORE_DIR)/mtx/mtx-1.3.12 -shadow -python mtx.i
	./threadit.py < mtx_wrap.c > ../mtx_wrap_thread.c ; \
	rm mtx_wrap.c ;
	mv mtx.py ..

../mtx.py: ../mtx_wrap_thread.c
