############################################################
#  
# $Id$
#

SWIG_GENERATED_C_FILES=../aci_shadow_wrap.c ../derrno_wrap.c ../checksum_wrap.c
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

../checksum_wrap.c: checksum.i
	swig -python checksum.i ; \
	mv checksum_wrap.c ..



