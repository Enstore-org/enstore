%module checksum
%{
/* $Id */
#include "zlib.h"
%}

%include "typemaps.i"

%typedef unsigned long int zint;
%typedef char * cptr;

%typemap(python,in) zint {
    if (PyLong_Check($source))
	$target= (unsigned long) PyLong_AsUnsignedLong($source);
    else if (PyInt_Check($source))
	$target= (unsigned long) PyInt_AsLong($source);
    else {
	PyErr_SetString(PyExc_TypeError, "expected integral type");
	return NULL;
    }
}
%typemap(python,out) zint {
	$target= PyLong_FromUnsignedLong((unsigned long)$source);
}
%typemap(python, in) cptr{
        $target= PyString_AsString($source);
}

zint adler32(zint, cptr, int);
