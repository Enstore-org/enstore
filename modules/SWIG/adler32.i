%module adler32
%{
#include "zlib.h"
%}

%include "typemaps.i"

%typedef unsigned long int zint;

%typemap(python,in) zint {
	$target= (unsigned long) PyLong_AsLong($source);
}
%typemap(python,out) zint {
	$target= PyLong_FromLong((unsigned long)$source);
}

zint adler32(zint, char *, int);
