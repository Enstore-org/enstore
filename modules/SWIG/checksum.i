%module checksum
%{
/* $Id */
#include "zlib.h"
%}

%include "typemaps.i"

%typedef unsigned long int zint;
%typedef char * cptr;

%typemap(python,in) zint {
	$target= (unsigned long) PyLong_AsLong($source);
}
%typemap(python,out) zint {
	$target= PyLong_FromLong((unsigned long)$source);
}
%typemap(python, in) cptr{
        $target= PyString_AsString($source);
}

zint adler32(zint, cptr, int);
