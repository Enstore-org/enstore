%module checksum
%{
/* $Id */
#include "zlib.h"

unsigned long int adler32_o(unsigned long int crc, char *buf, int offset, int nbytes){
	return adler32(crc, buf+offset, nbytes);
}

%}

%include "typemaps.i"


#ifdef SWIG_VERSION
/* SWIG_VERSION was first used in swig 1.3.11 and has hex value 0x010311. */

%{
/* Include in the generated wrapper file */
typedef unsigned long int zint;
typedef char * cptr;
%}
/* Tell SWIG about it */
typedef unsigned long int zint;
typedef char * cptr;

%typemap(in) zint {
    if (PyLong_Check($input))
        $1 = (unsigned long) PyLong_AsUnsignedLong($input);
    else if (PyInt_Check($input))
        $1 = (unsigned long) PyInt_AsLong($input);
    else {
        PyErr_SetString(PyExc_TypeError, "expected integral type");
        return NULL;
    }
}
%typemap(out) zint {
        $result = PyLong_FromUnsignedLong((unsigned long)$1);
}
%typemap(in) cptr{
        $1= PyString_AsString($input);
}

#else
/* No SWIG_VERSION defined means a version older than 1.3.11.  Here we only
 * care to differentiate between 1.3.x and 1.1.y, though an issue exists
 * for 1.3 versions with a patch level 10 or less. */

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

#endif

zint adler32(zint, cptr, int);

zint adler32_o(zint, cptr, int, int);
