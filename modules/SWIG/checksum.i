%module checksum
%{
/* $Id$ */
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
typedef long long off_t_2;
%}
/* Tell SWIG about it */
typedef unsigned long int zint;
typedef char * cptr;
typedef long long off_t_2;

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
%typemap(in) off_t_2 {
    if (PyLong_Check($input))
        $1 = (long long) PyLong_AsLongLong($input);
    else if (PyInt_Check($input))
        $1 = (long long) PyInt_AsLongLong($input);
    else {
        PyErr_SetString(PyExc_TypeError, "expected integral type");
        return NULL;
    }
}
#else
/* No SWIG_VERSION defined means a version older than 1.3.11.  Here we only
 * care to differentiate between 1.3.x and 1.1.y, though an issue exists
 * for 1.3 versions with a patch level 10 or less. */

%typedef unsigned long int zint;
%typedef char * cptr;
%typedef double off_t_2; /*Swig 1.1 doesn't have "long long" */

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
%typemap(python,in) off_t_2 {
    long long temp;
    /* Since SWIG 1.1 doesn't recognize "long long" as a data type, we
     * need to play some trickery to get it to work with large files.
     * Pretend it is a double, then pack the double with the bits from
     * the long long we really want.  When the double, which is really
     * the long long, is passed to convert_0_adler32_to_1_adler32 we get
     * the desired effect. */
    if (PyLong_Check($source)) {
	temp = (long long) PyLong_AsLongLong($source);
        memcpy(&($target), &temp, sizeof(long long));
    }	
    else if (PyInt_Check($source)) {
	temp = (long long) PyInt_AsLong($source);
        memcpy(&($target), &temp, sizeof(long long));
    }
    else {
	PyErr_SetString(PyExc_TypeError, "expected integral type");
	return NULL;
    }
}
#endif

zint adler32(zint, cptr, int);

zint adler32_o(zint, cptr, int, int);

zint convert_0_adler32_to_1_adler32(zint crc, off_t_2 filesize);
