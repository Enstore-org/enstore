/* $Id$ */

%module ftt

%{
    #include <ftt.h>
%}

%include pointer.i

%include "ftt_defines.h"

void ftt_eprintf(char *);  /* SWIG doesn't like varargs... */

%typedef int (*ftt_function_ptr)();  /* or function pointers */
void ftt_retry(ftt_descriptor, int, ftt_function_ptr, char *, int);


%include "ftt_common.h"






