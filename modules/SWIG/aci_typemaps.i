/*************************************************************
 * 
 * $Id$
 *
 * Typemaps for the aci_shadow module
 *
 ************************************************************/

%{
#include "return_list.c"
%}

/* allow structure members which are char arrays to be set */
%typemap(python, memberin) char[ANY]{
    /*XXX Warn about truncation ? */
    strncpy($target,$source,$dim0);
}

/* do not require version string return arguments to be supplied */
%typemap(python, ignore) version_string{
    static char result[ACI_MAX_VERSION_LEN];
    $target = &result[0];
}

/* convert version string return arg. to return value */
%typemap(python, argout) version_string{
    PyObject *o;
    o = PyString_FromString($source);
    $target = return_list($target,o);
}

/* handle an input list of aci_req_entry's */
%typemap(python, in) struct aci_req_entry* [ANY]{
    int i;
    PyObject *o;
    static struct aci_req_entry *result[$dim0];

    if (!PySequence_Check($source)) {
	PyErr_SetString(PyExc_TypeError, "not a sequence");
	return NULL;
    }
    
    if (PySequence_Length($source) != $dim0) {
	PyErr_SetString(PyExc_TypeError, "sequence too short");
	return NULL;
    }
    
    for (i=0; i<$dim0; ++i){
	o = PySequence_GetItem($source,i);
	if (!PyString_Check(o)){
	    /* XXX would be nice to accept an instance and get its .this attribute here */
	    PyErr_SetString(PyExc_TypeError, "not a string");
	    return NULL;
	}

	if (SWIG_GetPtr(PyString_AsString(o),(void**)&result[i],"_struct_aci_req_entry_p")){
	    PyErr_SetString(PyExc_TypeError, "expected struct_aci_req_entry_p");
	    return NULL;
	}
    }
    $target = result;
}

%typemap(python, in) struct aci_drive_entry* [ANY]{
    int i;
    PyObject *o;
    static struct aci_drive_entry *result[$dim0];

    if (!PySequence_Check($source)) {
	PyErr_SetString(PyExc_TypeError, "not a sequence");
	return NULL;
    }
    
    if (PySequence_Length($source) != $dim0) {
	PyErr_SetString(PyExc_TypeError, "sequence too short");
	return NULL;
    }
    
    for (i=0; i<$dim0; ++i){
	o = PySequence_GetItem($source,i);
	if (!PyString_Check(o)){
	    /* XXX would be nice to accept an instance and get its .this attribute here */
	    PyErr_SetString(PyExc_TypeError, "not a string");
	    return NULL;
	}

	if (SWIG_GetPtr(PyString_AsString(o),(void**)&result[i],"_struct_aci_drive_entry_p")){
	    PyErr_SetString(PyExc_TypeError, "expected struct_aci_drive_entry_p");
	    return NULL;
	}
    }
    $target = result;
}



/* this is here rather than in aci_typedefs.h so it doesn't get copied
   to the output and conflict with the typedef in rpc.h */    
typedef int bool_t;


#ifdef HANDLE_ACI_VOLSER_RANGE
/* handle arrays of range strings  (in struct aci_client_entry)  - doesn't work yet
*/
%typemap(python, memberin) aci_range{
    int len, i;
    PyObject *o;
    PyObject *seq = $source;
    if (!PySequence_Check(o)){
	PyErr_SetString(PyExc_TypeError,"not a sequence");
	return NULL;
    }
    len = PySequence_Length(o);
    if (len>ACI_MAX_RANGES) {
	/* XXX truncation warning */
	len = ACI_MAX_RANGES;
    }
    for (i=0;i<len;++i){
	o = PySequence_GetItem(seq,i);
	if (!PyString_Check(o)){
	    PyErr_SetString(PyExc_TypeError,"not a string");
	    return NULL;
	}
	strncpy($target[i],PyString_AsString(o),ACI_RANGE_LEN);
    }
}

%typemap(python, memberout) aci_range{
    int i;
    for (i=0; i<ACI_MAX_RANGES; ++i){
	if ($source[i][0]){
	    $target = return_list($target,PyString_FromString($source[i]));
	}
    }
}

#endif


