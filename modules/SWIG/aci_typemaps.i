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

/*Convert None to NULL */
%typemap(python, in) char *clientname {
    if ($source == Py_None)
	 $target = (char *)0;
    else
	 $target = PyString_AsString($source);
}

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

%typemap(python, ignore) struct aci_req_entry* [ANY] {
    static struct aci_req_entry *result[$dim0];
    $target = result;
}

%typemap(python, argout) struct aci_req_entry* [ANY]{
    int i;
    char ptr[128];
    
    for (i=0; i<$dim0 && $source[i]->request_no; ++i){
	SWIG_MakePtr(ptr, $source[i], "_struct_aci_req_entry_p");
	$target = return_list($target, PyString_FromString(ptr));
    }
}

%typemap(python, ignore) struct aci_drive_entry* [ANY] {
    static struct aci_drive_entry *result[$dim0];
    $target = result;
}

%typemap(python, argout) struct aci_drive_entry* [ANY]{
    int i;
    char ptr[128];

    for (i=0; i<$dim0 && $source[i]->drive_name[0]; ++i){
	SWIG_MakePtr(ptr, $source[i], "_struct_aci_drive_entry_p");
	$target = return_list($target, PyString_FromString(ptr));
    }

}

%typemap(python, ignore) struct aci_client_entry * {
    static struct aci_client_entry result;
    $target = &result;
}

%typemap(python, argout) struct aci_client_entry * {
    char ptr[128];

    SWIG_MakePtr(ptr, $source, "_struct_aci_client_entry_p");
    $target = return_list($target, PyString_FromString(ptr));
}




%typemap(python, ignore) struct aci_vol_desc * {
    static struct aci_vol_desc result;
    $target = &result;
}

%typemap(python, argout) struct aci_vol_desc * {
    char ptr[128];

    SWIG_MakePtr(ptr, $source, "_struct_aci_vol_desc_p");
}





/* this is here rather than in aci_typedefs.h so it doesn't get copied
   to the output and conflict with the typedef in rpc.h */    
typedef int bool_t;



%typemap(python, ignore) char *volser_ranges[ANY] {
    static char *result[$dim0];
    $target = &result[0];
}

%typemap(python, argout) char *volser_ranges[ANY]{
    int i;
    for (i=0; i< $dim0; ++i){
	if ($source[i][0]){
	    $target = return_list($target,PyString_FromString($source[i]));
	} else {
	    break;
	}
    }
}


/* the enum aci_media might be handled in the aci_typedefs.h file
 instead of using these typemaps by adding this line there:
 typedef int enum aci_media */
 
%typemap(python, ignore) enum aci_media * {
    static enum aci_media result;
    $target = &result;
}

%typemap(python, argout) enum aci_media * {
    $target = return_list($target, PyInt_FromLong(* $source ));
}


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
    for (i=0; i<ACI_MAX_RANGES && $source[i][0]; ++i){
	$target = return_list($target,PyString_FromString($source[i]));
    }
}

#endif
