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

/*Convert None to NULL */
/*For function(s) in aci_shadow.i with "char *volser" as an argument. */
%typemap(python, in) char *volser(char volser_name[ACI_VOLSER_LEN]) {
    /* aci_qvolsrange() modifies these arguments.  Thus, we need to make
       sure that they have an ACI_VOLSER_LEN sized character array for
       things to fit into. */
    memset(volser_name, 0, ACI_VOLSER_LEN);
    if ($source == Py_None) {
	 volser_name[0] = '\0';
	 $target = volser_name;
    } else {
	 memcpy(volser_name, PyString_AsString($source), ACI_VOLSER_LEN);
	 $target = volser_name;
    }
}

%typemap(python, argout) char *volser(char volser_name[ACI_VOLSER_LEN]) {
    /* return the next volume to start with (for aci_qvolsrange)*/
    $target = return_list($target, PyString_FromString($source));
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


/* aci_client_entry */
%typemap(python, ignore) struct aci_client_entry *client {
    static struct aci_client_entry result;
    $target = &result;
}

%typemap(python, argout) struct aci_client_entry *client {
    char ptr[128];

    SWIG_MakePtr(ptr, $source, "_struct_aci_client_entry_p");
    $target = return_list($target, PyString_FromString(ptr));
}


/* aci_req_entry */
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

/* aci_drive_entry */
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

/* aci_vol_desc */

%typemap(python, ignore) struct aci_vol_desc *desc {
    static struct aci_vol_desc result;
    $target = &result;
}

%typemap(python, argout) struct aci_vol_desc *desc {
    char ptr[128];

    SWIG_MakePtr(ptr, $source, "_struct_aci_vol_desc_p");
    $target = return_list($target, PyString_FromString(ptr));
}


/* this is here rather than in aci_typedefs.h so it doesn't get copied
   to the output and conflict with the typedef in rpc.h */    
typedef int bool_t;



%typemap(python, ignore) char *volser_ranges[ANY] {
    static char *result[$dim0];
    $target = &result[0];
}

%typemap(python, argout) char *volser_ranges[ANY] {
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


%typemap(python, ignore) int* nCount(int num) {
    $target = &num;
}

/* aci_volserinfo */
%typemap(python, ignore) struct aci_volserinfo* volserinfo{
    /* This situation is different from other lists.  We don't know
       what the length of the list will be, so we can't use $dim0. */
    static struct aci_volserinfo result[(ACI_MAX_QUERY_VOLSRANGE)];
    memset(result, 0, sizeof(result)); /* Insist this is cleared! */
    $target = result;
}

%typemap(python, argout) struct aci_volserinfo* volserinfo{
    /* Only aci_qvolsrange() in aci_shadow.i should have:
          struct aci_volserinfo* volserinfo
       as an argument. */
    int i;
    char ptr[128];
    struct aci_volserinfo* volserinfo_ptr[(ACI_MAX_QUERY_VOLSRANGE)];

    /* For aci_qvolsrange(), we need to make dynamic copies of elements in
       the static array.  Otherwise, when we return from this function
       the information gets released and a segmentation fault occurs when
       we finally do try and access it. */
    for (i=0; i < ACI_MAX_QUERY_VOLSRANGE && $source[i].volser[0]; ++i){
        volserinfo_ptr[i] = malloc(sizeof(struct aci_volserinfo));
        memcpy(volserinfo_ptr[i], &($source[i]),
               sizeof(struct aci_volserinfo));
        SWIG_MakePtr(ptr, volserinfo_ptr[i], "_struct_aci_volserinfo_p");
	$target = return_list($target, PyString_FromString(ptr));
    }
}


/* aci_media_info */
%typemap(python, ignore) struct aci_media_info* media_info {
    static struct aci_media_info result[(ACI_MAX_MEDIATYPES)];
    memset(result, 0, sizeof(result)); /* Insist this is cleared! */
    $target = result;
}

%typemap(python, argout) struct aci_media_info* media_info {
    /* Only aci_getcellinfo() in aci_shadow.i should have:
          struct aci_media_info* media_info
       as an argument. */
    int i;
    char ptr[128];
    struct aci_media_info* media_info_ptr[(ACI_MAX_MEDIATYPES)];

    /* For aci_getcellinfo(), we need to make dynamic copies of elements in
       the static array.  Otherwise, when we return from this function
       the information gets released and a segmentation fault occurs when
       we finally do try and access it. */
    for (i=0; i < ACI_MAX_MEDIATYPES && $source[i].eMediaType; ++i){
        media_info_ptr[i] = malloc(sizeof(struct aci_media_info));
        memcpy(media_info_ptr[i], &($source[i]),
               sizeof(struct aci_media_info));
        SWIG_MakePtr(ptr, media_info_ptr[i], "_struct_aci_media_info_p");
	$target = return_list($target, PyString_FromString(ptr));
    }
}
