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

#ifdef SWIG_VERSION
/* SWIG_VERSION was first used in swig 1.3.11 and has hex value 0x010311. */

/*Convert None to NULL */
%typemap(in) char *clientname {
    if ($input == Py_None)
	 $1 = (char *)0;
    else
	 $1 = PyString_AsString($input);
}

/*Convert None to NULL */
/*For function(s) in aci_shadow.i with "char *volser" as an argument. */
%typemap(in) char *volser(char volser_name[ACI_VOLSER_LEN]) {
    /* aci_qvolsrange() modifies these arguments.  Thus, we need to make
       sure that they have an ACI_VOLSER_LEN sized character array for
       things to fit into. */
    memset(volser_name, 0, ACI_VOLSER_LEN);
    if ($input == Py_None) {
	 volser_name[0] = '\0';
	 $1 = volser_name;
    } else {
	 memcpy(volser_name, PyString_AsString($input), ACI_VOLSER_LEN);
	 $1 = volser_name;
    }
}

%typemap(argout) char *volser(char volser_name[ACI_VOLSER_LEN]) {
    /* return the next volume to start with (for aci_qvolsrange)*/
    $result = return_list($result, PyString_FromString($1));
}

/* allow structure members which are char arrays to be set */
%typemap(memberin) char[ANY]{
    /*XXX Warn about truncation ? */
    strncpy($1,$input,$dim0);
}

/* do not require version string return arguments to be supplied */
%typemap(in, numinputs=0) version_string{
    static char result[ACI_MAX_VERSION_LEN];
    $1 = &result[0];
}

/* convert version string return arg. to return value */
%typemap(argout) version_string{
    PyObject *o;
    o = PyString_FromString($1);
    $result = return_list($result,o);
}


/* aci_client_entry */
%typemap(in, numinputs=0) struct aci_client_entry *client {
    static struct aci_client_entry result;
    $1 = &result;
}

%typemap(argout) struct aci_client_entry *client {
    char ptr[128];

    SWIG_NewPointerObj(ptr, $1, "_struct_aci_client_entry_p");
    $result = return_list($result, PyString_FromString(ptr));
}


/* aci_req_entry */
%typemap(in, numinputs=0) struct aci_req_entry* [ANY] {
    static struct aci_req_entry *result[$dim0];
    $1 = result;
}

%typemap(argout) struct aci_req_entry* [ANY]{
    int i;
    char ptr[128];
    
    for (i=0; i<$dim0 && $1[i]->request_no; ++i){
	SWIG_NewPointerObj(ptr, $1[i], "_struct_aci_req_entry_p");
	$result = return_list($result, PyString_FromString(ptr));
    }
}

/* aci_drive_entry */
%typemap(in, numinputs=0) struct aci_drive_entry* [ANY] {
    static struct aci_drive_entry *result[$dim0];
    $1 = result;
}

%typemap(argout) struct aci_drive_entry* [ANY]{
    int i;
    char ptr[128];

    for (i=0; i<$dim0 && $1[i]->drive_name[0]; ++i){
	SWIG_NewPointerObj(ptr, $1[i], "_struct_aci_drive_entry_p");
	$result = return_list($result, PyString_FromString(ptr));
    }

}

/* aci_ext_drive_entry */
%typemap(in, numinputs=0) struct aci_ext_drive_entry* [ANY] {
    static struct aci_ext_drive_entry *result[$dim0];
    $1 = result;
}

%typemap(argout) struct aci_ext_drive_entry* [ANY]{
    int i;
    char ptr[128];

    for (i=0; i<$dim0 && $1[i]->drive_name[0]; ++i){
	SWIG_NewPointerObj(ptr, $1[i], "_struct_aci_ext_drive_entry_p");
	$result = return_list($result, PyString_FromString(ptr));
    }

}

/* aci_ext_drive_entry4 */
%typemap(in, numinputs=0) struct aci_ext_drive_entry4* [ANY] {
    static struct aci_ext_drive_entry4 *result[$dim0];
    $1 = result;
}

%typemap(argout) struct aci_ext_drive_entry4* [ANY]{
    int i;
    char ptr[128];

    for (i=0; i<$dim0 && $1[i]->drive_name[0]; ++i){
	SWIG_NewPointerObj(ptr, $1[i], "_struct_aci_ext_drive_entry4_p");
	$result = return_list($result, PyString_FromString(ptr));
    }

}

/* aci_vol_desc */

%typemap(in, numinputs=0) struct aci_vol_desc *desc {
    static struct aci_vol_desc result;
    $1 = &result;
}

%typemap(argout) struct aci_vol_desc *desc {
    char ptr[128];

    SWIG_NewPointerObj(ptr, $1, "_struct_aci_vol_desc_p");
    $result = return_list($result, PyString_FromString(ptr));
}


/* this is here rather than in aci_typedefs.h so it doesn't get copied
   to the output and conflict with the typedef in rpc.h */    
typedef int bool_t;



%typemap(in, numinputs=0) char *volser_ranges[ANY] {
    static char *result[$dim0];
    $1 = &result[0];
}

%typemap(argout) char *volser_ranges[ANY] {
    int i;
    for (i=0; i< $dim0; ++i){
	if ($1[i][0]){
	    $result = return_list($result,PyString_FromString($1[i]));
	} else {
	    break;
	}
    }
}


/* the enum aci_media might be handled in the aci_typedefs.h file
 instead of using these typemaps by adding this line there:
 typedef int enum aci_media */
 
%typemap(in, numinputs=0) enum aci_media * {
    static enum aci_media result;
    $1 = &result;
}


%typemap(in, numinputs=0) int* nCount(int num) {
    $1 = &num;
}

/* aci_volserinfo */
%typemap(in, numinputs=0) struct aci_volserinfo* volserinfo{
    /* This situation is different from other lists.  We don't know
       what the length of the list will be, so we can't use $dim0. */
    static struct aci_volserinfo result[(ACI_MAX_QUERY_VOLSRANGE)];
    memset(result, 0, sizeof(result)); /* Insist this is cleared! */
    $1 = result;
}

%typemap(argout) struct aci_volserinfo* volserinfo{
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
    for (i=0; i < ACI_MAX_QUERY_VOLSRANGE && $1[i].volser[0]; ++i){
        volserinfo_ptr[i] = malloc(sizeof(struct aci_volserinfo));
        memcpy(volserinfo_ptr[i], &($1[i]),
               sizeof(struct aci_volserinfo));
        SWIG_NewPointerObj(ptr, volserinfo_ptr[i], "_struct_aci_volserinfo_p");
	$result = return_list($result, PyString_FromString(ptr));
    }
}


/* aci_media_info */
%typemap(in, numinputs=0) struct aci_media_info* media_info {
    static struct aci_media_info result[(ACI_MAX_MEDIATYPES)];
    memset(result, 0, sizeof(result)); /* Insist this is cleared! */
    $1 = result;
}

%typemap(argout) struct aci_media_info* media_info {
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
    for (i=0; i < ACI_MAX_MEDIATYPES && $1[i].eMediaType; ++i){
        media_info_ptr[i] = malloc(sizeof(struct aci_media_info));
        memcpy(media_info_ptr[i], &($1[i]),
               sizeof(struct aci_media_info));
        SWIG_NewPointerObj(ptr, media_info_ptr[i], "_struct_aci_media_info_p");
	$result = return_list($result, PyString_FromString(ptr));
    }
}

#else
/* No SWIG_VERSION defined means a version older than 1.3.11.  Here we only
 * care to differentiate between 1.3.x and 1.1.y, though an issue exists
 * for 1.3 versions with a patch level 10 or less. */

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

/* aci_ext_drive_entry */
%typemap(python, ignore) struct aci_ext_drive_entry* [ANY] {
    static struct aci_ext_drive_entry *result[$dim0];
    $target = result;
}

%typemap(python, argout) struct aci_ext_drive_entry* [ANY]{
    int i;
    char ptr[128];

    for (i=0; i<$dim0 && $source[i]->drive_name[0]; ++i){
	SWIG_MakePtr(ptr, $source[i], "_struct_aci_ext_drive_entry_p");
	$target = return_list($target, PyString_FromString(ptr));
    }

}

/* aci_ext_drive_entry4 */
%typemap(python, ignore) struct aci_ext_drive_entry4* [ANY] {
    static struct aci_ext_drive_entry4 *result[$dim0];
    $target = result;
}

%typemap(python, argout) struct aci_ext_drive_entry4* [ANY]{
    int i;
    char ptr[128];

    for (i=0; i<$dim0 && $source[i]->drive_name[0]; ++i){
	SWIG_MakePtr(ptr, $source[i], "_struct_aci_ext_drive_entry4_p");
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

#endif
