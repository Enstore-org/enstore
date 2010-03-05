#include <Python.h>

#include <limits.h>
#include <errno.h>
#include <sys/types.h>
#if defined(__linux__)
#  include <attr/xattr.h>
#elif defined(__sun)
#  include <sys/stat.h>
#  include <fcntl.h>
#elif defined(__APPLE__)
#  include <sys/xattr.h>
#else
#error "Does not support your OS."
#endif


/***************************************************************************
 * prototypes
 **************************************************************************/

void initExtendedAttributes(void);
static PyObject * raise_exception(char *msg);
static char* extended_attributes(char *path, char *name);
static PyObject * ExtendedAttributes_get(PyObject *self, PyObject *args);
static PyObject * ExtendedAttributes_put(PyObject *self, PyObject *args);
static PyObject * extendedAttributesGet(char *path, char *name);
static PyObject * extendedAttributesPut(char *path, char *name, void *contents,
   int length);
static PyObject *ExtendedAttributesErrObject;

static char ExtendedAttributes_Doc[] =  "ExtendedAttributes accesses file metadata";

static char ExtendedAttributes_get_Doc[] = "\
ExtendedAttributesGet(path)";
static char ExtendedAttributes_put_Doc[] = "\
ExtendedAttributesGet(path, name, contents, length)";

/*  Module Methods table. 
 *
 *  There is one entry with four items for for each method in the module
 *
 *  Entry 1 - the method name as used  in python
 *        2 - the c implementation function
 *        3 - flags 
 *	  4 - method documentation string
 */

static PyMethodDef ExtendedAttributes_Methods[] = {
   { "extendedAttributesGet", ExtendedAttributes_get, 1, ExtendedAttributes_get_Doc},
   { "extendedAttributesPut", ExtendedAttributes_put, 1, ExtendedAttributes_put_Doc},
   { 0, 0}        /* Sentinel */
};

/***************************************************************************
 * python defined functions
 **************************************************************************/

static PyObject *
raise_exception(char *msg)
{
        PyObject	*v;
        int		i = errno;

#   ifdef EINTR
    if ((i==EINTR) && PyErr_CheckSignals()) return NULL;
#   endif

    /* note: format should be the same as in FTT.c */
    v = Py_BuildValue("(s,i,s,i)", msg, i, strerror(i), getpid());
    if (v != NULL)
    {   PyErr_SetObject(ExtendedAttributesErrObject, v);
	Py_DECREF(v);
    }
    return NULL;
}

static PyObject *
ExtendedAttributes_get(PyObject *self, PyObject *args)
{
  int sts;
  char *path, *use_name;
  PyObject *name = Py_None;
  PyObject *EA_dict;
  
  /* Get the parameter. */
  sts = PyArg_ParseTuple(args, "s|O", &path, &name);
  if (!sts)
     return(raise_exception("extended_attributes get - invalid parameter"));

  if(name == Py_None)
     use_name = NULL;
  else if(PyString_Check(name))
     use_name = PyString_AsString(name);
  else
     return(raise_exception("extended_attributes get - invalid parameter"));
  
  errno = 0;
  EA_dict = extendedAttributesGet(path, use_name);

  /* If a specific extended attribute was requested and we didn't find it,
   * return an exception. */
  if(use_name != NULL && PyDict_Size(EA_dict) == 0)
     return(raise_exception("extended_attributes get - attribute not found"));

  return EA_dict;
}

static PyObject *
ExtendedAttributes_put(PyObject *self, PyObject *args)
{
  int sts;
  char *path, *name;
  PyObject *contents;

  char *attr_string;
  size_t attr_length;
  
  /* Get the parameter. */
  sts = PyArg_ParseTuple(args, "ssO", &path, &name, &contents);
  if (!sts)
     return(raise_exception("extended_attributes put - invalid parameter"));

  /* Check to make sure this is a python string or None. */
  if(Py_None == contents)
  {
     /* If None was passed in for the contents, treat this like a delete. */
     attr_string = NULL;
     attr_length = 0;
  }
  else if(PyString_Check(contents))
  {
     attr_string = PyString_AsString(contents);
     attr_length = PyString_Size(contents);
  }
  else
  {
     return raise_exception("ExtendedAttributes put - invalid contents param");
  }
  
  errno = 0;
  return extendedAttributesPut(path, name, attr_string, attr_length);
}

/***************************************************************************
 * inititalization
 **************************************************************************
 *   Module initialization.   Python call the entry point init<module name>
 *   when the module is imported.  This should the only non-static entry point
 *   so it is exported to the linker.
 *
 *   First argument must be a the module name string.
 *   
 *   Second       - a list of the module methods
 *
 *   Third	- a doumentation string for the module
 * 
 *   Fourth & Fifth - see Python/modsupport.c
 */

void
initExtendedAttributes()
{
    PyObject	*m, *d;
    
    m = Py_InitModule4("ExtendedAttributes", ExtendedAttributes_Methods,
		       ExtendedAttributes_Doc, 
		       (PyObject*)NULL, PYTHON_API_VERSION);
    d = PyModule_GetDict(m);
    ExtendedAttributesErrObject = PyErr_NewException(
       "ExtendedAttributes.error", NULL, NULL);
    if (ExtendedAttributesErrObject != NULL)
	PyDict_SetItemString(d, "error", ExtendedAttributesErrObject);
}


#if defined(__linux__) || defined(__APPLE__)
/*
 * FreeBSD, H-PUX and IRIX I believe support the same mechanism.  They
 * remain untested.
 */

/* Get the actual attribute name to use.  This return value needs to be
 * free()ed. */
char* get_name(char * name)
{
   /* Extended attributes need the "name" to fit into a set of attribute
    * namespaces.  The namespaces currently defined are:
    *   security
    *   system
    *   trusted
    *   user
    * If a namespace is not included in the "name" value then the
    * default of 'user' is used.  The namespace and the attriubte name
    * are seperated by a period (.).  Attribute names should not contain
    * a period unless the part before the period is the namespace.
    * EOPNOTSUPP is the error if an incorrect namespace is specified. */

   char *use_name; /* Incase we need to add the attribute namespace. */
   const char *DEFAULT_NAMESPACE = "user";
   
   if(strchr(name, '.') == NULL)
   {
      /* Add two to the malloc size.  One for the "." seperating the
       * attribute namespace from the name and the other for the
       * trailing NULL. */
      use_name = (char*)malloc(strlen(name) + strlen(DEFAULT_NAMESPACE) + 2);
      if(use_name == NULL)
      {
	 return NULL;
      }
      (void) memset(use_name, 0, sizeof(use_name));

      /* Set the name to use with the default attribute namespace prepended. */
      (void) strncat(use_name, DEFAULT_NAMESPACE, sizeof(DEFAULT_NAMESPACE));
      (void) strncat(use_name, ".", 1);
      (void) strncat(use_name, name, sizeof(name));
   }
   else
   {
      /* Add one to the malloc size.  This is for the trailing NULL. */
      use_name = (char*)malloc(strlen(name) + 1);
      if(use_name == NULL)
      {
	 return NULL;
      }
      (void) strncpy(use_name, name, strlen(name) + 1);
   }

   return use_name;
}

static char* extended_attributes(char *path, char *name)
{
   ssize_t xattr_len;
   int sts = -1;
   void *ex_at;

   /* Obtain the initial guess of the length of the current return
    * value from getxattr(). */
#ifdef __linux__
   xattr_len = getxattr(path, name, NULL, 0); /* Get initial length to try. */
#elif defined ( __APPLE__ )
   xattr_len = getxattr(path, name, NULL, 0, 0, 0); /* Get initial length to try. */
#else
#error "not MacOS X or Linux"
#endif
   ex_at = malloc(xattr_len + 1);
   memset(ex_at, 0, xattr_len + 1);

   /* Since it is possible that between the previous getxattr() call and
    * the next call that a attribute size is increased, we need to handle
    * this with a loop that increase the size of the return value buffer when
    * ERANGE occurs. */
   errno = ERANGE;
   while(sts == -1 && errno == ERANGE)
   {
#ifdef __linux__
      sts = getxattr(path, name, ex_at, xattr_len);
#elif defined ( __APPLE__ )
      sts = getxattr(path, name, ex_at, xattr_len, 0, 0);
#else
#error "not MacOS X or Linux"
#endif
      if(sts == -1 && errno == ERANGE)
      {
	 xattr_len += 100;
	 ex_at = realloc(ex_at, xattr_len + 1);
	 memset(ex_at, 0, xattr_len + 1);
      }
      else if(sts == -1)
      {
	 fprintf(stderr, "Failed to obtain extended attribute list.\n");
	 /* This should raise an error. */
	 return NULL;
      }
   }

   return ex_at; /* Need to free this when done with it! */
}


PyObject * extendedAttributesGet(char *path, char *name)
{
   PyObject *extended_attribute_dict;
   ssize_t xattr_len;
   int sts = -1;

   char *use_name;  /* Incase we need to add the attribute namespace. */

   void *ex_at;
   void *cur_ex_at, *ex_at_list;
   
   extended_attribute_dict = PyDict_New(); /* New empty dictionary. */

   /* Obtain the initial guess of the length of the current return
    * value from listxattr(). */
#ifdef __linux__
   if((xattr_len = listxattr(path, NULL, 0)) < 0)
#elif defined ( __APPLE__ )
   if((xattr_len = listxattr(path, NULL, 0, 0)) < 0)
#else
#error "not MacOS X or Linux"
#endif
   {
      return raise_exception("Failed to obtain extened attribute list1");
   }
   ex_at_list = malloc(xattr_len + 1);
   memset(ex_at_list, 0, xattr_len + 1);

   /* Since it is possible that between the previous listxattr() call and
    * the next call that a attribute gets added, we need to handle this
    * with a loop that increase the size of the return value buffer when
    * ERANGE occurs. */
   errno = ERANGE;
   while(sts == -1 && errno == ERANGE)
   {
#ifdef __linux__
      sts = listxattr(path, ex_at_list, xattr_len);
#elif defined ( __APPLE__ )
      sts = listxattr(path, ex_at_list, xattr_len, 0);
#else
#error "not MacOS X or Linux"
#endif
      if(sts == -1 && errno == ERANGE)
      {
	 xattr_len += 100; /* make it bigger */
	 ex_at_list = realloc(ex_at_list, xattr_len + 1);
	 memset(ex_at_list, 0, xattr_len + 1);
      }
      else if(sts == -1)
      {
	 return raise_exception("Failed to obtain extened attribute list2");
      }
   }

   /* If the namespace was not included in the attribute name, get the
    * name in the default namespace. */
   if(name != NULL)
   {
      use_name = get_name(name);
      if(use_name == NULL)
	 return raise_exception("Failed to obtain memory for attributes");
   }
   else
      use_name = NULL;

   /* Now loop over the names in the list returned from listxattr().
    * The information stored in ex_at_list are strings seperated by NULLs. */
   cur_ex_at = ex_at_list;
   while(cur_ex_at < ex_at_list + xattr_len)
   {
      if(use_name != NULL && strcmp(use_name, cur_ex_at) != 0)
      {
	 /* Skip to the next item in the list of names. */
	 cur_ex_at = cur_ex_at + strlen(cur_ex_at) + 1;
	 
	 continue;
      }
	 
      /* Get the contents of the current extended attribute. */
      ex_at = extended_attributes(path, cur_ex_at);
      if(ex_at == NULL)
      {
	 /* If we get here, the most likely reason is a permission error,
	  * though it could be anything that could cause reading a file to
	  * fail.  If we aren't supposed to see it, don't worry about it
	  * and move onto the next one. */

	 if(use_name != NULL)
	 {
	    free(use_name);  /* Avoid resource leaks. */
	    
	    /* errno should still be set correctly. */
	    return extended_attribute_dict;
	 }
	 
	 /* Skip to the next item in the list of names. */
	 cur_ex_at = cur_ex_at + strlen(cur_ex_at) + 1;
	 
	 continue;
      }

      /* Add the item to the dictionary. */
      PyDict_SetItemString(extended_attribute_dict, cur_ex_at,
			   PyString_FromString(ex_at));

      free(ex_at);

      /* Skip to the next item in the list of names. */
      cur_ex_at = cur_ex_at + strlen(cur_ex_at) + 1;
   }

   /* We didn't find a matching extended attribute. Set appropriate error. */
   if(use_name != NULL)
   {
      errno = ENOENT;

      free(use_name);  /* Avoid resource leaks. */
   }

   free(ex_at_list);
   return extended_attribute_dict;
}

PyObject * extendedAttributesPut(char *path, char *name, void *contents,
				 int length)
{
   char *use_name; /* Incase we need to add the attribute namespace. */

   /* If the namespace was not included in the attribute name, get the
    * name in the default namespace. */
   if(name != NULL)
   {
      use_name = get_name(name);
      if(use_name == NULL)
	 return raise_exception("Failed to obtain memory for attributes1");
   }
   else
   {
      errno = ENOENT;
      return raise_exception("Failed to obtain memory for attributes2");
   }

   if(contents == NULL)
   {
      /* Treat None for the contents like a deleted. */
#ifdef __linux__
      if(removexattr(path, use_name) < 0)
#elif defined ( __APPLE__ )
      if(removexattr(path, use_name, 0) < 0)
#else
#error "not MacOS X or Linux"
#endif
      {
	 free(use_name);  /* Avoid resource leaks. */
	 return raise_exception("Failed to remove extended attribute");
      }
   }
   else
   {
      /* Write the contents of the extended attribute file. */
#ifdef __linux__
      if(setxattr(path, use_name, contents, length, 0) < 0)
#elif defined ( __APPLE__ )
	 if(setxattr(path, use_name, contents, length, 0, 0) < 0)
#else
#error "not MacOS X or Linux"
#endif
      {
	 free(use_name);  /* Avoid resource leaks. */
	 return raise_exception("Failed to write extended attribute");
      }
   }

   free(use_name);  /* Avoid resource leaks. */

   return Py_None; /*Py_RETURN_NONE;*/
}

#elif defined(__sun)
static char* extended_attributes(char *path, char *name)
{
/* Not used for SunOS implimentation. */
}

PyObject * extendedAttributesGet(char *path)
{
   /* See pages 590-593 of Solaris Systems Programming by Rich Teer for
    * explanations of how Solaris does extended attributes. */
   
   PyObject *extended_attribute_dict;

   DIR *dir_fp;
   int fd, attr_fd;
   struct dirent *dir;
   struct stat stat_info;
   
   void *ex_at;

   extended_attribute_dict = PyDict_New(); /* New empty dictionary. */

   /* Open the extended attribute "directory". */
   if((fd = attropen(path, ".", O_RDONLY)) < 0)
   {
      return raise_exception("Failed to obtain extened attribute list");
   }

   /* Convert it to a stream. */
   dir_fp = fopendir(fd);

   /* I'm not sure what this does.  It skips over the first two possible
    * attributes.  Perhaps these are to skip "." (dot) and ".." (dot-dot). */
   if(readdir(dir_fp) == NULL || readdir(dir_fp) == NULL)
   {
      return extended_attribute_dict; /* No attributes to return. */
   }

   /* Loop over the "directory contents". */
   while((dir = readdir(dir_fp)) != NULL)
   {
      /* Open the extended attribute file... */
      if((attr_fd = openat(fd, dir->dname, O_RDONLY | O_XATTR)) < -1)
      {
	 /* If we get here, the most likely reason is a permission error,
	  * though it could be anything that could cause reading a file to
	  * fail.  If we aren't meant to see it, don't worry about it
	  * and move onto the next one. */
	 continue
      }
      /* ...then stat it to find out how big it is... */
      if(stat(attr_fd, &stat_info) < 0)
      {
	 goto next2; /* Just close attr_fd. */
      }
      /* ...then allocate the memeory for the contents... */
      if((ex_at = malloc(stat_info.st_size + 1) == NULL))
      {
	 goto next2; /* Just close attr_fd. */
      }
      /* ...and finally read it. */
      if(read(attr_fd, ex_at, stat_info.st_size) < 0)
      {
	 goto next1; /* Close attr_fd and free ex_at. */
      }

      /* Add the item to the dictionary. */
      PyDict_SetItemString(extended_attribute_dict, dir->dname,
			   PyString_FromString(ex_at));

      /* cleanup */
next1:
      free(ex_at);
next2:
      close(attr_fd);
   }

   closedir(dir_fp);  /* more cleanup */
   
   return extended_attribute_dict;
}

PyObject * extendedAttributesPut(char *path, char *name, void *contents,
				 int length)
{
   int rtn;
   int remember_errno;
   
   /* Open the extended attribute "directory". */
   if((fd = attropen(path, ".", O_RDONLY)) < 0)
   {
      return raise_exception("Failed to obtain extened attribute list");
   }

   /* Convert it to a stream. */
   dir_fp = fopendir(fd);

   /* Open the extended attribute file. */
   if((attr_fd = openat(fd, name, O_RDONLY | O_XATTR)) < -1)
   {
      /* If we get here, the most likely reason is a permission error,
       * though it could be anything that could cause reading a file to
       * fail.  If we aren't meant to see it, don't worry about it
       * and move onto the next one. */
      continue;
   }

   /* Write the contents of the extended attribute file. */
   rtn = write(attr_fd, contents, length);
   remember_errno = errno;
   
   /* cleanup */
   close(attr_fd);
   closedir(dir_fp);

   /* Handle any errors. */
   if(rtn < -1)
   {
      /* In case close() or closedir() set errno themselves. */
      errno = remember_errno;
      
      return raise_exception("Failed to write extended attribute");
   }

   return Py_RETURN_NONE;
}
#elif defined ( __bsdi__ )
/* For FreeBSD the functions that need to be looked at are called:
 * extattr_get_file()
 * extattr_set_file()
 * extattr_delete_file()
 * extattr_list_file() */
#endif /* defined(__linux__) or defined(__APPLE__) */
