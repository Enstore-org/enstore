#include <Python.h>

#ifdef STDC_HEADERS
#include <stdlib.h>
#endif

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

/* Since major is a function on SVR4, we can't use `ifndef major'.  */
#ifdef MAJOR_IN_MKDEV
#include <sys/mkdev.h>
#define HAVE_MAJOR
#endif

#ifdef MAJOR_IN_SYSMACROS
#include <sys/sysmacros.h>
#define HAVE_MAJOR
#endif

#ifdef major                    /* Might be defined in sys/types.h.  */
#define HAVE_MAJOR
#endif

#ifndef HAVE_MAJOR
#define major(dev) (((dev) >> 8) & 0xff)
#define minor(dev) ((dev) & 0xff)
#endif
#undef HAVE_MAJOR

#include <sys/stat.h>


static char Devcodes_Doc[] =  "Get major and  minor device codes";

/*
   Method implementations
*/

static char GetCodes_Doc[] = "Get major and minor device codes";

static PyObject*
GetCodes(PyObject *self, PyObject *args)
{
  char *filename;
  int istatus;
  int dmajor;
  int dminor;
  struct stat statbuf;

/*
        Get the arguements
*/
  PyArg_ParseTuple(args, "s", &filename);
/*
        Get the tape stats from FTT
*/
  istatus = stat(filename, &statbuf);
  dmajor = major(statbuf.st_dev);
  dminor = minor(statbuf.st_dev);

  return( Py_BuildValue ("{s:i,s:i}",
                         "Major", dmajor,
                         "Minor", dminor
                         ) );
}

/*
   Module Methods table.

   There is one entry with four items for for each method in the module

   Entry 1 - the method name as used  in python
         2 - the c implementation function
         3 - flags
         4 - method documentation string
*/
static PyMethodDef Devcodes_Methods[] = {
  { "GetCodes", GetCodes, 1, GetCodes_Doc},
  {0,     0}        /* Sentinel */
};

/*
   Module initialization.   Python call the entry point init<module name>
   when the module is imported.  This should the only non-static entry point
   so it is exported to the linker.

   The Py_InitModule4 is not in the python 1.5 documentation but is copied
   from the oracle module.  It extends Py_InitModule with documentation
   and seems useful.

   First argument must be a the module name string.

   Seond        - a list of the module methods

   Third        - a doumentation string for the module

   Fourth & Fifth - see Python/modsupport.c

*/
void initDevcodes()
{
  (void) Py_InitModule4("Devcodes", Devcodes_Methods, Devcodes_Doc,
                               (PyObject*)NULL,PYTHON_API_VERSION);

}
