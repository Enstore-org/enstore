#include <Python.h>

/*
  Module description
*/
static char ETape_Doc[] =  "ETape is a module which interfaces to ENSTORE TAPE drives";

/*
  CopyToTape Method description
*/
static char ET_CopyToTape_Doc[] = "Copy data from an fd to tape";

/*
   Method implementation
*/
static PyObject*
ET_CopyToTape(PyObject *self, PyObject *args)
{
  PyObject *ifob;
  char *iname, *ipathname;

  PyObject *ofob;
  char *oname, *opathname;
  int sts = 0;

  if (!PyArg_ParseTuple(args, "ss|O!", &iname, &ipathname,
                              &PyFile_Type, &ifob))
      return NULL;
  if (!PyArg_ParseTuple(args+1, "ss|O!", &oname, &opathname,
                              &PyFile_Type, &ofob))
  return Py_BuildValue("i",sts);

}

/*
   Module Methods table. 

   There is one entry with four items for for each method in the module

   Entry 1 - the method name as used  in python
         2 - the c implementation function
         3 - flags 
         4 - method documentation string
*/
static PyMethodDef ETape_Methods[] = {
  { "CopyToTape", ET_CopyToTape, 1, ET_CopyToTape_Doc},
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

   Third	- a doumentation string for the module
  
   Fourth & Fifth - see Python/modsupport.c

*/
void initETape()
{
  PyObject *m = Py_InitModule4("ETape", ETape_Methods, ETape_Doc, 
                               (PyObject*)NULL,PYTHON_API_VERSION);

}
