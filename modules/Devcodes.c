#include <Python.h>

static char Devcodes_Doc[] =  "Get major, minor device codes";

/*
   Method implementations
*/

static char GetCodes_Doc[] = "Get major, minor device codes";

static PyObject*
GetCodes(PyObject *self, PyObject *args)
{
char *filename;
/*
	Get the arguements
*/
  PyArg_ParseTuple(args, "s", filename);
/*
	Get the tape stats from FTT
*/
  return( Py_BuildValue ("{s:i,s:i}", 
    "Major",3 ,
    "Minor", 3
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

   Third	- a doumentation string for the module
  
   Fourth & Fifth - see Python/modsupport.c

*/
void initDevcodes()
{
  (void) Py_InitModule4("Devcodes", Devcodes_Methods, Devcodes_Doc, 
                               (PyObject*)NULL,PYTHON_API_VERSION);

}
