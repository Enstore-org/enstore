#include <Python.h>
#include <ftt.h>

/*
  Module description
*/
static char ETape_Doc[] =  "ETape is a module which interfaces to ENSTORE TAPE drives";


/*
   Method implementations
*/
static char ET_CopyToTape_Doc[] = "Copy data from an fd to tape";

static PyObject*
ET_CopyToTape(PyObject *self, PyObject *args)
{
  ftt_descriptor ftt_desc;
  char *buff;
  int length;
  int sts;

  if (!PyArg_ParseTuple(args, "ls#", (long int *)&ftt_desc, &buff, &length))
      return NULL;
  sts=ftt_write(ftt_desc, buff, length);
  return Py_BuildValue("i",0);
}

static char ET_CloseTape_Doc[] = "Close a tape";

static PyObject*
ET_CloseTape(PyObject *self, PyObject *args)
{
  ftt_descriptor ftt_desc;
  ftt_stat_buf   stbuff;
  int sts;
  PyObject *ErrDict;

  if (!PyArg_ParseTuple(args, "l", (long int *)&ftt_desc))
      return NULL;
  stbuff = ftt_alloc_stat();
  sts=ftt_get_stats(ftt_desc, stbuff);

  ErrDict = Py_BuildValue ("{s:s,s:s,s:s}", 
    "Remain", ftt_extract_stats(stbuff,FTT_REMAIN_TAPE),
    "Nwrite", ftt_extract_stats(stbuff,FTT_N_WRITES),
    "Werrors", ftt_extract_stats(stbuff,FTT_WRITE_ERRORS)
    );
  sts=ftt_free_stat(stbuff);
  sts=ftt_close(ftt_desc);
  return ErrDict;
}

static char ET_OpenRead_Doc[] = "Open a tape drive for reading";

static PyObject*
ET_OpenRead(PyObject *self, PyObject *args)
{
  char *iname;
  ftt_descriptor ftt_desc;

  if (!PyArg_ParseTuple(args, "s", &iname))
      return NULL;
    
  ftt_desc = ftt_open(iname, FTT_RDONLY);

  
  return Py_BuildValue("i",0);
}

static char ET_OpenWrite_Doc[] = "Open a tape drive for writing";

static PyObject*
ET_OpenWrite(PyObject *self, PyObject *args)
{
  char *oname;
  ftt_descriptor ftt_desc;

  if (!PyArg_ParseTuple(args, "s", &oname))
      return NULL;
  ftt_desc = ftt_open(oname, FTT_RDWR);
  return Py_BuildValue("l",(long int)ftt_desc);

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
  { "OpenWrite", ET_OpenWrite, 1, ET_OpenWrite_Doc},
  { "OpenRead", ET_OpenRead, 1, ET_OpenRead_Doc},
  { "CopyToTape", ET_CopyToTape, 1, ET_CopyToTape_Doc},
  { "CloseTape", ET_CloseTape, 1, ET_CloseTape_Doc},
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
  (void) Py_InitModule4("ETape", ETape_Methods, ETape_Doc, 
                               (PyObject*)NULL,PYTHON_API_VERSION);

}
