#include <Python.h>

#include "ETape.h"	/* get ET_descriptor typedef definition */

#define CKALLOC(malloc_call) if ( !(malloc_call) ) {PyErr_NoMemory(); return NULL;} 

/*
 An error reporter which produces an error string and raises an exception for python
 all errors throw an ETape.error exception
*/
static PyObject *ETErrObject;
#ifdef HAVE_STDARG_PROTOTYPES
static PyObject *
raise_ftt_exception(char * location,  ET_descriptor *ET_desc, ...)
#else
static PyObject *
raise_ftt_exception(location, ET_desc, va_alist)
	char *location;
	ET_descriptor *ET_desc;
        va_dcl;
#endif
{
  char errbuf[500];
  int sts;
  int errnum;
  /*  dealloc and raise exception  */
  sprintf(errbuf,"Error at %s - FTT reports: %s\n", location, ftt_get_error(&errnum));
  printf("%s errno=%d\n",errbuf,errnum);
  PyErr_SetString(ETErrObject,errbuf);
  /*
    Could be "freezing tape in drive," so do not rewind or unload
  */
  sts=ftt_close(ET_desc->ftt_desc);
  if (sts <0)
     printf("error in ftt exceptio\n");
  /*
    Free the memory we allocated   N.B. WE MUST ALWAYS HAVE OPENED A FTT
  */
  free(ET_desc->buffer);
  free(ET_desc);

  return NULL;
}

/*
  Module description
*/
static char ETape_Doc[] =  "ETape is a module which interfaces to ENSTORE TAPE drives";

/*
   Method implementations
*/

/* = = = = = = = = = = = = = = -  ET_OpenRead  = = = = = = = = = = = = = = - */
/*
   Returns an ETape desciptor - atually a python long used as a pointer to a ETape structure.
   p1 - device name
   p2 - loc - the file number we should position to
   p3 - position - the number of FM to move to reach loc - may be negative
   p4 - the block size - used to allocate a read buffer.  May be larger that actual 
          block size but should not be smaller.

 */

static char ET_OpenRead_Doc[] = "Open a tape drive for reading";

static PyObject* ET_OpenRead(PyObject *self, PyObject *args)
{
  char *fname;
  int position;
  int loc;
  int sts;
  ET_descriptor *ET_desc;
/*
	Allocate an ETApe desciptor block
*/
  CKALLOC( ET_desc = (ET_descriptor*)malloc (sizeof(ET_descriptor) ) ); 
/*
	Parse the arguments 
*/
  PyArg_ParseTuple(args, "siii", &fname, &position, &loc, &ET_desc->block_size);
/*
	Allocate a read buffer
*/
  CKALLOC( ET_desc->buffer = malloc( ET_desc->block_size ) );

  ET_desc->hadeof = 0;
  ET_desc->bytes_xferred = 0;
/*
	Open the FTT way
*/

  ET_desc->ftt_desc = ftt_open(fname, FTT_RDONLY);
  sts = ftt_open_dev(ET_desc->ftt_desc);
  if (!sts) 
    return raise_ftt_exception("ET_OpenRead_opendev", ET_desc, "%s", fname);
/*
	Position to the file, if backwards then skip back forward to BOF
*/
  if ((loc == 0) && (position != 0))
  {
    sts = ftt_rewind(ET_desc->ftt_desc);
    if (sts<0)
    {
        return raise_ftt_exception("ET_OpenRead_rew", ET_desc, "%s", fname);
    }
  } else {
    if (position != 0) 
    {
      sts = ftt_skip_fm(ET_desc->ftt_desc, position);
      if (sts<0)
        return raise_ftt_exception("ET_OpenRead_skipfm", ET_desc, "%s", fname);
    }
    if (position < 0)
    {
      sts = ftt_skip_fm(ET_desc->ftt_desc, 1);
      if (sts<0)
        return raise_ftt_exception("ET_OpenRead_NegForward", ET_desc, "%s", fname);
    }
  }
  
  return Py_BuildValue("l",(long int)ET_desc);}
/* = = = = = = = = = = = = = = -  ET_ReadBlock  = = = = = = = = = = = = = = - */

static char ET_ReadBlock_Doc[] = "Read a block from tape";
/*
   Returns a python string containing the data - string length 0 implies eof
   p1 - ETape descriptor returned by open read
 */
static PyObject* ET_ReadBlock(PyObject *self, PyObject *args)
{
  ET_descriptor *ET_desc;
  int len;
/*
	Get the arguments
*/
  PyArg_ParseTuple(args, "l|i", (long int *)&ET_desc);
  if (ET_desc->hadeof)    /* this is not ftt exception fix */
    return raise_ftt_exception("ET_ReadBlock_ReadAfterEof", ET_desc);
  len=ftt_read(ET_desc->ftt_desc, ET_desc->buffer, ET_desc->block_size);
  if (len < 0)
     return raise_ftt_exception("ET_ReadBlock", ET_desc);
  if (len == 0)
     ET_desc->hadeof = 1;
  ET_desc->bytes_xferred += len;
  return Py_BuildValue("s#", ET_desc->buffer, len);
}
/* = = = = = = = = = = = = = = -  ET_CloseRead  = = = = = = = = = = = = = = - */

static char ET_CloseRead_Doc[] = "Close an input tape";
/*
   Returns a python list (bytes remaining on tape, #of bytes read, #of tape erros (as defined by drive)
   p1 - ETape descriptor returned by open read
 */

static PyObject* ET_CloseRead(PyObject *self, PyObject *args)
{
  ET_descriptor *ET_desc;
  ftt_stat_buf   stbuff;
  int sts;
  PyObject *ErrDict;
  char *c1,*c2,*c3;
/*
	Get the arguements
*/
  PyArg_ParseTuple(args, "l", (long int *)&ET_desc);
/*
	Get the tape stats from FTT
*/
  stbuff = ftt_alloc_stat();
  sts=ftt_get_stats(ET_desc->ftt_desc, stbuff);
  if (sts <0)
    return raise_ftt_exception("ET_CloseRead_stats", ET_desc);
  c1= ftt_extract_stats(stbuff,FTT_REMAIN_TAPE);
  if (c1 == NULL) c1 = "Invalid";
  c2= ftt_extract_stats(stbuff,FTT_N_READS);
  if (c2 == NULL) c2 = "Invalid";
  c3= ftt_extract_stats(stbuff,FTT_READ_ERRORS);
  if (c3 == NULL) c3 = "Invalid";
  ErrDict = Py_BuildValue ("[s,s,s,i]", c1,c2,c3, ET_desc->bytes_xferred);
  sts=ftt_free_stat(stbuff);
/*
	Close the ftt file
*/
  sts=ftt_close(ET_desc->ftt_desc);
  if (sts <0)
    return raise_ftt_exception("ET_CloseRead", ET_desc);
/*
	Free the memory we allocated
*/
  free(ET_desc->buffer);
  free(ET_desc);
  
  return ErrDict;
}
/* = = = = = = = = = = = = = = -  ET_OpenWrite  = = = = = = = = = = = = = = - */

static char ET_OpenWrite_Doc[] = "Open a tape drive for writing";

/*
   Returns an ETape desciptor
   p1 - device name
   p2 - # of file marks to move before opening
   p3 - block size - writes are accumualted in buffer.
 */
static PyObject*
ET_OpenWrite(PyObject *self, PyObject *args)
{
  char *fname;
  ET_descriptor *ET_desc; 
  int sts;
  int eod;
/*
	Get the arguements
*/
  CKALLOC( ET_desc = (ET_descriptor*)malloc (sizeof(ET_descriptor)) );
  PyArg_ParseTuple(args, "sii", &fname, &eod, &ET_desc->block_size);
/*
	Allocate a write buffer
*/
  CKALLOC( ET_desc->buffer = malloc( ET_desc->block_size ) );
  ET_desc->bufptr =  ET_desc->buffer;
  ET_desc->bytes_xferred =0;
/*
	open the ftt file
*/
  printf("DEBUG open write %s\n",fname);

  ET_desc->ftt_desc = ftt_open(fname, FTT_RDWR);
  sts = ftt_open_dev(ET_desc->ftt_desc);
  if (!sts)
    return raise_ftt_exception("ET_OpenWrite", ET_desc, "%s", fname);
  if (eod != 0)
  {
    sts = ftt_skip_fm(ET_desc->ftt_desc, eod);
    if (sts<0)
      return raise_ftt_exception("ET_OpenWrite_skipfm", ET_desc, "%s", fname);
  }
/*
	Return The ETape descriptor
*/
  return Py_BuildValue("l",(long)ET_desc);
}
/* = = = = = = = = = = = = = = -  ET_WriteBlock  = = = = = = = = = = = = = = - */
/*
   Returns void
   p1 - ETape desciptor returned by openread
   p2 - the data to write  
   ET_WriteBlock does not really write a block.  It copies the
   the data passed in the agruement to a buffer and if the buffer is full
   it writes a block.  It accepts any size input 0 and lengths grater than a 
   tape block
*/
static char ET_WriteBlock_Doc[] = "Write a block to tape";

static PyObject * ET_WriteBlock(PyObject *self, PyObject *args)
{
  ET_descriptor *ET_desc;
  int sts;
  char *data_buff;
  int length;
  int partlen;

  PyArg_ParseTuple(args, "ls#", &ET_desc, &data_buff, &length);
  ET_desc->bytes_xferred += length;
  /*printf("DEBUG write %d\n",length);*/
  while (length > 0)
  {
    if (ET_desc->bufptr + length < ET_desc->buffer + ET_desc->block_size) 
    {
      memcpy(ET_desc->bufptr, data_buff, length);
      ET_desc->bufptr += length;
      break;
    } else {
      partlen = ET_desc->buffer + ET_desc->block_size - ET_desc->bufptr;
      memcpy(ET_desc->bufptr, data_buff, partlen);
      
      sts=ftt_write(ET_desc->ftt_desc,  ET_desc->buffer, ET_desc->block_size);
      if (sts != ET_desc->block_size)
         return raise_ftt_exception("ET_WriteBlock", ET_desc);
      ET_desc->bufptr = ET_desc->buffer;
      length -= partlen;
      data_buff += partlen;
    }
  }
  return Py_BuildValue("i",0);
}
/* = = = = = = = = = = = = = = -  ET_CloseWrite  = = = = = = = = = = = = = = - */

static char ET_CloseWrite_Doc[] = "Close an output tape";
/*
    Returns python list - bytes remianing on tape, number of bytes written, number of write errors
    p1 - ETape desciptor
 */
static PyObject* ET_CloseWrite(PyObject *self, PyObject *args)
{
  ET_descriptor *ET_desc;
  ftt_stat_buf   stbuff;
  int sts;
  PyObject *ErrDict;
  int partlen;
  char *c1,*c2,*c3;
/*
        Parse the arguments
*/
  PyArg_ParseTuple(args, "l", (long int *)&ET_desc);
/*
	Write unwritten buffer
*/
  printf("DEBUG close \n");
  partlen = ET_desc->bufptr - ET_desc->buffer;
  if (partlen > 0)
  {
    sts=ftt_write(ET_desc->ftt_desc,  ET_desc->buffer, partlen);
    if (sts != partlen)
        return raise_ftt_exception("ET_CloseWrite_Block", ET_desc);
  }
/*
	Get the tape stats
*/
  sts=ftt_write2fm(ET_desc->ftt_desc);
  if (sts <0 )
      return raise_ftt_exception("ET_CloseWrite_FM", ET_desc);
  sts=ftt_skip_fm(ET_desc->ftt_desc,-1);
  if (sts <0 )
      return raise_ftt_exception("ET_CloseWrite_MB", ET_desc);

  stbuff = ftt_alloc_stat();
  /*printf( "ronDBG - setting ftt_debug=4\n" );ftt_debug = 4;*/
  sts=ftt_get_stats(ET_desc->ftt_desc, stbuff);
  if (sts < 0)
    return raise_ftt_exception("ET_ClosePartial", ET_desc);
  c1 = ftt_extract_stats(stbuff,FTT_REMAIN_TAPE);
  if (c1 == NULL) c1 = "Invalid";
  c2 = ftt_extract_stats(stbuff,FTT_N_WRITES);
  if (c2 == NULL) c2 = "Invalid";
  c3 = ftt_extract_stats(stbuff,FTT_WRITE_ERRORS);
  if (c3 == NULL) c3 = "Invalid";

  ErrDict = Py_BuildValue ("[s,s,s,i]", c1,c2,c3, ET_desc->bytes_xferred);
/*
	Close the drive
*/
  sts=ftt_close(ET_desc->ftt_desc);
  if (sts < 0)
    return raise_ftt_exception("ET_CloseWrite", ET_desc);
/*
	Free the memory
*/
  free(ET_desc->buffer);
  free(ET_desc); 

  return ErrDict;
}

/* = = = = = = = = = = = = = = -  ET_Rewind  = = = = = = = = = = = = = = - */

static char ET_Rewind_Doc[] = "Rewind a drive";

static PyObject* ET_Rewind(PyObject *self, PyObject *args)
{
  char *cartid;
  char *fname;
  ET_descriptor *ET_desc;

  int sts;

  PyArg_ParseTuple(args, "ss", &cartid, &fname);
/*
        Allocate an ETApe desciptor block
*/
  CKALLOC( ET_desc = (ET_descriptor*)malloc (sizeof(ET_descriptor) ) );
  ET_desc->ftt_desc = ftt_open(fname, FTT_RDONLY);
  sts = ftt_rewind(ET_desc->ftt_desc);
  if (sts < 0)
    return raise_ftt_exception("ET_Rewind", ET_desc, "%s", fname);

/*
        Close the drive
*/
  sts=ftt_close(ET_desc->ftt_desc);
  if (sts < 0)
    return raise_ftt_exception("ET_RewindEnd", ET_desc);
/*
        Free the memory
*/
  free(ET_desc);
  return Py_BuildValue("i",0);
}

/* = = = = = = = = = = = = = = -  Python Module Definitions = = = = = = = = = = = = = = - */

/*
   Module Methods table. 

   There is one entry with four items for for each method in the module

   Entry 1 - the method name as used  in python
         2 - the c implementation function
         3 - flags 
         4 - method documentation string
*/
static PyMethodDef ETape_Methods[] = {
  { "ET_OpenWrite",  ET_OpenWrite,  1, ET_OpenWrite_Doc},
  { "ET_OpenRead",   ET_OpenRead,   1, ET_OpenRead_Doc},
  { "ET_WriteBlock", ET_WriteBlock, 1, ET_WriteBlock_Doc},
  { "ET_ReadBlock",  ET_ReadBlock,  1, ET_ReadBlock_Doc},
  { "ET_CloseRead",  ET_CloseRead,  1, ET_CloseRead_Doc},
  { "ET_CloseWrite", ET_CloseWrite, 1, ET_CloseWrite_Doc},
  { "ET_Rewind", ET_Rewind, 1, ET_Rewind_Doc},
  { 0, 0}        /* Sentinel */
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
  PyObject *m, *d;
  m= Py_InitModule4("ETape", ETape_Methods, ETape_Doc, 
                               (PyObject*)NULL,PYTHON_API_VERSION);
  d = PyModule_GetDict(m);
  ETErrObject = PyErr_NewException("ETape.error", NULL, NULL);
  if (ETErrObject != NULL)
             PyDict_SetItemString(d,"error",ETErrObject);
}

