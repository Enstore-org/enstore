/*  This file (Trace.c) was created by Ron Rechenmacher <ron@fnal.gov> on
    Jul 27, 1998. "TERMS AND CONDITIONS" governing this file are in the README
    or COPYING file. If you do not have such a file, one can be obtained by
    contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
    $RCSfile$
    $Revision$
    $Date$
    */

#include <Python.h>		/* all the Py.... stuff */
#include <assert.h>             /* assert */

#include "trace.h"		/* trace... */


/*
 *  Module description:
 *      Two methods:
 *	    1)  (dat_byts,dat_CRC,san_CRC) = obj.to_HSM(  frmDriverObj, to_DriverObj
 *						         , crc_fun, sanity_byts, header )
 *	    2)  (dataCRC,sanitySts) = obj.frmHSM(  frmDriverObj, to_DriverObj
 *						 , dataSz, sanitySz, sanityCRC )
 */
static char Trace_Doc[] = "Trace is a module which interfaces to a global trace buffer";


/******************************************************************************
 * @+Public+@
 * ROUTINE: raise_exception:  Added by ron on 01-May-1998
 *
 * DESCRIPTION:		An error reporter which produces an error string and
 *			 raises an exception for python.
 *
 ******************************************************************************/

static PyObject *TraceErrObject;

static PyObject *
#ifdef HAVE_STDARG_PROTOTYPES
raise_exception(  char		*method_name
		, ... )
#else
raise_exception( method_name, ETdesc, va_alist )
     char		*method_name;
     va_dcl;
#endif
{							/* @-Public-@ */
	char	errbuf[500];

    /*  dealloc and raise exception fix */
    sprintf(  errbuf, "Error in %s\n", method_name );
    PyErr_SetString( TraceErrObject, errbuf );
    return (NULL);
}   /* raise_exception */


/******************************************************************************
 * @+Public+@
 * ROUTINE: init_function:  Added by ron on 01-May-1998
 *
 * DESCRIPTION:		write to trace buffer
 *
 ******************************************************************************/

static char init_function_Doc[] = "write to trace_buffer";

static PyObject *
init_function(  PyObject	*self
	     , PyObject	*args )
{							/* @-Public-@ */
	char		*name;


    /*  Parse the arguments */
    PyArg_ParseTuple(  args, "s", &name );
    trace_init( name );
    return (Py_BuildValue(""));
}   /* init_function */


/******************************************************************************
 * @+Public+@
 * ROUTINE: trace_function:  Added by ron on 01-May-1998
 *
 * DESCRIPTION:		write to trace buffer
 *
 ******************************************************************************/

static char trace_function_Doc[] = "write to trace_buffer";

static PyObject *
trace_function(  PyObject	*self
	       , PyObject	*args )
{							/* @-Public-@ */
	int		ii, sts;
	int		lvl;
	char		*msg;
	PyObject	*obj;
	int		tuple_size;
	char		fmt[2+TRC_MAX_PARAMS+1];
	int		push[TRC_MAX_PARAMS*2]; /* area for converted data */
	void		*pp_arg[TRC_MAX_PARAMS];
	void		*pp;


    fmt[0] = 'i';
    fmt[1] = 's';
    pp = push;
    tuple_size = PyTuple_Size(args);
    for (ii=2; ii<tuple_size && (ii<(TRC_MAX_PARAMS+2)); ii++)
    {   obj = PyTuple_GetItem( args, ii );
	if (PyInt_Check(obj))
	{   fmt[ii] = 'i';
	    pp_arg[ii-2] = (int *)pp;
	    pp = (int *)pp + 1;
	}
	else
	{   fmt[ii] = 'd';
	    /* floats are pushed as double -- must use double! */
	    pp_arg[ii-2] = (double *)pp;
	    pp = (double *)pp + 1;
	}
    }
    fmt[ii] = '\0';

    /* IICH -- hard coded numbers, but what else can I do? */
    sts = PyArg_ParseTuple(  args, fmt, &lvl, &msg
			   , pp_arg[0], pp_arg[1], pp_arg[2]
			   , pp_arg[3], pp_arg[4], pp_arg[5] );
    if (!sts)
	printf( "PyArg_ParseTuple error\n" );
    trace_(  lvl, msg
	  , push[0], push[1], push[2], push[3], push[4], push[5] );
    return (Py_BuildValue(""));
}   /* trace_function */


/******************************************************************************
 * @+Public+@
 * ROUTINE: mode_function:  Added by ron on 01-May-1998
 *
 * DESCRIPTION:		write to trace buffer
 *
 ******************************************************************************/

static char mode_function_Doc[] = "get (and optionally set) the trace mode";

static PyObject *
mode_function(  PyObject	*self
	       , PyObject	*args )
{							/* @-Public-@ */
	int		sts, mode;

    /* initialize optional arg */
    mode = trc_cntl_sp->mode;
    sts = PyArg_ParseTuple(  args, "|i", &mode );
    if (!sts)
	printf( "PyArg_ParseTuple error\n" );

    sts = trc_cntl_sp->mode;
    trc_cntl_sp->mode = mode;

    return (Py_BuildValue("i",sts));
}   /* mode_function */


/* = = = = = = = = = = = = = = -  Python Module Definitions = = = = = = = = = = = = = = - */

/*  Module Methods table. 

    There is one entry with four items for for each method in the module

    Entry 1 - the method name as used  in python
          2 - the c implementation function
	  3 - flags 
	  4 - method documentation string
	  */

static PyMethodDef Trace_Functions[] = {
    { "init",  init_function,  1, init_function_Doc},
    { "trace",  trace_function,  1, trace_function_Doc},
    { "mode",  mode_function,  1, mode_function_Doc},
    { 0, 0}        /* Sentinel */
};

/******************************************************************************
    Module initialization.   Python call the entry point init<module name>
    when the module is imported.  This should the only non-static entry point
    so it is exported to the linker.

    The Py_InitModule4 is not in the python 1.5 documentation but is copied
    from the oracle module.  It extends Py_InitModule with documentation
    and seems useful.

    First argument must be a the module name string.
    
    Second       - a list of the module methods

    Third	- a doumentation string for the module
  
    Fourth & Fifth - see Python/modsupport.c
    */

void
initTrace()
{
	PyObject	*m, *d;

    m = Py_InitModule4(  "Trace", Trace_Functions, Trace_Doc
		       , (PyObject*)NULL, PYTHON_API_VERSION );
    d = PyModule_GetDict(m);
    TraceErrObject = PyErr_NewException("Trace.error", NULL, NULL);
    if (TraceErrObject != NULL)
	PyDict_SetItemString(d,"error",TraceErrObject);
    trace_init( "python trace" );
}

