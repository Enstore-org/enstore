/*  This file (Ptrace.c) was created by Ron Rechenmacher <ron@fnal.gov> on
    Jul 27, 1998. "TERMS AND CONDITIONS" governing this file are in the README
    or COPYING file. If you do not have such a file, one can be obtained by
    contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
    $RCSfile$
    $Revision$
    $Date$
    */

#include <Python.h>		/* all the Py.... stuff */
#include <assert.h>             /* assert */
#include <stdlib.h>		/* getenv */

#include "trace.h"		/* trace... */


/*
 *  Module description:
 *      Two methods:
 *	    1)  (dat_byts,dat_CRC,san_CRC) = obj.to_HSM(  frmDriverObj, to_DriverObj
 *						         , crc_fun, sanity_byts, header )
 *	    2)  (dataCRC,sanitySts) = obj.frmHSM(  frmDriverObj, to_DriverObj
 *						 , dataSz, sanitySz, sanityCRC )
 */
static char Ptrace_Doc[] = "Ptrace is a module which interfaces to a global trace buffer";


/******************************************************************************
 * @+Public+@
 * ROUTINE: raise_exception:  Added by ron on 01-May-1998
 *
 * DESCRIPTION:		An error reporter which produces an error string and
 *			 raises an exception for python.
 *
 ******************************************************************************/

static PyObject *PtraceErrObject;

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
    PyErr_SetString( PtraceErrObject, errbuf );
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
	char		*keyfile="";


    /*  Parse the arguments */
    PyArg_ParseTuple(  args, "s|s", &name, &keyfile );
    trace_init( name, keyfile );
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


static int
OnOff( int on, char *id_s, unsigned lvl1, unsigned lvl2 )
{
	unsigned	id_i, new_msk=0;
	char		*end_p;
	unsigned	old_lvl;

    if (lvl1 > 31) lvl1 = 31;
    if (lvl2 > 31) lvl2 = 31;

    if (lvl1 > lvl2) new_msk = (1<<lvl1) | (1<<lvl2);
    else for (; (lvl1<=lvl2); lvl1++) new_msk |= (1<<lvl1);

    id_i = strtol(id_s,&end_p,10);
    if (end_p != (id_s+strlen(id_s)))	/* check if conversion worked */
    {   /* did not work - id_s must not have a pure number -
	   check for name */
	int	i;

	/* first check special case */
	if (  (strcmp(id_s,"global")==0)
	    ||(strcmp(id_s,"Global")==0)
	    ||(strcmp(id_s,"GLOBAL")==0))
	{
	    for (id_i=(TRC_MAX_PIDS+TRC_MAX_PROCS); id_i--; )
	    {   old_lvl = trc_cntl_sp->lvl_a[id_i];
		if (on)
		    trc_cntl_sp->lvl_a[id_i] |=  new_msk;
		else
		{   trc_cntl_sp->lvl_a[id_i] &= ~new_msk;
		}
	    }
	    return (0);
	}
	if (  (strcmp(id_s,"initial")==0)
	    ||(strcmp(id_s,"Initial")==0)
	    ||(strcmp(id_s,"INITIAL")==0))
	{   old_lvl = trc_cntl_sp->intl_lvl;
	    if (on)
		trc_cntl_sp->intl_lvl |=  new_msk;
	    else
	    {   trc_cntl_sp->intl_lvl &= ~new_msk;
	    }
	    return (old_lvl);
	}
	if (  (strcmp(id_s,"tty")==0)
	    ||(strcmp(id_s,"Tty")==0)
	    ||(strcmp(id_s,"TTy")==0))
	{   old_lvl = trc_cntl_sp->tty_lvl;
	    if (on)
		trc_cntl_sp->tty_lvl |=  new_msk;
	    else
	    {   trc_cntl_sp->tty_lvl &= ~new_msk;
	    }
	    return (old_lvl);
	}

	for (i=TRC_MAX_PROCS; i--; )
	{
	    if (strcmp(trc_cntl_sp->t_name_a[i],id_s) == 0)
		break;
	}
	if (i == -1)
	    return (1);
	id_i = i + TRC_MAX_PIDS;
    }

    /* at this point, either id_s was a number or it was a name that was
       converted to a number */

    if (id_i > (TRC_MAX_PIDS+TRC_MAX_PROCS-1))
	id_i = (TRC_MAX_PIDS+TRC_MAX_PROCS-1);

    old_lvl = trc_cntl_sp->lvl_a[id_i];
    if (on)
	trc_cntl_sp->lvl_a[id_i] |=  new_msk;
    else
	trc_cntl_sp->lvl_a[id_i] &= ~new_msk;

    return (old_lvl);
}

/******************************************************************************
 * @+Public+@
 * ROUTINE: on_function:  Added by ron on 01-May-1998
 *
 * DESCRIPTION:		write to trace buffer
 *
 ******************************************************************************/

static char on_function_Doc[] = "turn trace levels on";

static PyObject *
on_function(  PyObject	*self
	    , PyObject	*args )
{							/* @-Public-@ */
	int		sts;
	char		*id_s;
	unsigned	lvl1, lvl2;

    sts = PyArg_ParseTuple(  args, "sii", &id_s, &lvl1, &lvl2 );
    if (!sts)
	printf( "PyArg_ParseTuple error\n" );

    return (Py_BuildValue("i",OnOff(1,id_s,lvl1,lvl2)));
}   /* on_function */


/******************************************************************************
 * @+Public+@
 * ROUTINE: off_function:  Added by ron on 01-May-1998
 *
 * DESCRIPTION:		write to trace buffer
 *
 ******************************************************************************/

static char off_function_Doc[] = "turn trace levels off";

static PyObject *
off_function(  PyObject	*self
	    , PyObject	*args )
{							/* @-Public-@ */
	int		sts;
	char		*id_s;
	unsigned	lvl1, lvl2;

    sts = PyArg_ParseTuple(  args, "sii", &id_s, &lvl1, &lvl2 );
    if (!sts)
	printf( "PyArg_ParseTuple error\n" );

    return (Py_BuildValue("i",OnOff(0,id_s,lvl1,lvl2)));
}   /* off_function */


/* = = = = = = = = = = = = = = -  Python Module Definitions = = = = = = = = = = = = = = - */

/*  Module Methods table. 

    There is one entry with four items for for each method in the module

    Entry 1 - the method name as used  in python
          2 - the c implementation function
	  3 - flags 
	  4 - method documentation string
	  */

static PyMethodDef Ptrace_Functions[] = {
    { "init",  init_function,  1, init_function_Doc},
    { "trace",  trace_function,  1, trace_function_Doc},
    { "mode",  mode_function,  1, mode_function_Doc},
    { "on",  on_function,  1, on_function_Doc},
    { "off",  off_function,  1, off_function_Doc},
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
initPtrace()
{
	PyObject	*m, *d;

    m = Py_InitModule4(  "Ptrace", Ptrace_Functions, Ptrace_Doc
		       , (PyObject*)NULL, PYTHON_API_VERSION );
    d = PyModule_GetDict(m);
    PtraceErrObject = PyErr_NewException("Ptrace.error", NULL, NULL);
    if (PtraceErrObject != NULL)
	PyDict_SetItemString(d,"error",PtraceErrObject);

    trace_init( "python", "" );
}

