/*  This file (Ptrace.c) was created by Ron Rechenmacher <ron@fnal.gov> on
    Jul 27, 1998. "TERMS AND CONDITIONS" governing this file are in the README
    or COPYING file. If you do not have such a file, one can be obtained by
    contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
    $RCSfile$
    $Revision$
    $Date$
    */

#include <Python.h>		/* all the Py.... stuff */
#include <graminit.h>		/* parse-mode flags */
#include <pythonrun.h>		/* PyRun interfaces */
#include <compile.h>		/* needed by frameobject.h */
#include <frameobject.h>	/* PyFrame_Check */
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
static char	Ptrace_Doc[] = "Ptrace is a module which interfaces to a global trace buffer";
static PyObject	*PtraceModuleDict;  /* used to get "trc_fun{1,2}" -- they
				       probably cannot be fetch just once (they
				       probably change when set_func{1,2} is
				       called */
static PyObject *PtraceStrFunc;



/******************************************************************************
 * @+Public+@
 * ROUTINE: raise_exception:  Added by ron on 01-May-1998
 *
 * DESCRIPTION:		An error reporter which produces an error string and
 *			 raises an exception for python.
 *
 ******************************************************************************/

static PyObject *PtraceErrObject;

static char *
PyString_AsString_Safe(PyObject *s){
    if (!s || !PyString_Check(s)){
	return "NULL";
    } else return PyString_AsString(s);
}


static PyObject *
raise_exception(  char		*method_name )
{							/* @-Public-@ */
	char	errbuf[500];

    /*  dealloc and raise exception fix */
    sprintf(  errbuf, "Error in %.485s\n", method_name );
    PyErr_SetString( PtraceErrObject, errbuf );
    return (NULL);
}   /* raise_exception */


void
print_type( PyObject *obj )
{
    printf("%s\n", PyString_AsString(PyObject_Repr(PyObject_Type(obj))));
}

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
    trace_init( name, keyfile, 0, 0 );
    return (Py_BuildValue(""));
}   /* init_function */


/******************************************************************************
 * @+Public+@
 * ROUTINE: trace_function:  Added by ron on 01-May-1998
 *
 * DESCRIPTION:		write to trace buffer
 *
 ******************************************************************************/
void
Ptrace_QPut(  struct timeval	*tp
	    , int		lvl
	    , const char	*msg
	    , ... )
{
	va_list		ap;

    va_start( ap, msg );
    TRACE_QPUT( tp, lvl, msg, ap );
    va_end( ap );
}   /* Ptrace_QPut */

#define TRC_FUNC( func, time, pid, name, args ) \
do\
{       PyObject *func_o;\
    func_o = PyDict_GetItemString( PtraceModuleDict,func );\
    /* _GetItemString does not INCREF */\
    if (func_o)\
    {   PyObject *arg_o=Py_BuildValue("fisO",time,pid,name,args);\
        PyObject *result_o;\
	if (!arg_o) return (raise_exception("trace - can not build arg for " func));\
	result_o = PyEval_CallObject( func_o, arg_o );\
	Py_DECREF( arg_o );\
	if (result_o) Py_DECREF( result_o );\
	else return (raise_exception("trace - " func " error"));\
    }\
    else return (raise_exception("trace - " func " not set"));\
}\
while (0)

int
get_lvl_msg_push(  PyObject *args
		 , int	    *lvl
		 , char     **msg
		 , int	    *push )
{
	int		ii, sts;
	char		fmt[2+TRC_MAX_PARAMS+1];
	void		*pp_arg[TRC_MAX_PARAMS];
	void		*pp;
	int		tuple_size;
	PyObject	*obj;

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
#       if 0 /* seems to be broken in v1_5_1 */
	else if (PyFloat_Check(obj))
	{   fmt[ii] = 'd'; /* d IS for double and f IS for float */
	    /* floats are pushed as double -- must use double! */
	    pp_arg[ii-2] = (double *)pp;
	    pp = (double *)pp + 1;
	}
#       endif
	else
	{   fmt[ii] = 'O';
	    pp_arg[ii-2] = (PyObject *)pp;
	    pp = (PyObject *)pp + 1;
	    ii++; break;  /* finish up */
	}
    }
    fmt[ii] = '\0';
    
    /* IICH -- hard coded numbers, but what else can I do? */
    sts = PyArg_ParseTuple(  args, fmt, lvl, msg
			   , pp_arg[0], pp_arg[1], pp_arg[2]
			   , pp_arg[3], pp_arg[4], pp_arg[5] );
    return (sts);
}   /* get_lvl_msg_push */

static char trace_function_Doc[] = "write to trace_buffer";

static PyObject *
trace_function(  PyObject	*self
	       , PyObject	*args )
{							/* @-Public-@ */
	int		lvl;
	char		*msg;
	int		push[TRC_MAX_PARAMS*2*100]; /* area for converted data */
	struct timeval	tt;
	int		have_time=0;


    if (!get_lvl_msg_push(args,&lvl,&msg,push))
	return (raise_exception("trace - parse error -- max is 8 or possibly 7 args"));

    /* mode is most likely to be on and lvl (for fun1-) is most likely to
       be off, so check lvl first */
    if ((trc_lvl_ip[0]&(1<<lvl)) && (trc_cntl_sp->mode&1))
    {   /* circular queue put operation */
	Ptrace_QPut( &tt, lvl, msg, push[0], push[1], push[2]
		    , push[3], push[4], push[5] );
	have_time = 1;
    }

    if ((trc_lvl_ip[1]&(1<<lvl)) && (trc_cntl_sp->mode&2))
    {   if (!have_time) { gettimeofday( &tt, 0 ); have_time = 1; }
	TRC_FUNC( "func1", (float)tt.tv_sec, trc_pid
		 , trc_cntl_sp->t_name_a[trc_tid-TRC_MAX_PROCS], args );
    }
    if ((trc_lvl_ip[2]&(1<<lvl)) && (trc_cntl_sp->mode&4))
    {   if (!have_time) { gettimeofday( &tt, 0 ); have_time = 1; }
	TRC_FUNC( "func2", (float)tt.tv_sec, trc_pid
		 , trc_cntl_sp->t_name_a[trc_tid-TRC_MAX_PROCS], args );
    }
    if ((trc_lvl_ip[3]&(1<<lvl)) && (trc_cntl_sp->mode&8))
    {   char    traceLvlStr[33] = "                                ";
	if (!have_time) { gettimeofday( &tt, 0 ); have_time = 1; }
	/* printing is slow, but we need to make it slower by checking ??? */
	printf( "%5d %" TRC_DEF_TO_STR(TRC_MAX_NAME) "s %s%s\n"
	       , trc_pid
	       , trc_cntl_sp->t_name_a[ trc_tid-TRC_MAX_PIDS]
	       , &traceLvlStr[32-lvl]
	       , msg );
    }

    Py_INCREF(Py_None);
    return (Py_None);
}   /* trace_function */



static int
get_depth()
{
    PyThreadState *state;
    state = PyThreadState_Get();  /* gets access to interpreter state */
    return state->recursion_depth;
}

int
code_args_as_string(PyFrameObject *frame,
                    PyCodeObject *code, 
                    char *buf, int len)
/*** XXX note that this does not pick up default args or kwargs */
{  
    int nargs;
    int pos=0;
    char *varname;
    char *value;
    int i,wlen;
    
    buf[0] = '\0';

    nargs = code->co_argcount;
    for (i=0;i<nargs;++i){
        varname = PyString_AsString_Safe(PyObject_Repr(
            PyTuple_GetItem(code->co_varnames,i)));
        value = PyString_AsString_Safe(PyObject_Repr(
            frame->f_localsplus[i]));
        wlen = strlen(varname)+strlen(value)+1;

        if (pos)
            wlen+=2; /* space for comma and space */

        if (pos+wlen<len-3){
            if (pos){
                strcat(buf,", ");
            }
            strcat(buf,varname);
            strcat(buf,"=");
            strcat(buf,value);
            pos+=wlen;
        } else {
	    strcat(buf,"...");
	    break;
	}
    }
    return nargs;
}

/* extract the module name from a frame object */
static 
char *get_mod_name(PyFrameObject *f)
{
    if (!f || !f->f_globals || !PyDict_Check(f->f_globals))
	return "?";
    else 
	return PyString_AsString_Safe(
	    PyDict_GetItemString(f->f_globals,"__name__")); 
}


/******************************************************************************
 * @+Public+@
 * ROUTINE: get_msg:  Added by ron on 30-Mar-1999
 *
 * DESCRIPTION:		stringify args 
 *
 * RETURN VALUES:	None.
 *
 ******************************************************************************/


int
get_msg(  PyObject	*args
	  , char		*msg)
{							/* @-Public-@ */
	int		sts;
	char		*arg_event;
	PyObject        *arg_frame;
	PyObject	*arg_arg;
	PyFrameObject   *frame;
	PyCodeObject	*code;
	char            *function_name;
	char            *module_name;
	char            *source_file;
	int             line_no;
	int             from_line_no;
	char            *from_source_file;
	char            buf[201];
	int             nargs;
	char            *cp;

    sts = PyArg_ParseTuple( args, "OsO", &arg_frame, &arg_event, &arg_arg );

    if (!sts) return (sts);



    /* this next block had better work (i.e. the system should give me what
       I expect -- I will not do error checking */
    frame = (PyFrameObject*)arg_frame;
    code = frame->f_code;


    function_name = PyString_AsString_Safe(code->co_name);
    module_name = get_mod_name(frame); 

    
    source_file = PyString_AsString_Safe(frame->f_code->co_filename);
    cp = strrchr(source_file,'/');
    if (cp) source_file = cp+1;

    line_no = frame->f_lineno;

    if (frame->f_back){
	from_source_file = PyString_AsString_Safe(
	    frame->f_back->f_code->co_filename);
	from_line_no = frame->f_back->f_lineno;
    } else {
	from_source_file = "?";
	from_line_no=0;
    }
    cp = strrchr(from_source_file,'/');
    if (cp) from_source_file = cp+1;


    switch (arg_event[0])
    {
    case 'c':
	nargs = code_args_as_string(frame, code, buf, 200);
	sprintf(  msg, " call %.50s.%.50s(%.200s) from %.50s:%d", 
                        /*XXX kludge alert! note space before "c"  */
                  module_name,function_name, buf, 
		  from_source_file, from_line_no);
	break;
    case 'r':
	sprintf(  msg, "ret  %.50s.%.50s %.500s", module_name, 
		  function_name, 
		  PyString_AsString_Safe(PyObject_Repr(arg_arg)));
	break;
    case 'e':
	sprintf(  msg, "exc  %.50s.%.50s at %.50s:%d", 
		  module_name, function_name, source_file,line_no );
	break;
    default:			/* must be 'l' as in "line" */
	sprintf(  msg, "line %.50s",
		  function_name );
	break;
    }

    return (1);
}   /* get_msg */



/******************************************************************************
 * @+Public+@
 * ROUTINE: profile_function:  Added by ron on 01-May-1998
 *
 * DESCRIPTION:		write to trace buffer
 *
 ******************************************************************************/

static char profile_function_Doc[] = "efficient profile function";

static PyObject *
profile_function(  PyObject	*self
		 , PyObject	*args )
{							/* @-Public-@ */
	char		 msg[20000];
	int		 depth=0;     /* initialize to quiet gcc */
	struct timeval	 tt;
	PyObject	*argsTuple=0; /* initialize to quiet gcc */
	int		 have_mesg=0; /* for all */
	int		 have_time=0; /* for circQ, alarm and log */
	int		 have_dpth=0; /* for circQ and print; has requiremnt */
	int		 have_argT=0; /* for alarm and log */

#   define LVL 31

    if ((trc_lvl_ip[0]&(1<<LVL)) && (trc_cntl_sp->mode&1))
    {   if (!get_msg(args,msg))
	    return (raise_exception("profile_function - parse error"));
	depth = get_depth();
	if (depth > 31) depth=31;
	Ptrace_QPut( &tt, depth, msg );
	have_time = have_mesg = have_dpth = 1;
    }

    if ((trc_lvl_ip[1]&(1<<LVL)) && (trc_cntl_sp->mode&2))
    {   if (!have_time) gettimeofday( &tt, 0 );
	if (!have_mesg)
	    if (!get_msg(args,msg))
		return (raise_exception("profile_function - parse error"));
	argsTuple = Py_BuildValue( "is", 31, msg );
	TRC_FUNC( "func1", (float)tt.tv_sec, trc_pid
		 , trc_cntl_sp->t_name_a[trc_tid-TRC_MAX_PROCS], argsTuple );
	have_time = have_mesg = have_argT = 1;
    }
    if ((trc_lvl_ip[2]&(1<<LVL)) && (trc_cntl_sp->mode&4))
    {   if (!have_time) gettimeofday( &tt, 0 );
	if (!have_mesg)
	    if (!get_msg(args,msg))
		return (raise_exception("profile_function - parse error"));
	if (!have_argT) argsTuple = Py_BuildValue( "is", 31, msg );
	TRC_FUNC( "func2", (float)tt.tv_sec, trc_pid
		 , trc_cntl_sp->t_name_a[trc_tid-TRC_MAX_PROCS], argsTuple );
	have_time = have_mesg = have_argT= 1;
    }
    if ((trc_lvl_ip[3]&(1<<LVL)) && (trc_cntl_sp->mode&8))
    {   char    traceLvlStr[33] = "                                ";
	if (!have_time) gettimeofday( &tt, 0 );
	if (!have_mesg)
	    if (!get_msg(args,msg))
		return (raise_exception("profile_function - parse error"));
	if (!have_dpth) depth = get_depth();
	/* printing is slow, but we need to make it slower by checking ??? */
	printf( "%5d %" TRC_DEF_TO_STR(TRC_MAX_NAME) "s %s%s\n"
	       , trc_pid
	       , trc_cntl_sp->t_name_a[ trc_tid-TRC_MAX_PIDS]
	       , &traceLvlStr[32-depth]
	       , msg );
    }
    if (have_argT) { Py_DECREF( argsTuple ); }


    Py_INCREF(Py_None);
    return (Py_None);
}   /* profile_function */


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
	unsigned	lvl1, lvl2, mask=1;

    sts = PyArg_ParseTuple(  args, "sii|i", &id_s, &lvl1, &lvl2, &mask );
    if (!sts)
	printf( "PyArg_ParseTuple error\n" );

    return (Py_BuildValue("i",traceOnOff(1,mask,id_s,lvl1,lvl2)));
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
	unsigned	lvl1, lvl2, mask=1;

    sts = PyArg_ParseTuple(  args, "sii|i", &id_s, &lvl1, &lvl2, &mask );
    if (!sts)
	printf( "PyArg_ParseTuple error\n" );

    return (Py_BuildValue("i",traceOnOff(0,mask,id_s,lvl1,lvl2)));
}   /* off_function */


static PyObject *
set_str_func(  PyObject	*self
	     , PyObject	*args
	     , char	*str )
{
	int		sts;
	PyObject	*obj_p;

    sts = PyArg_ParseTuple(  args, "O", &obj_p );
    if (!sts) return (raise_exception("set_str_func - parse"));

    if (PyDict_SetItemString(PtraceModuleDict,str,obj_p))
    {   printf( "error???\n" );
	PyErr_Clear();
    }

    return (Py_BuildValue(""));
}   /* set_str_func */


/******************************************************************************
 * @+Public+@
 * ROUTINE: func1_set_function:  Added by ron on 01-May-1998
 *
 * DESCRIPTION:		write to trace buffer
 *
 ******************************************************************************/

static char func1_set_function_Doc[] = "sets the function 1 function :)";

static PyObject *
func1_set_function(  PyObject	*self
	    , PyObject	*args )
{							/* @-Public-@ */
    return (set_str_func(self,args,"func1"));
}   /* func1_set_function */


/******************************************************************************
 * @+Public+@
 * ROUTINE: func2_set_function:  Added by ron on 01-May-1998
 *
 * DESCRIPTION:		write to trace buffer
 *
 ******************************************************************************/

static char func2_set_function_Doc[] = "sets the function 2 function :)";

static PyObject *
func2_set_function(  PyObject	*self
	    , PyObject	*args )
{							/* @-Public-@ */
    return (set_str_func(self,args,"func2"));
}   /* func2_set_function */


/* = = = = = = = = = = = = = = -  Python Module Definitions = = = = = = = = = = = = = = - */

/*  Module Methods table. 

    There is one entry with four items for for each method in the module

    Entry 1 - the method name as used  in python
          2 - the c implementation function
	  3 - flags 
	  4 - method documentation string
	  */

static PyMethodDef Ptrace_Functions[] = {
    { "init",  init_function,  1, init_function_Doc },
    { "trace",  trace_function,  1, trace_function_Doc },
    { "profile",  profile_function,  1, profile_function_Doc },
    { "mode",  mode_function,  1, mode_function_Doc },
    { "on",  on_function,  1, on_function_Doc },
    { "off",  off_function,  1, off_function_Doc },
    { "func1_set",  func1_set_function,  1, func1_set_function_Doc },
    { "func2_set",  func2_set_function,  1, func2_set_function_Doc },
    { 0, 0 }        /* Sentinel */
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
	PyObject *mm;
	PyObject *oo;

    mm = Py_InitModule4(  "Ptrace", Ptrace_Functions, Ptrace_Doc
			, (PyObject*)NULL, PYTHON_API_VERSION );
    PtraceModuleDict = PyModule_GetDict( mm );
    PtraceErrObject = PyErr_NewException( "Ptrace.error", NULL, NULL );
    if (PtraceErrObject != NULL)
	PyDict_SetItemString( PtraceModuleDict, "error", PtraceErrObject );

    /* setup a "default" environment upon module import */
    trace_init( "python", "", 0, 0 );


    PyDict_SetItemString(  PtraceModuleDict, "__builtins__"
			 , PyEval_GetBuiltins() );
    /* extra paren's in next line to quiet gcc */
    if ((oo=PyDict_GetItemString(PtraceModuleDict,"__builtins__")))
    {   oo = PyDict_GetItemString( oo, "str" );
	PtraceStrFunc = oo;
    }
    else printf( "error initializing Ptrace module\n" );
}

