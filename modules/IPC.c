/*  This file (IPC.c) was created by Ron Rechenmacher <ron@fnal.gov> on
    Nov  5, 1998. "TERMS AND CONDITIONS" governing this file are in the README
    or COPYING file. If you do not have such a file, one can be obtained by
    contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
    $RCSfile$
    $Revision$
    $Date$
    */


#include "Python.h"
#include "structmember.h"	/* for python member stuff */

#include <unistd.h>		/* sleep */

#include <sys/ipc.h>		/* IPC_PRIVATE, IPC_CREAT */
#include <sys/types.h>		/* msgget, semget */
#include <sys/msg.h>		/* msgget */
#include <sys/sem.h>		/* semget */
#include <sys/shm.h>		/* shmget, shmat, shmdt */

#include "IPC.h"		/* struct s_IPCshmgetObject */

/* POSIXly correct systems don't define union semun in their system headers */

/* This is for Linux */

#ifdef _SEM_SEMUN_UNDEFINED
union semun
{
    int val;	               /* value for SETVAL */
    struct semid_ds *buf;      /* buffer for IPC_STAT & IPC_SET */
    unsigned short int *array; /* array for GETALL & SETALL */
    struct seminfo *__buf;     /* buffer for IPC_INFO */
};
#endif

/* This is for SunOS and OSF1 */

#if defined(sun) || defined(__osf__)
union semun
{
    int val;	               /* value for SETVAL */
    struct semid_ds *buf;      /* buffer for IPC_STAT & IPC_SET */
    unsigned short int *array; /* array for GETALL & SETALL */
    struct seminfo *__buf;     /* buffer for IPC_INFO */
};
#endif




static	PyObject	*g_ErrObject;


/*****************************************************************************
 */

static PyObject *
raise_exception( char *msg )
{
	char		buf[200];
        PyObject	*v;
        int		i = errno;

#   ifdef EINTR
    if ((i==EINTR) && PyErr_CheckSignals()) return NULL;
#   endif

    sprintf( buf, "%s - %s", msg, strerror(i) );
    v = Py_BuildValue( "(is)", i, buf );
    if (v != NULL)
    {   PyErr_SetObject( g_ErrObject, v );
	Py_DECREF(v);
    }
    return NULL;
}


/*****************************************************************************
 * IPCshmget - object generation and object methods
 */

static PyObject *
IPCshmget_offset( struct s_IPCshmgetObject	*so
		 ,PyObject			*args )
{
	int	off;
	int	val_i;

    /* I do not know when to use _Parse and when to use _ParseTuple */
    if (!PyArg_ParseTuple(args,"ii",&off,&val_i)) return (NULL);

    if (off > (so->size_bytes/sizeof(int)))
	return (raise_exception("past end"));

    *(so->i_p+off) = val_i;

    Py_INCREF( Py_None );
    return (Py_None);
}

static PyObject *
IPCshmget_offget( struct s_IPCshmgetObject	*so
		 ,PyObject			*args )
{
	int	off;

    /* I do not know when to use _Parse and when to use _ParseTuple */
    if (!PyArg_Parse(args,"i",&off)) return (NULL);

    if (off > (so->size_bytes/sizeof(int)))
	return (raise_exception("past end"));

    return (Py_BuildValue("i",*(so->i_p+off)));
}


/* List of methods for shmget (shm) objects */
static PyMethodDef IPCshmget_methods[] = {
    {"offset",	(PyCFunction)IPCshmget_offset},
    {"offget",	(PyCFunction)IPCshmget_offget},
    {NULL,	NULL}           /* sentinel */
};

#define SHMOFF(x) offsetof(struct s_IPCshmgetObject, x)

static struct memberlist IPCshmget_memberlist[] = {
        {"id",   T_INT,          SHMOFF(id)},
        {NULL}  /* Sentinel */
};

/*********----------------------------------------------------------**********/

static void
IPCshmget_dealloc( struct s_IPCshmgetObject	*so )
{
    (void)shmdt(  so->i_p );
    (void)shmctl( so->id, IPC_RMID, 0 );
    PyMem_DEL( so );
    return;
}

static PyObject *
IPCshmget_getattr(  struct s_IPCshmgetObject	*so
		  , char			*name )
{
	PyObject	*rr;

    rr = Py_FindMethod( IPCshmget_methods, (PyObject *)so, name );

    if (rr) return (rr);

    PyErr_Clear();
    return (PyMember_Get((char *)so,IPCshmget_memberlist,name));
}

/*static - no static! -- allow others to test for specific object type */
PyTypeObject IPCshmget_Type = {
    PyObject_HEAD_INIT(0)   /* Must fill in type value later */
    0,
    "shmget",
    sizeof(struct s_IPCshmgetObject),
    0,
    (destructor)IPCshmget_dealloc, /*tp_dealloc*/
    0,              /*tp_print*/
    (getattrfunc)IPCshmget_getattr, /*tp_getattr*/ /* NOT needed to call shmat */
    0,              /*tp_setattr*/
    0,              /*tp_compare*/
    0/*(reprfunc)IPCshmget_repr*/, /*tp_repr*/
    0,              /*tp_as_number*/
    0,              /*tp_as_sequence*/
    0,              /*tp_as_mapping*/
};


/*********----------------------------------------------------------**********/

static char IPC_shmget_Doc[] = "";

static PyObject *
IPC_shmget(  PyObject	*self
	   , PyObject	*args )
{							/* @-Public-@ */
	int				key, size_bytes, shmflg;
	struct s_IPCshmgetObject	*so;

    if (!PyArg_ParseTuple(args,"iii",&key,&size_bytes,&shmflg)) return (NULL);

    so = PyObject_NEW( struct s_IPCshmgetObject, &IPCshmget_Type );
    if (so == NULL) return (NULL);

    so->id = shmget( key, size_bytes, shmflg );
    if (so->id == -1)
    {   Py_DECREF( so );
	return (raise_exception("shmget"));
    }

    so->size_bytes = size_bytes;
    so->i_p = (int *)shmat( so->id, 0, 0 );

    if (so->i_p == (int *)-1)
    {   (void)shmctl( so->id, IPC_RMID, 0 );
	Py_DECREF( so );
	return (raise_exception("shmget"));
    }

    /*    rr = Py_BuildValue( "O", so );
	  return (rr);
	  Py_XDECREF( so );
    */
    return ((PyObject *)so);
}   /* IPC_shmget */



/*****************************************************************************
 * IPCsemget - object generation and object methods
 */

struct s_IPCsemgetObject {
    PyObject_HEAD
    int	id;
};

/* List of methods for semget (shm) objects */
static PyMethodDef IPCsemget_methods[] = {
    {NULL,	NULL}           /* sentinel */
};

#define SEMOFF(x) offsetof(struct s_IPCsemgetObject, x)

static struct memberlist IPCsemget_memberlist[] = {
        {"id",   T_INT,          SEMOFF(id)},
        {NULL}  /* Sentinel */
};

/*********----------------------------------------------------------**********/

static void
IPCsemget_dealloc( struct s_IPCsemgetObject	*so )
{
	union semun	semun_u;

    semun_u.val = 0;/* just initialize ("arg" is not used for RMID)*/
    (void)semctl( so->id, 0, IPC_RMID, semun_u );
    PyMem_DEL( so );
    return;
}

static PyObject *
IPCsemget_getattr(  struct s_IPCsemgetObject	*so
		  , char			*name )
{
	PyObject	*rr;

    rr = Py_FindMethod( IPCsemget_methods, (PyObject *)so, name );

    if (rr) return (rr);

    PyErr_Clear();
    return (PyMember_Get((char *)so,IPCsemget_memberlist,name));
}

static PyTypeObject IPCsemget_Type = {
    PyObject_HEAD_INIT(0)   /* Must fill in type value later */
    0,
    "semget",
    sizeof(struct s_IPCsemgetObject),
    0,
    (destructor)IPCsemget_dealloc, /*tp_dealloc*/
    0,              /*tp_print*/
    (getattrfunc)IPCsemget_getattr, /*tp_getattr*/
    0,              /*tp_setattr*/
    0,              /*tp_compare*/
    0/*(reprfunc)IPCsemget_repr*/, /*tp_repr*/
    0,              /*tp_as_number*/
    0,              /*tp_as_sequence*/
    0,              /*tp_as_mapping*/
};


/*********----------------------------------------------------------**********/

static char IPC_semget_Doc[] = "";

static PyObject *
IPC_semget(  PyObject	*self
	   , PyObject	*args )
{							/* @-Public-@ */
	int				key, nsems, shmflg;
	struct s_IPCsemgetObject	*so;

    if (!PyArg_ParseTuple(args,"iii",&key,&nsems,&shmflg)) return (NULL);

    so = PyObject_NEW( struct s_IPCsemgetObject, &IPCsemget_Type );
    if (so == NULL) return (NULL);

    so->id = semget( key, nsems, shmflg );
    if (so->id == -1)
    {   Py_DECREF( so );
	return (raise_exception("semget"));
    }

    /*    rr = Py_BuildValue( "O", so );
	  return (rr);
	  Py_XDECREF( so );
    */
    return ((PyObject *)so);
}   /* IPC_semget */



/*****************************************************************************
 * IPCmsgget - object generation and object methods
 */

struct s_IPCmsggetObject {
    PyObject_HEAD
    int	id;
};


/* List of methods for msgget (shm) objects */
static PyMethodDef IPCmsgget_methods[] = {
    {NULL,	NULL}           /* sentinel */
};

#define MSGOFF(x) offsetof(struct s_IPCmsggetObject, x)

static struct memberlist IPCmsgget_memberlist[] = {
        {"id",   T_INT,          MSGOFF(id)},
        {NULL}  /* Sentinel */
};


/*********----------------------------------------------------------**********/

static void
IPCmsgget_dealloc( struct s_IPCmsggetObject	*so )
{
    (void)msgctl( so->id, IPC_RMID, 0 );
    PyMem_DEL( so );
    return;
}

static PyObject *
IPCmsgget_getattr(  struct s_IPCmsggetObject	*so
		  , char			*name )
{
	PyObject	*rr;

    rr = Py_FindMethod( IPCmsgget_methods, (PyObject *)so, name );

    if (rr) return (rr);

    PyErr_Clear();
    return (PyMember_Get((char *)so,IPCmsgget_memberlist,name));
}

static PyTypeObject IPCmsgget_Type = {
    PyObject_HEAD_INIT(0)   /* Must fill in type value later */
    0,
    "msgget",
    sizeof(struct s_IPCmsggetObject),
    0,
    (destructor)IPCmsgget_dealloc, /*tp_dealloc*/
    0,              /*tp_print*/
    (getattrfunc)IPCmsgget_getattr, /*tp_getattr*/ /* NOT needed to call shmat */
    0,              /*tp_setattr*/
    0,              /*tp_compare*/
    0/*(reprfunc)IPCmsgget_repr*/, /*tp_repr*/
    0,              /*tp_as_number*/
    0,              /*tp_as_sequence*/
    0,              /*tp_as_mapping*/
};


/*********----------------------------------------------------------**********/

static char IPC_msgget_Doc[] = "";

static PyObject *
IPC_msgget(  PyObject	*self
	   , PyObject	*args )
{							/* @-Public-@ */
	int				key, shmflg;
	struct s_IPCmsggetObject	*so;

    if (!PyArg_ParseTuple(args,"ii",&key,&shmflg)) return (NULL);

    so = PyObject_NEW( struct s_IPCmsggetObject, &IPCmsgget_Type );
    if (so == NULL) return (NULL);

    so->id = msgget( key, shmflg );
    if (so->id == -1)
    {   Py_DECREF( so );
	return (raise_exception("msgget"));
    }

    /*    rr = Py_BuildValue( "O", so );
	  return (rr);
	  Py_XDECREF( so );
    */
    return ((PyObject *)so);
}   /* IPC_msgget */



/*****************************************************************************
 */

static void
insint(  PyObject	*d
       , char		*name
       , int		value )
{
    PyObject *v = PyInt_FromLong((long) value);
    if (!v || PyDict_SetItemString(d,name,v))
	PyErr_Clear();

    Py_XDECREF(v);
}

static PyMethodDef IPC_methods[] = {
    { "shmget", IPC_shmget, 1, IPC_shmget_Doc },
    { "semget", IPC_semget, 1, IPC_semget_Doc },
    { "msgget", IPC_msgget, 1, IPC_msgget_Doc },
    { 0, 0}        /* Sentinel */
};

static char IPC_Doc[] = "interface to shm, sem and msgq IPC routines";

void
initIPC()
{
	PyObject	*oo;

    oo = Py_InitModule4(  "IPC", IPC_methods, IPC_Doc, (PyObject*)NULL
			, PYTHON_API_VERSION );
    oo= PyModule_GetDict( oo );
    g_ErrObject = PyErr_NewException( "IPC.error", NULL, NULL );
    if (g_ErrObject == NULL) return;

    PyDict_SetItemString( oo, "error", g_ErrObject );

    /*    IPCshmget_Type.ob_type = &PyType_Type;
    Py_INCREF( &IPCshmget_Type );
    if (   PyDict_SetItemString(oo,"IPCshmgetType",(PyObject *)&IPCshmget_Type)
	!= 0)
	return;
    */

    insint( oo, "IPC_PRIVATE", IPC_PRIVATE );
    insint( oo, "IPC_CREAT", IPC_CREAT );

    return;
}

