/*
 * $Id$
 * $Log$
 * Revision 1.1.1.1  1998/09/11 16:54:52  huangch
 * ivm's initial version of libtppy -- python interface to libtp
 *
 * Revision 1.2  1998/05/12  16:45:36  ivm
 * Added upd() method
 *
 *
 */

#include "Python.h"
#include "libtp.h"

static void 		TxnDealloc(TxnObject*);
static PyObject*	TxnGetattr(PyObject*, char*);

PyTypeObject TxnType = {
	PyObject_HEAD_INIT(&PyType_Type)
	0,
	"bsdtxn",
	sizeof(TxnObject),
	0,
	(destructor)TxnDealloc, /*tp_dealloc*/
	0,			/*tp_print*/
	(getattrfunc)TxnGetattr, /*tp_getattr*/
	0,			/*tp_setattr*/
	0,			/*tp_compare*/
	0,			/*tp_repr*/
	0,			/*tp_as_number*/
	0,			/*tp_as_sequence*/
	0,			/*tp_as_mapping*/	/* ??? */
};

PyObject*
libtpdb_txn(DbObject* self, PyObject* args)
{
	EnvObject	*env = NULL;
	DB_TXN 	*parent = NULL,
		*tid = NULL;
	TxnObject	*pobj = NULL,
			*ptr = NULL;
	int	status;

	env = self -> env;

	if( env == NULL || env -> env.tx_info == NULL )
	{
		PyErr_SetString(LibtpError, 
			"LIBTP Transaction Manager is not initialized");
		return NULL;
	}

	if (!PyArg_ParseTuple(args, "|O", &pobj))
		return NULL;
	else if( pobj != NULL )
		parent = pobj -> tid;

	status = txn_begin(env->env.tx_info, parent, &tid);
	if( status )
	{
		PyErr_SetFromErrno(LibtpError);
		return NULL;
	}

	if ((ptr = PyObject_NEW(TxnObject, &TxnType)) == NULL)
		return NULL;

	ptr -> tid = tid;
	ptr -> db = self;
	Py_INCREF(self);
		
	return (PyObject*) ptr;
}

static PyObject*
TxnCommit(TxnObject *tx)
{
	int	status = 0;

	if( tx -> tid != NULL )
	{
#ifdef	DEBUG
		fprintf(stderr, "TxnCommit(%x)... ", txn_id(tx->tid));
#endif
		status = txn_commit(tx -> tid);
#ifdef	DEBUG
		fprintf(stderr, "txn_commit() = %d\n", status);
#endif
	}
	tx -> tid = NULL;
	return Py_BuildValue("i", status);
}

static PyObject*
TxnAbort(TxnObject *tx)
{
	int	status = 0;

	if( tx -> tid != NULL )
	{
#ifdef	DEBUG
		fprintf(stderr, "TxnAbort(%x)... ", txn_id(tx->tid));
#endif
		status = txn_abort(tx -> tid);
#ifdef	DEBUG
		fprintf(stderr, "txn_abort() = %d\n", status);
#endif
	}
	tx -> tid = NULL;
	return Py_BuildValue("i", status);
}

static PyObject*
TxnPrepare(TxnObject *tx)
{
	int	status;

	if( tx -> tid == NULL )
		return NULL;

#ifdef	DEBUG
	fprintf(stderr, "TxnPrepare(%x)... ", txn_id(tx->tid));
#endif
	status = txn_prepare(tx -> tid);
	return Py_BuildValue("i", status);
}

static void
TxnDealloc(TxnObject *tx)
{
	if( tx -> tid != NULL )
	{
#ifdef	DEBUG
		fprintf(stderr, "TxnDealloc(%x)...\n", txn_id(tx->tid));
#endif
		TxnAbort(tx);
	}
#ifdef	DEBUG
	else
		fprintf(stderr, "TxnDealloc(aborted)...\n");
#endif
	Py_XDECREF(tx -> db);
	PyMem_DEL(tx);
}

static PyMethodDef txn_methods[] = {
	{"abort",		(PyCFunction)TxnAbort},
	{"commit",		(PyCFunction)TxnCommit},
	{"prepare",		(PyCFunction)TxnPrepare},
	{NULL,	       	NULL}		/* sentinel */
};

static PyObject *
TxnGetattr(t, name)
	PyObject *t;
        char *name;
{
	PyObject* ptr;
	ptr = Py_FindMethod(txn_methods, t, name);
	return ptr;
	
}

