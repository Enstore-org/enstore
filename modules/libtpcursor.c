/*
 * $Id$
 * $Log$
 * Revision 1.6  1999/10/06 02:10:02  huangch
 * Correct the previous stupid comment which was committed by accident.
 * Here is the right comment:
 * [1] remove CurSet2(), of which the function is merged into CurSet()
 * [2] add joinitem(), which can not be used directly by db.py since the
 *     data item is pickled. However, it is left there for preserving the
 *     effort which might be useful in the future.
 *
 * Revision 1.5  1999/10/06 02:04:31  huangch
 * libtpcursor.c
 *
 * Revision 1.4  1999/09/29 23:49:28  huangch
 * add support for duplicated key, cursor, and join
 *
 * Revision 1.3  1999/03/01 18:39:05  huangch
 * add new arguiment to call cursor()
 *
 * Revision 1.2  1999/03/01 18:04:49  huangch
 * correct CurCurrent
 *
 * Revision 1.1.1.1  1998/09/11 16:54:52  huangch
 * ivm's initial version of libtppy -- python interface to libtp
 *
 * Revision 1.3  1998/05/13  20:49:39  ivm
 * Added cursor::delete() method
 *
 * Revision 1.2  1998/05/12  16:45:36  ivm
 * Added upd() method
 *
 *
 */
#include "Python.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>

#include	"libtp.h"


staticforward PyTypeObject CurType;
extern PyTypeObject TxnType;

PyObject*
libtpdb_cursor(DbObject *dp, PyObject* args)
{
	/* No args accepted for now */
	int status;
	DBC* cursor = NULL;
	CurObject* c;
	TxnObject* t = NULL;
	DB_TXN*	tid = NULL;
	

	check_dbobject_open(dp);

	if(!PyArg_ParseTuple(args, "|O", &t))
		return NULL;

	if( t != NULL && t -> ob_type == &TxnType )
		tid = t -> tid;

	status = dp -> db -> cursor(dp -> db, tid, &cursor, 0);
	if( status )
	{
		PyErr_SetFromErrno(LibtpError);
		return NULL;
	}

	if ((c = PyObject_NEW(CurObject, &CurType)) == NULL)
		return NULL;

	c -> cursor = cursor;
	c -> db = dp;
	c -> txn = t;
	Py_XINCREF(t);
	Py_XINCREF(dp);

	return (PyObject*) c;
}

#define MAX_INDEX 32

/* libtpdb_join() is not static for it has to be seen by libtpmodule */

PyObject*
libtpdb_join(PyObject *self, PyObject *args)
{
	DbObject *primary;
	PyObject *curslist;
	int flags;
	DBC *dbcp;
	DBC *cursors[MAX_INDEX+1];	/* avoid memory allocation */
	int idx;
	CurObject *c;
	int status;

	/* get arguments */

	if (!PyArg_ParseTuple(args, "OO|i", &primary, &curslist, &flags))
	{
		PyErr_SetString(LibtpError, "Wrong arguments to join()");
		return(NULL);
	}

	/* convert Python list of cursors to C array of cursors */

	idx = PyList_Size(curslist);
	cursors[idx--] = NULL;

	for (; idx >= 0; idx--)
	{
		cursors[idx] = ((CurObject*)PyList_GetItem(curslist, idx))->cursor;
	}

	status = primary->db->join(primary->db, cursors, flags, &dbcp);

	/* handle join error */

	if (status)
	{
		PyErr_SetFromErrno(LibtpError);
		return(NULL);
	}

	if ((c = PyObject_NEW(CurObject, &CurType)) == NULL)
	{
		return(NULL);
	}

	c->cursor = dbcp;
	c->db = NULL;
	c->txn = NULL;
	
	return (PyObject*) c;
}

/*
*	LIBTP cursor type implementation
*/

#define is_curobject(v) ((v)->ob_type == &CurType)
#define check_curobject_open(v) if ((v)->cursor == NULL) \
               { PyErr_SetString(LibtpError, "LIBTP DB cursor has already been closed"); \
                 return NULL; }


/*
*	LIBTP DB Cursor methods
*/


static PyObject *
CurGet(c, args, flag)
	CurObject *c;
        PyObject *args;
        int flag;
{
	int status;
	DBT krec, drec;

	check_curobject_open(c);
	memset(&krec, 0, sizeof(krec));
	memset(&drec, 0, sizeof(drec));

	if ( args != NULL )
	{
		/* some methods accept key argument */
		if (!PyArg_ParseTuple(args, "s#|s#",
			      &krec.data, &krec.size, &drec.data, &drec.size))
			return NULL;
	}

	/* take care of DB_GET_BOTH */

	if ((flag == DB_SET) && drec.size)
	{
		flag = DB_GET_BOTH;
	}

	status = (c->cursor->c_get)(c->cursor, &krec,
				     &drec, flag);
	if (status != 0) {
		if (status != DB_NOTFOUND)
			PyErr_SetFromErrno(LibtpError);
		else
			PyErr_SetObject(PyExc_KeyError, args);
		return NULL;
	}

	return Py_BuildValue("s#s#", krec.data, krec.size,
			     drec.data, drec.size);
}

static PyObject *
CurFirst(c)
	CurObject *c;
{
	return CurGet(c, NULL, DB_FIRST);
}

static PyObject *
CurNext(c)
	CurObject *c;
{
	return CurGet(c, NULL, DB_NEXT);
}

static PyObject *
CurNextDup(c)
	CurObject *c;
{
	return CurGet(c, NULL, DB_NEXT_DUP);
}

static PyObject *
CurPrev(c)
	CurObject *c;
{
	return CurGet(c, NULL, DB_PREV);
}

static PyObject *
CurLast(c)
	CurObject *c;
{
	return CurGet(c, NULL, DB_LAST);
}

static PyObject *
CurCurrent(c)
	CurObject *c;
{
	return CurGet(c, NULL, DB_CURRENT);
}

static PyObject *
CurSet(c, key)
	CurObject *c;
        PyObject *key;
{
	return CurGet(c, key, DB_SET);
}

static PyObject *
CurJoinItem(c)
	CurObject *c;
{
	return CurGet(c, NULL, DB_JOIN_ITEM);
}

static PyObject *
CurSetRange(c, key)
	CurObject *c;
        PyObject *key;
{
	return CurGet(c, key, DB_SET_RANGE);
}

static PyObject*
CurClose(c)
	CurObject *c;
{
	if( c->cursor != NULL )
		c -> cursor -> c_close(c -> cursor);
	c -> cursor = NULL;
	Py_INCREF(Py_None);
	return Py_None;
}

static void
CurDealloc(c)
	CurObject *c;
{
	if( c -> cursor != NULL && c -> db -> db != NULL )
	{
		c -> cursor -> c_close(c -> cursor);
		c -> cursor = NULL;
	}
	Py_XDECREF(c -> db);
	Py_XDECREF(c -> txn);
	PyMem_DEL(c);
}

static	PyObject*
CurPut(CurObject* c, DBT* key, DBT* data, int flags)
{
	int status;
	check_curobject_open(c);
	status = c->cursor->c_put(c->cursor, key, data, flags);
	return Py_BuildValue("i", status);
}

static PyObject*
CurUpd(CurObject* c, PyObject* arg)
{
	int status;
	DBT	data;

	memset(&data, 0, sizeof(data));

	if( !PyArg_Parse(arg, "s#", &data.data, &data.size) )
		return NULL;

	return CurPut(c, 0, &data, DB_CURRENT);
}

static PyObject*
CurDel(CurObject* c)
{
	int status;
	check_curobject_open(c);
	status = c->cursor->c_del(c->cursor, 0);
	return Py_BuildValue("i", status);
}

static PyMethodDef CurMethods[] = {
	{"set",		(PyCFunction)CurSet,	METH_VARARGS},
	{"setRange",	(PyCFunction)CurSetRange, METH_VARARGS},
	{"next",	(PyCFunction)CurNext,	METH_VARARGS},
	{"nextDup",	(PyCFunction)CurNextDup,METH_VARARGS},
	{"prev",	(PyCFunction)CurPrev,	METH_VARARGS},
	{"first",	(PyCFunction)CurFirst,	METH_VARARGS},
	{"last",	(PyCFunction)CurLast,	METH_VARARGS},
	{"current",	(PyCFunction)CurCurrent,METH_VARARGS},
	{"close",	(PyCFunction)CurClose,	METH_VARARGS},
	{"update",	(PyCFunction)CurUpd,	METH_VARARGS},
	{"joinitem",	(PyCFunction)CurJoinItem, METH_VARARGS},
	{"delete",	(PyCFunction)CurDel,	METH_VARARGS},
	{NULL,	       	NULL}		/* sentinel */
};

static PyObject *
CurGetAttr(c, name)
	PyObject *c;
        char *name;
{
	return Py_FindMethod(CurMethods, c, name);
}

static PyTypeObject CurType = {
	PyObject_HEAD_INIT(&PyType_Type)
	0,
	"libtpcursor",
	sizeof(CurObject),
	0,
	(destructor)CurDealloc, /*tp_dealloc*/
	0,			/*tp_print*/
	(getattrfunc)CurGetAttr, /*tp_getattr*/
	0,			/*tp_setattr*/
	0,			/*tp_compare*/
	0,			/*tp_repr*/
	0,			/*tp_as_number*/
	0,			/*tp_as_sequence*/
	0
};

