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

staticforward PyTypeObject LockType;

/*
* LIBTP lock constructor.
*
* lock = env.lock(name,mode)
*/

PyObject*
libtpenv_newlock(EnvObject* self, PyObject* args)
{
	DBT	name;
	
	/* By default create lock in NOT-GRANTED MODE */
	db_lockmode_t   mode = DB_LOCK_NG;

	int		flags = 0;
	int		status;
	char*		flags_str;
	LockObject*	ptr = NULL;

	if( self -> env.lk_info == NULL )
	{
		PyErr_SetString(LibtpError, "LIBTP Lock Manager is not initialized");
		return NULL;
	}

	memset(&name, 0, sizeof(name));

	if (!PyArg_ParseTuple(args, "s#s", &name.data, &name.size, &flags_str))
		return NULL;

	if( flags_str != NULL )
	{
		switch(flags_str[0])
		{
		case 0:
			break;
		case 'r':
			mode = DB_LOCK_READ;
			break;
		
		case 'w':
			mode = DB_LOCK_WRITE;
			break;
		
		case 'z':
			mode = DB_LOCK_NG;
			break;
		}

		if( flags_str[0] && flags_str[1] == 'n' )
			flags |= DB_LOCK_NOWAIT;
	}

	if ((ptr = PyObject_NEW(LockObject, &LockType)) == NULL)
		return NULL;

	ptr -> lock = 0;
	status = lock_get(self->env.lk_info, getpid(), flags, &name, mode, 
			&ptr->lock );
	if( status )
	{
		PyErr_SetFromErrno(LibtpError);
		PyMem_DEL(ptr);
		return NULL;
	}

	ptr -> env = self;
	Py_INCREF(self);
	return (PyObject*)ptr;
}

/*
* LIBTP lock constructor.
*
* lock = db.lock(name,mode)
*/

PyObject*
libtpdb_newlock(DbObject* self, PyObject* args)
{
	if( self -> env == NULL )
	{
		PyErr_SetString(LibtpError, "LIBTP envoronment is not open");
		return NULL;
	}

	return libtpenv_newlock(self -> env, args);
}

static PyObject*
LockRelease(LockObject* ptr)
{
	if( ptr -> env != NULL && 
			ptr -> env -> env.lk_info != NULL && 
			ptr->lock != 0 )
		lock_put( ptr->env->env.lk_info, ptr->lock );

	ptr -> lock = 0;
	Py_INCREF(Py_None);
	return Py_None;
}

static void 
LockDealloc(LockObject* ptr)
{
	LockRelease(ptr);
	Py_XDECREF(ptr->env);
	PyMem_DEL(ptr);
}

static PyMethodDef lock_methods[] = {
	{"release",		(PyCFunction)LockRelease},
	{NULL,	       	NULL}		/* sentinel */
};

static PyObject *
LockGetattr(t, name)
	PyObject *t;
        char *name;
{
	PyObject* ptr;
	ptr = Py_FindMethod(lock_methods, t, name);
	return ptr;
}

static PyTypeObject LockType = {
	PyObject_HEAD_INIT(&PyType_Type)
	0,
	"libtplock",
	sizeof(LockObject),
	0,
	(destructor)LockDealloc, /*tp_dealloc*/
	0,			/*tp_print*/
	(getattrfunc)LockGetattr, /*tp_getattr*/
	0,			/*tp_setattr*/
	0,			/*tp_compare*/
	0,			/*tp_repr*/
	0,			/*tp_as_number*/
	0,			/*tp_as_sequence*/
	0,			/*tp_as_mapping*/	/* ??? */
};

