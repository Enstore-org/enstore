/*
 * $Id$
 * $Log$
 * Revision 1.9  1999/10/06 02:14:06  huangch
 * Correct the previous stupid comment which was committed by accident
 * Here is the correct comment:
 * [1] add flag dupsort to allow the duplicated keys to be sorted by the
 *     value field. This is essential for join cursor to work.
 *
 * Revision 1.8  1999/10/06 02:04:31  huangch
 * libtpcursor.c
 *
 * Revision 1.7  1999/09/29 23:49:28  huangch
 * add support for duplicated key, cursor, and join
 *
 * Revision 1.6  1999/08/25 22:39:36  huangch
 * add DB_INIT_CDB, DB_RECOVER, and DB_RECOVER_FATAL
 *
 * Revision 1.5  1999/08/19 22:59:26  huangch
 * add status(), to show the status of the database, and length(), to show the number of the records
 *
 * Revision 1.4  1999/03/01 20:13:10  huangch
 * make thread default
 *
 * Revision 1.3  1999/03/01 18:42:18  huangch
 * add one more argument to call cursor()
 *
 * Revision 1.2  1999/03/01 18:12:40  huangch
 * add THREAD option
 *
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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>

/* Please don't include internal header files of the Berkeley db package
   (it messes up the info required in the Setup file) */

extern PyTypeObject TxnType;

staticforward PyTypeObject DbType;

PyObject *LibtpError;

static PyObject *
libtpdb_close(dp)
	DbObject *dp;
{
	if (dp->db != NULL) {
		(dp->db->sync)(dp->db, 0);
		if ((dp->db->close)(dp->db, 0) != 0)
                        PyErr_SetFromErrno(LibtpError);
		dp->db = NULL;
	}
#ifdef	DEBUG
	fprintf(stderr, "libtpdb closed\n");
#endif
	Py_XDECREF(dp->env);
	dp->env = NULL;
	Py_INCREF(Py_None);
	return Py_None;
}

static void
libtpdb_dealloc(dp)
	DbObject *dp;
{
#ifdef	DEBUG
	fprintf(stderr, "libtpdb_dealloc() ...\n");
#endif
	libtpdb_close(dp);
	PyMem_DEL(dp);
}

/*
static int
libtpdb_length(dp)
	DbObject *dp;
{
	return dp->db == NULL ? 0 : 1;
}
*/

/* length() -- return the number of records in database */

static int
libtpdb_length(dp)
DbObject *dp;
{
	DB_BTREE_STAT *bt_status;

	/* DB_RECORDCOUNT doesn't work here
	if (dp->db->stat(dp->db, &bt_status, NULL, DB_RECORDCOUNT))
	*/
	if (dp->db->stat(dp->db, &bt_status, NULL, 0))
	{
		/* handling erro here */
	}

	return (bt_status->bt_nrecs);
}

/* status() -- return the status of database */

static PyObject *
libtpdb_status(dp)
DbObject *dp;
{
	DB_BTREE_STAT *bt_status;

	if (dp->db->stat(dp->db, &bt_status, NULL, 0))
	{
		/* handling erro here */
	}

	return Py_BuildValue("{s:i,s:i,s:i,s:i,s:i,s:i,s:i,s:i,s:i,s:i,s:i,s:i,s:i,s:i,s:i,s:i,s:i,s:i,s:i}",
		"flags",	bt_status->bt_flags,
		"maxkey",	bt_status->bt_maxkey,
		"minkey",	bt_status->bt_minkey,
		"re_len",	bt_status->bt_re_len,
		"re_pad",	bt_status->bt_re_pad,
		"pagesize",	bt_status->bt_pagesize,
		"levels",	bt_status->bt_levels,
		"nrecs",	bt_status->bt_nrecs,
		"int_pg",	bt_status->bt_int_pg,
		"leaf_pg",	bt_status->bt_leaf_pg,
		"dup_pg",	bt_status->bt_dup_pg,
		"over_pg",	bt_status->bt_over_pg,
		"free",		bt_status->bt_free,
		"int_pgfree",	bt_status->bt_int_pgfree,
		"leaf_pgfree",	bt_status->bt_leaf_pgfree,
		"dup_pgfree",	bt_status->bt_dup_pgfree,
		"over_pgfree",	bt_status->bt_over_pgfree,
		"magic",	bt_status->bt_magic,
		"version",	bt_status->bt_version);
}

static	PyObject*
GetByKey(DbObject *dp, char* kptr, int ksiz, DB_TXN* tid)
{
	DBT	drec;
	DBT	krec;
	int	status;

	memset(&krec, 0, sizeof(krec));
	memset(&drec, 0, sizeof(drec));

	krec.data = kptr;
	krec.size = ksiz;

        status = (dp->db->get)(dp->db, tid, &krec, &drec, 0);
	if( status )
	{
		if( status == DB_NOTFOUND )
                        PyErr_SetObject(PyExc_KeyError, 
				PyString_FromStringAndSize(kptr, ksiz));
                else
                        PyErr_SetFromErrno(LibtpError);
                return NULL;
	}
	else
		return PyString_FromStringAndSize((char *)drec.data, 
			(int)drec.size);

}

static	int ParseKeyTuple(PyObject* args, char** data, int* size,
		TxnObject** txn)
{
	int		ok = 0;
	PyObject*	ptr = NULL;
	*txn = NULL;

	if( PyTuple_Check(args) )
	{
		ok = PyArg_ParseTuple(args, "s#|O", data, size, &ptr);
		if( ptr != NULL && ptr -> ob_type == &TxnType )
			*txn = (TxnObject*)ptr;
	}
	else if( PyString_Check(args) )
		ok = PyArg_Parse(args, "s#", data, size);

	if( !ok )
		PyErr_SetString(PyExc_TypeError,
			"libtp key type must be string or (string,txn)");
	return ok;
}

static PyObject *
libtpdb_subscript(dp, key)
	DbObject *dp;
        PyObject *key;
{
	int status;
	char *data;
	int size;
	TxnObject* txn = NULL;
	DB_TXN*	tid = NULL;

        check_dbobject_open(dp);
	if( !ParseKeyTuple(key, &data, &size, &txn) )
		return NULL;
	
	if( txn != NULL )
		tid = txn -> tid;
	return GetByKey(dp, data, size, tid);
}

static int
libtpdb_ass_sub(dp, key, value)
	DbObject *dp;
        PyObject *key, *value;
{

	int status;
	DBT krec, drec;
	char *data;
	int size;
	TxnObject* txn = NULL;
	DB_TXN*	tid = NULL;
	
        check_dbobject_open(dp);
	memset(&krec, 0, sizeof(krec));
	memset(&drec, 0, sizeof(drec));
	if( !ParseKeyTuple(key, &data, &size, &txn) )
		return (int) NULL;
	krec.data = data;
	krec.size = size;
	
	if( txn != NULL )
		tid = txn -> tid;


	if (value == NULL) {
		status = (dp->db->del)(dp->db, tid, &krec, 0);
		if( status == DB_NOTFOUND || status == DB_KEYEMPTY )
				status = 0;
	}
	else {
		if (!PyArg_Parse(value, "s#", &data, &size)) {
			PyErr_SetString(PyExc_TypeError,
					"DB value type must be string");
			return -1;
		}
		drec.data = data;
		drec.size = size;
#if 0
		/* For RECNO, put fails with 'No space left on device'
		   after a few short records are added??  Looks fine
		   to this point... linked with 1.85 on Solaris Intel
		   Roger E. Masse 1/16/97
		 */
		fprintf(stderr, "before put data: '%s', size: %d\n",
		       drec.data, drec.size);
		fprintf(stderr, "before put key= '%s', size= %d\n",
		       krec.data, krec.size);
#endif
		status = (dp->db->put)(dp->db, tid, 
			&krec, &drec, 0);
	}
	
	if (status != 0) {
		if (status < 0)
			PyErr_SetFromErrno(LibtpError);
		else
			PyErr_SetObject(PyExc_KeyError, key);
		return -1;
	}
	return 0;
}

static PyObject *
libtpdb_keys(dp, args)
	DbObject *dp;
        PyObject *args;
{
	PyObject *list, *item;
	DBT krec, drec;
	int status;
	int err;
	DBC*	cursor;
	int	n = 0;
	TxnObject* txn = NULL;
	DB_TXN*	tid = NULL;
	
	check_dbobject_open(dp);

	if( !PyArg_ParseTuple(args, "|O", &txn) )
		return NULL;

	if( txn != NULL )
		tid = txn -> tid;

	list = PyList_New(0);
	if (list == NULL)
		return NULL;

	status = dp -> db -> cursor(dp -> db, tid, &cursor, 0);
	if( status )
		return NULL;

	memset(&krec, 0, sizeof(krec));
	memset(&drec, 0, sizeof(krec));


	while( (status = cursor -> c_get(cursor, &krec, &drec, DB_NEXT )) == 0 )
	{
		item = PyString_FromStringAndSize((char *)krec.data,
						  (int)krec.size);
		if (item == NULL) {
			Py_DECREF(list);
			cursor -> c_close(cursor);
			return NULL;
		}
		err = PyList_Append(list, item);
		Py_DECREF(item);
		if (err != 0) {
			Py_DECREF(list);
			cursor -> c_close(cursor);
			return NULL;
		}
		memset(&krec, 0, sizeof(krec));
		memset(&drec, 0, sizeof(krec));
		++n;
	}
	if ( status != DB_NOTFOUND )
	{
		PyErr_SetFromErrno(LibtpError);
		Py_DECREF(list);
		cursor -> c_close(cursor);
		return NULL;
	}
	cursor -> c_close(cursor);
	return list;
}

static PyObject *
libtpdb_has_key(dp, args)
	DbObject *dp;
        PyObject *args;
{
	DBT krec, drec;
	int status;
	char *data;
	int size;
	TxnObject* txn = NULL;
	DB_TXN*	tid = NULL;
	
        check_dbobject_open(dp);
	memset(&krec, 0, sizeof(krec));
	memset(&drec, 0, sizeof(drec));

	if( !ParseKeyTuple(args, &data, &size, &txn) )
		return NULL;

	krec.data = data;
	krec.size = size;
	
	if( txn != NULL )
		tid = txn -> tid;

	krec.data = data;
	krec.size = size;

	status = (dp->db->get)(dp->db, tid, &krec, &drec, 0);
	if (status == DB_NOTFOUND || status == DB_KEYEMPTY)
		status = 1;
	if (status < 0) {
		PyErr_SetFromErrno(LibtpError);
		return NULL;
	}

	return PyInt_FromLong(status == 0);
}

static PyObject *
libtpdb_sync(dp)
	DbObject *dp;
{
	int status;

	check_dbobject_open(dp);
	status = (dp->db->sync)(dp->db, 0);
	if (status != 0) {
		PyErr_SetFromErrno(LibtpError);
		return NULL;
	}
	return PyInt_FromLong(status == 0);
}

extern PyObject*
libtpdb_cursor(DbObject *dp, PyObject* args);	/* in libtpcursor.c */

extern PyObject*
libtpdb_join(PyObject *dp, PyObject *curlist, int flags);	/* in libtpcursor.c */

extern PyObject*
libtpdb_txn(DbObject *dp, PyObject* args);	/* in libtptxn.c */

extern PyObject*
libtpdb_newlock(DbObject *dp, PyObject* args);	/* in libtplock.c */

static PyMethodDef DbMethods[] = {
	{"close",		(PyCFunction)libtpdb_close},
	{"has_key",		(PyCFunction)libtpdb_has_key, METH_VARARGS},
	{"sync",		(PyCFunction)libtpdb_sync},
	{"keys",		(PyCFunction)libtpdb_keys, METH_VARARGS},
	{"txn",			(PyCFunction)libtpdb_txn,	METH_VARARGS},
	{"cursor",		(PyCFunction)libtpdb_cursor, METH_VARARGS},
	{"lock",		(PyCFunction)libtpdb_newlock},
	{"status",		(PyCFunction)libtpdb_status, METH_VARARGS},
	{NULL,	       	NULL}		/* sentinel */
};

static PyObject *
libtpdb_getattr(dp, name)
	PyObject *dp;
        char *name;
{
	return Py_FindMethod(DbMethods, dp, name);
}

static PyMappingMethods libtpdb_as_mapping = {
	(inquiry)libtpdb_length,		/*mp_length*/
	(binaryfunc)libtpdb_subscript,	/*mp_subscript*/
	(objobjargproc)libtpdb_ass_sub	/*mp_ass_subscript*/
};

static PyTypeObject DbType = {
	PyObject_HEAD_INIT(&PyType_Type)
	0,
	"libtpdb",
	sizeof(DbObject),
	0,
	(destructor)libtpdb_dealloc, /*tp_dealloc*/
	0,			/*tp_print*/
	(getattrfunc)libtpdb_getattr, /*tp_getattr*/
	0,			/*tp_setattr*/
	0,			/*tp_compare*/
	0,			/*tp_repr*/
	0,			/*tp_as_number*/
	0,			/*tp_as_sequence*/
	&libtpdb_as_mapping,	/*tp_as_mapping*/
};

/*
*	Env methods
*/

#define	PARSE_INT(key, dest)	\
	if( value = PyDict_GetItemString(opt, key) )	\
		dest = PyInt_AsLong(value);


#define	SET_BIT(dest, bit, val)			\
	{	int _x = (val);			\
		if( _x )			\
			dest |= (bit);		\
		else				\
			dest &= ~(bit);		\
	}

#define	PARSE_BIT(key, dest, bit)			\
	{						\
		if( value = PyDict_GetItemString(opt, key) )	\
			SET_BIT(dest, bit, PyInt_AsLong(value) );	\
	}


static	void	parse_env_dict(PyObject* opt,
	DB_ENV* env,
	int* flags)
{
	PyObject*	value;
	char*		str;

	memset(env, 0, sizeof(*env));
	*flags = 0;

	if( opt == NULL )
		return;

	*flags = DB_INIT_LOCK | DB_INIT_MPOOL | DB_INIT_TXN | DB_THREAD;

	PARSE_BIT("create",	*flags,	DB_CREATE);
	PARSE_BIT("init_cdb",	*flags,	DB_INIT_CDB);
	PARSE_BIT("init_lock",	*flags,	DB_INIT_LOCK);
	PARSE_BIT("init_log",	*flags,	DB_INIT_LOG);
	PARSE_BIT("init_mpool",	*flags,	DB_INIT_MPOOL);
	PARSE_BIT("init_txn",	*flags,	DB_INIT_TXN);
	PARSE_BIT("recover",	*flags,	DB_RECOVER);
	PARSE_BIT("recover_fatal",	*flags,	DB_RECOVER_FATAL);
	PARSE_BIT("thread",	*flags, DB_THREAD);
	PARSE_INT("logmax",	env->lg_max);

	env -> lg_max *= 1024;	/* make it Kilobytes */

	if( value = PyDict_GetItemString(opt, "errfile") )
		env -> db_errfile = fopen(PyString_AsString(value), "a");
}

static void
libtpenv_dealloc(EnvObject* self)
{
#ifdef	DEBUG
	fprintf(stderr, "libtpenv_dealloc()\n");
	fprintf(stderr, "db_appexit...\n");
#endif
	db_appexit(&self->env);
#ifdef	DEBUG
	fprintf(stderr, "done...\n");
#endif
	PyMem_DEL(self);
}

static	void	parse_db_info(PyObject* opt,
	DB_INFO* info)
{
	PyObject*	value;
	char*		str;

	memset(info, 0, sizeof(*info));

	if( opt == NULL )
		return;

	PARSE_INT("pagesize", 	info->db_pagesize);
	PARSE_INT("cachesize", 	info->db_cachesize);
	PARSE_INT("lorder", 	info->db_lorder);
	PARSE_INT("pagesize", 	info->db_pagesize);
	PARSE_INT("minkey", 	info->bt_minkey);
	PARSE_INT("ffactor",  	info->h_ffactor);
	PARSE_INT("nelem",    	info->h_nelem);
	PARSE_INT("reclen", 	info->re_len);

	PARSE_BIT("dup",	info->flags, DB_DUP);
	PARSE_BIT("dupsort",	info->flags, DB_DUPSORT);

}

static DbObject *
newdbobject(type, file, flags, mode, env, info)
	int	type;	/* DB_HASH, DB_BTREE, etc. */
	char *file;
        int flags;
        int mode;
	DB_ENV* env;
	DB_INFO* info;
{
	DbObject *dp;
	char	home[256];
	int	init_flags;
	int	status;

	if ((dp = PyObject_NEW(DbObject, &DbType)) == NULL)
	{
		PyErr_SetFromErrno(LibtpError);
		Py_DECREF(dp);
		return NULL;
	}

	if ( db_open(file, type, flags, mode, env, info,
				   &dp->db)) {
		perror(file);
		PyErr_SetFromErrno(LibtpError);
		Py_DECREF(dp);
		return NULL;
	}

	return dp;
}

static PyObject *
open_db(self, args, type)
	EnvObject *self;
        PyObject *args;
	int	type;
{
	char *file;
	char *flag = NULL;
	int flags = 0; 
	int mode = 0666;
	PyObject 	*env = NULL,
			*opt = NULL;
	DB_INFO		info;
	DbObject	*ptr = NULL;

	memset(&info, 0, sizeof(info));

	/*
	* Parameters are:
	*	filename
	*	flag: 'c','r','w','n'			opt
	*	mode: unix protection mask		opt
	*	db options dictionary
	*/

	if (!PyArg_ParseTuple(args, "s|siOO",
			      &file, &flag, &mode, &opt))
		return NULL;

	/* Parse flag */
	if (flag != NULL) {
		if (flag[0] == 'r')
			flags = DB_RDONLY;
		else if (flag[0] == 'c')
			flags = DB_CREATE;
		else if (flag[0] == 'n')
			flags = DB_CREATE|DB_TRUNCATE;
		else if (flag[0] == 'w')
			flags = 0;
		else {
			PyErr_SetString(LibtpError,
				"Flag should begin with 'r', 'w', 'c' or 'n'");
			return NULL;
		}
	}

	/* Parse db options */
	if( opt != NULL )
		parse_db_info(opt, &info);

	ptr = newdbobject(type, file, flags, mode, &self->env, &info);

	ptr -> env = self;
	if( ptr != NULL )
		Py_INCREF(self);
	return (PyObject*)ptr;
}

static PyObject *
libtpenv_hashopen(self, args)
	EnvObject *self;
        PyObject *args;
{
	return open_db(self, args, DB_HASH);
}

static PyObject *
libtpenv_btreeopen(self, args)
	EnvObject *self;
        PyObject *args;
{
	return open_db(self, args, DB_BTREE);
}

static PyObject *
libtpenv_rnopen(self, args)
	EnvObject *self;
        PyObject *args;
{
	return open_db(self, args, DB_RECNO);
}

static PyObject *
libtpenv_getattr(PyObject* self, char* name);

static PyTypeObject EnvType = {
	PyObject_HEAD_INIT(&PyType_Type)
	0,
	"libtpenv",
	sizeof(EnvObject),
	0,
	(destructor)libtpenv_dealloc, /*tp_dealloc*/
	0,			/*tp_print*/
	(getattrfunc)libtpenv_getattr, /*tp_getattr*/
	0,			/*tp_setattr*/
	0,			/*tp_compare*/
	0,			/*tp_repr*/
	0,			/*tp_as_number*/
	0,			/*tp_as_sequence*/
	0,			/*tp_as_mapping*/
};

static	PyObject*
newEnvObject(PyObject* self, PyObject* args)
{
	/* Args: home, dictionary-optional */
	PyObject*	dict = NULL;
	char		home[256] = ".", 
			*p = NULL;
	EnvObject*	ptr = NULL;
	int		flags = 0;
	int		status = 0;

	if(!PyArg_ParseTuple(args, "|sO", &p, &dict))
		return NULL;

	if ((ptr = PyObject_NEW(EnvObject, &EnvType)) == NULL)
		return NULL;

	if( p != NULL )
		strcpy(home, p);

	if( dict != NULL )
		parse_env_dict(dict, &ptr->env, &flags);

	status = db_appinit(home, NULL, &ptr->env, flags);
	if( status )
	{
		perror("appinit");
		PyErr_SetFromErrno(LibtpError);
		Py_DECREF(ptr);
		return NULL;
	}
	return (PyObject*)ptr;
}

extern PyObject*
libtpenv_newlock(EnvObject *env, PyObject* args);	/* in libtplock.c */

static PyMethodDef EnvMethods[] = {
	{"hashopen",		(PyCFunction)libtpenv_hashopen},
	{"btreeopen",		(PyCFunction)libtpenv_btreeopen},
	{"nropen",		(PyCFunction)libtpenv_rnopen},
	{"lock",		(PyCFunction)libtpenv_newlock},
	{NULL,	       	NULL}		/* sentinel */
};

static PyObject *
libtpenv_getattr(self, name)
	PyObject *self;
        char *name;
{
	return Py_FindMethod(EnvMethods, self, name);
}


static PyMethodDef LibtpModuleMethods[] = {
	{"init",	(PyCFunction)newEnvObject, METH_VARARGS},
	{"join",	(PyCFunction)libtpdb_join, METH_VARARGS},
	{0,		0},
};

void
initlibtp() {
	PyObject *m, *d;

	DbType.ob_type = &PyType_Type;
	m = Py_InitModule("libtp", LibtpModuleMethods);
	d = PyModule_GetDict(m);
	LibtpError = PyErr_NewException("libtp.error", NULL, NULL);
	if (LibtpError != NULL)
		PyDict_SetItemString(d, "error", LibtpError);
}
