#ifndef	_LIBTP_H_
#define	_LIBTP_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <db.h>

typedef struct
{
	PyObject_HEAD
	DB_ENV	env;
} EnvObject;

typedef struct {
	PyObject_HEAD
	DB *db;
	EnvObject*	env;
} DbObject;

typedef struct {
	PyObject_HEAD
	DbObject* db;
	DB_TXN *tid;
} TxnObject;

typedef struct {
	PyObject_HEAD
	DBC*		cursor;
	DbObject* 	db;
	TxnObject*	txn;
} CurObject;

typedef struct {
	PyObject_HEAD
	DB_LOCK		lock;
	EnvObject*	env;
} LockObject;

#define is_dbobject(v) ((v)->ob_type == &DbType)
#define check_dbobject_open(v) if ((v)->db == NULL) \
               { PyErr_SetString(LibtpError, "LIBTP DB object has already been closed"); \
                 return NULL; }

extern PyObject* LibtpError;
#endif	/*	_LIBTP_H_	*/

