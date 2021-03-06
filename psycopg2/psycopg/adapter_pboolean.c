/* adapter_pboolean.c - psycopg boolean type wrapper implementation
 *
 * Copyright (C) 2003-2004 Federico Di Gregorio <fog@debian.org>
 *
 * This file is part of psycopg.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>
#include <stringobject.h>
#include <string.h>

#define PSYCOPG_MODULE
#include "psycopg/config.h"
#include "psycopg/python.h"
#include "psycopg/psycopg.h"
#include "psycopg/adapter_pboolean.h"
#include "psycopg/microprotocols_proto.h"


/** the Boolean object **/

static PyObject *
pboolean_str(pbooleanObject *self)
{
#ifdef PSYCOPG_NEW_BOOLEAN
    if (PyObject_IsTrue(self->wrapped)) {
        return PyString_FromString("true");
    }
    else {
        return PyString_FromString("false");
    }
#else
    if (PyObject_IsTrue(self->wrapped)) {
        return PyString_FromString("'t'");
    }
    else {
        return PyString_FromString("'f'");
    }
#endif
}

static PyObject *
pboolean_getquoted(pbooleanObject *self, PyObject *args)
{
    if (!PyArg_ParseTuple(args, "")) return NULL;
    return pboolean_str(self);
}

static PyObject *
pboolean_conform(pbooleanObject *self, PyObject *args)
{
    PyObject *res, *proto;

    if (!PyArg_ParseTuple(args, "O", &proto)) return NULL;

    if (proto == (PyObject*)&isqlquoteType)
        res = (PyObject*)self;
    else
        res = Py_None;

    Py_INCREF(res);
    return res;
}

/** the Boolean object */

/* object member list */

static struct PyMemberDef pbooleanObject_members[] = {
    {"adapted", T_OBJECT, offsetof(pbooleanObject, wrapped), RO},
    {NULL}
};

/* object method table */

static PyMethodDef pbooleanObject_methods[] = {
    {"getquoted", (PyCFunction)pboolean_getquoted, METH_VARARGS,
     "getquoted() -> wrapped object value as SQL-quoted string"},
    {"__conform__", (PyCFunction)pboolean_conform, METH_VARARGS, NULL},
    {NULL}  /* Sentinel */
};

/* initialization and finalization methods */

static int
pboolean_setup(pbooleanObject *self, PyObject *obj)
{
    Dprintf("pboolean_setup: init pboolean object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, ((PyObject *)self)->ob_refcnt
      );

    Py_INCREF(obj);
    self->wrapped = obj;

    Dprintf("pboolean_setup: good pboolean object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, ((PyObject *)self)->ob_refcnt
      );
    return 0;
}

static int
pboolean_traverse(PyObject *obj, visitproc visit, void *arg)
{
    pbooleanObject *self = (pbooleanObject *)obj;

    Py_VISIT(self->wrapped);
    return 0;
}

static void
pboolean_dealloc(PyObject* obj)
{
    pbooleanObject *self = (pbooleanObject *)obj;

    Py_CLEAR(self->wrapped);

    Dprintf("pboolean_dealloc: deleted pboolean object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        obj, obj->ob_refcnt
      );

    obj->ob_type->tp_free(obj);
}

static int
pboolean_init(PyObject *obj, PyObject *args, PyObject *kwds)
{
    PyObject *o;

    if (!PyArg_ParseTuple(args, "O", &o))
        return -1;

    return pboolean_setup((pbooleanObject *)obj, o);
}

static PyObject *
pboolean_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc(type, 0);
}

static void
pboolean_del(PyObject* self)
{
    PyObject_GC_Del(self);
}

static PyObject *
pboolean_repr(pbooleanObject *self)
{
    return PyString_FromFormat("<psycopg2._psycopg.Boolean object at %p>",
                                self);
}


/* object type */

#define pbooleanType_doc \
"Boolean(str) -> new Boolean adapter object"

PyTypeObject pbooleanType = {
    PyObject_HEAD_INIT(NULL)
    0,
    "psycopg2._psycopg.Boolean",
    sizeof(pbooleanObject),
    0,
    pboolean_dealloc, /*tp_dealloc*/
    0,          /*tp_print*/

    0,          /*tp_getattr*/
    0,          /*tp_setattr*/

    0,          /*tp_compare*/

    (reprfunc)pboolean_repr, /*tp_repr*/
    0,          /*tp_as_number*/
    0,          /*tp_as_sequence*/
    0,          /*tp_as_mapping*/
    0,          /*tp_hash */

    0,          /*tp_call*/
    (reprfunc)pboolean_str, /*tp_str*/

    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/

    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE|Py_TPFLAGS_HAVE_GC, /*tp_flags*/
    pbooleanType_doc, /*tp_doc*/

    pboolean_traverse, /*tp_traverse*/
    0,          /*tp_clear*/

    0,          /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/

    0,          /*tp_iter*/
    0,          /*tp_iternext*/

    /* Attribute descriptor and subclassing stuff */

    pbooleanObject_methods, /*tp_methods*/
    pbooleanObject_members, /*tp_members*/
    0,          /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/

    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/

    pboolean_init, /*tp_init*/
    0, /*tp_alloc  will be set to PyType_GenericAlloc in module init*/
    pboolean_new, /*tp_new*/
    (freefunc)pboolean_del, /*tp_free  Low-level free-memory routine */
    0,          /*tp_is_gc For PyObject_IS_GC */
    0,          /*tp_bases*/
    0,          /*tp_mro method resolution order */
    0,          /*tp_cache*/
    0,          /*tp_subclasses*/
    0           /*tp_weaklist*/
};


/** module-level functions **/

PyObject *
psyco_Boolean(PyObject *module, PyObject *args)
{
    PyObject *obj;

    if (!PyArg_ParseTuple(args, "O", &obj))
        return NULL;

    return PyObject_CallFunction((PyObject *)&pbooleanType, "O", obj);
}
