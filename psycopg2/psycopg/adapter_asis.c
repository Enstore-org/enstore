/* adapter_asis.c - adapt types as they are
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
#include "psycopg/adapter_asis.h"
#include "psycopg/microprotocols_proto.h"

/** the AsIs object **/

static PyObject *
asis_str(asisObject *self)
{
    if (self->wrapped == Py_None) {
        return PyString_FromString("NULL");
    }
    else {
        return PyObject_Str(self->wrapped);
    }
}

static PyObject *
asis_getquoted(asisObject *self, PyObject *args)
{
    if (!PyArg_ParseTuple(args, "")) return NULL;
    return asis_str(self);
}

static PyObject *
asis_conform(asisObject *self, PyObject *args)
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

/** the AsIs object */

/* object member list */

static struct PyMemberDef asisObject_members[] = {
    {"adapted", T_OBJECT, offsetof(asisObject, wrapped), RO},
    {NULL}
};

/* object method table */

static PyMethodDef asisObject_methods[] = {
    {"getquoted", (PyCFunction)asis_getquoted, METH_VARARGS,
     "getquoted() -> wrapped object value as SQL-quoted string"},
    {"__conform__", (PyCFunction)asis_conform, METH_VARARGS, NULL},
    {NULL}  /* Sentinel */
};

/* initialization and finalization methods */

static int
asis_setup(asisObject *self, PyObject *obj)
{
    Dprintf("asis_setup: init asis object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, ((PyObject *)self)->ob_refcnt
      );

    Py_INCREF(obj);
    self->wrapped = obj;

    Dprintf("asis_setup: good asis object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, ((PyObject *)self)->ob_refcnt
      );
    return 0;
}

static int
asis_traverse(asisObject *self, visitproc visit, void *arg)
{
    Py_VISIT(self->wrapped);
    return 0;
}

static void
asis_dealloc(PyObject* obj)
{
    asisObject *self = (asisObject *)obj;

    Py_CLEAR(self->wrapped);

    Dprintf("asis_dealloc: deleted asis object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        obj, obj->ob_refcnt
      );

    obj->ob_type->tp_free(obj);
}

static int
asis_init(PyObject *obj, PyObject *args, PyObject *kwds)
{
    PyObject *o;

    if (!PyArg_ParseTuple(args, "O", &o))
        return -1;

    return asis_setup((asisObject *)obj, o);
}

static PyObject *
asis_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc(type, 0);
}

static void
asis_del(PyObject* self)
{
    PyObject_GC_Del(self);
}

static PyObject *
asis_repr(asisObject *self)
{
    return PyString_FromFormat("<psycopg2._psycopg.AsIs object at %p>", self);
}


/* object type */

#define asisType_doc \
"AsIs(str) -> new AsIs adapter object"

PyTypeObject asisType = {
    PyObject_HEAD_INIT(NULL)
    0,
    "psycopg2._psycopg.AsIs",
    sizeof(asisObject),
    0,
    asis_dealloc, /*tp_dealloc*/
    0,          /*tp_print*/

    0,          /*tp_getattr*/
    0,          /*tp_setattr*/

    0,          /*tp_compare*/

    (reprfunc)asis_repr, /*tp_repr*/
    0,          /*tp_as_number*/
    0,          /*tp_as_sequence*/
    0,          /*tp_as_mapping*/
    0,          /*tp_hash */

    0,          /*tp_call*/
    (reprfunc)asis_str, /*tp_str*/

    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/

    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE|Py_TPFLAGS_HAVE_GC, /*tp_flags*/
    asisType_doc, /*tp_doc*/

    (traverseproc)asis_traverse, /*tp_traverse*/
    0,          /*tp_clear*/

    0,          /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/

    0,          /*tp_iter*/
    0,          /*tp_iternext*/

    /* Attribute descriptor and subclassing stuff */

    asisObject_methods, /*tp_methods*/
    asisObject_members, /*tp_members*/
    0,          /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/

    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/

    asis_init, /*tp_init*/
    0, /*tp_alloc  will be set to PyType_GenericAlloc in module init*/
    asis_new, /*tp_new*/
    (freefunc)asis_del, /*tp_free  Low-level free-memory routine */
    0,          /*tp_is_gc For PyObject_IS_GC */
    0,          /*tp_bases*/
    0,          /*tp_mro method resolution order */
    0,          /*tp_cache*/
    0,          /*tp_subclasses*/
    0           /*tp_weaklist*/
};


/** module-level functions **/

PyObject *
psyco_AsIs(PyObject *module, PyObject *args)
{
    PyObject *obj;

    if (!PyArg_ParseTuple(args, "O", &obj))
        return NULL;

    return PyObject_CallFunction((PyObject *)&asisType, "O", obj);
}
