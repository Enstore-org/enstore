/*  This file (IPC.h) was created by Ron Rechenmacher <ron@fnal.gov> on
    Nov 10, 1998. "TERMS AND CONDITIONS" governing this file are in the README
    or COPYING file. If you do not have such a file, one can be obtained by
    contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
    $RCSfile$
    $Revision$
    $Date$
    */


struct s_IPCshmgetObject {
    PyObject_HEAD
    int	id;
    int	*i_p;
    int	size_bytes;
};

extern PyTypeObject IPCshmget_Type;
