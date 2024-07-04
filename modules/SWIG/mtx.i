%module mtx
%{
  char *device;
  int absolute_addressing;

typedef struct {
        int num_args;
        char *name;
        void (*command)(void);
        int need_device;
        int need_status;
} command_table_struct;
command_table_struct command_table[22];

%}

%include cpointer.i
// This tells SWIG to treat char ** as a special case
#if defined(SWIGPYTHON)
%typemap(in) char ** {
  /* Check if is a list */
  if (PyList_Check($input)) {
    int size = PyList_Size($input);
    int i = 0;
    $1 = (char **) malloc((size+1)*sizeof(char *));
    for (i = 0; i < size; i++) {
      PyObject *o = PyList_GetItem($input,i);
      if (PyString_Check(o))
         $1[i] = PyString_AsString(PyList_GetItem($input,i));
      else {
         PyErr_SetString(PyExc_TypeError,"list must contain strings");
         free($1);
         return NULL;
      }
    }
    $1[i] = 0;
  } else {
    PyErr_SetString(PyExc_TypeError,"not a list");
    return NULL;
  }
}
#endif

// This cleans up the char ** array we malloc’d before the function call
#if defined(SWIGPYTHON)
%typemap(freearg) char ** {
  free((char *) $1);
 }
#endif

#if defined(SWIGPYTHON)
// This allows a C function to return a char ** as a Python list
%typemap(out) char ** {
  int len,i;
  len = 0;
  while ($1[len]) len++;
  $result = PyList_New(len);
  for (i = 0; i < len; i++) {
    PyList_SetItem($result,i,PyString_FromString($1[i]));
  }
}
#endif

%inline %{
 void status()
{
  extern command_table_struct command_table[22];
  execute_command(&command_table[1]);
  return;
}

%}

char *device;
int absolute_addressing;
command_table_struct command_table[22];
void execute_command(struct command_table_struct *command);
void open_device(void);
void Move(int src, int dest);
void set_scsi_timeout(int timeout);
int get_scsi_timeout();
void Test_UnitReady(void);
