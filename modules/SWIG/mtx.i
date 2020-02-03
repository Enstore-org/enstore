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

%include pointer.i
// This tells SWIG to treat char ** as a special case
%typemap(python,in) char ** {
  /* Check if is a list */
  if (PyList_Check($source)) {
    int size = PyList_Size($source);
    int i = 0;
    $target = (char **) malloc((size+1)*sizeof(char *));
    for (i = 0; i < size; i++) {
      PyObject *o = PyList_GetItem($source,i);
      if (PyString_Check(o))
$target[i] = PyString_AsString(PyList_GetItem($source,i));
      else {
PyErr_SetString(PyExc_TypeError,"list must contain strings");
free($target);
return NULL;
      }
    }
    $target[i] = 0;
  } else {
    PyErr_SetString(PyExc_TypeError,"not a list");
    return NULL;
  }
}
// This cleans up the char ** array we malloc’d before the function call
%typemap(python,freearg) char ** {
  free((char *) $source);
 }
// This allows a C function to return a char ** as a Python list
%typemap(python,out) char ** {
  int len,i;
  len = 0;
  while ($source[len]) len++;
  $target = PyList_New(len);
  for (i = 0; i < len; i++) {
    PyList_SetItem($target,i,PyString_FromString($source[i]));
  }
}

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
