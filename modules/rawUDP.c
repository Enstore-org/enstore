/******************************************************************************
 * High speed udp server and supprting functions
 *
 ******************************************************************************/

/* $Id: */

#include <sys/types.h>
#include <sys/socket.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>
#include "Python.h"

#define MAX_PACKET_SIZE 16384
#define RCV_TIMEOUT 60.
#define DEBUG 0

extern int errno;


struct list *_create_list(int);
void _receiver(void *arg);


static PyObject * raise_exception(char *msg);
static PyObject * get(PyObject *self, PyObject *args);
static PyObject * create_list(PyObject *self, PyObject *args);
static PyObject * delete(PyObject *self, PyObject *args);
static PyObject * receiver(PyObject *self, PyObject *args);
void initrawUDP(void);


struct client_addr 
{
    char *addr;
    int port;
};

struct msg 
{
    struct client_addr client;
    char *message;
};

struct node 
{
    struct msg *el;
    struct node *next;
};

struct list
{
    int port;
    char *inbuf;
    char *outbuf;
    struct node *head;
    struct node *tail;
    pthread_mutex_t *lock;
    size_t size;
};

#define dprintf if (DEBUG) (void)printf

struct list *_create_list(int port)
{
    struct list *lst = (struct list *) malloc(sizeof(struct list));
    pthread_mutex_t  *lock = (pthread_mutex_t *) malloc(sizeof(pthread_mutex_t));

    if (lst != NULL) {
	lst->head = NULL;
	lst->tail = NULL;
	lst->size = 0;
	pthread_mutex_init(lock, NULL);
	lst->port = port;
	lst->inbuf = (char *) malloc(MAX_PACKET_SIZE);
	lst->outbuf = (char *) malloc(MAX_PACKET_SIZE);
	lst->lock = lock;
    }
    return lst;
};


struct node *_new_node(struct list *lst, size_t msg_len)
{
    struct node *new;
    struct node *rc;

    //pthread_mutex_lock(lst->lock);
    dprintf("_new_node %d\n", msg_len);
    new = (struct node *) malloc(sizeof(struct node));
    dprintf("_new_node %d\n", 1);
    if (new == NULL) {
	rc = NULL;
    }
    rc = new;
    dprintf("_new_node %d\n", 1);
    new->el = (struct msg *) malloc(sizeof(struct msg));
    dprintf("_new_node %d\n", 2);
    if (new->el == NULL) {
	rc = NULL;
    }
    new->el->message = (void *) malloc(msg_len+1);
    dprintf("_new_node %d\n", 2);
    if (new->el->message == NULL) {
	rc = NULL;
    }
    memset(new->el->message, 0, msg_len+1); 
    new->next = NULL;
    lst->tail = new;
    if (lst->head == NULL) {
	lst->head = lst->tail;
    }
    dprintf("_new_node exit\n");
    //pthread_mutex_unlock(lst->lock);
    
    return rc;
}
/*
char *_get(struct list *lst)
{
    struct msg *ret;
    if (lst->head == NULL)
    {
	ret = NULL;
    }
    else
    {
	ret = lst->head->el;
	lst->head = lst->head->next;
    }
    //dprintf("get: %s", (char *)ret);
    
    return ret;
}
*/
void _delete(struct list *lst, struct node *n)
{
    //pthread_mutex_lock(lst->lock);
    if (n != NULL) {
	if (n->el != NULL) {
	    if (n->el->message != NULL)
		free(n->el->message);
	    free(n->el);
	}
	free(n);
    }
    //pthread_mutex_unlock(lst->lock);

}


void _receiver(void *arg)
{
    ssize_t rcvd;
    int flags=0;
    //struct msg request;
    struct node *n;
    static char buf[MAX_PACKET_SIZE];
    struct sockaddr_in s_in, client;
    int sock, slen=sizeof(s_in);
    struct list *lst;

    dprintf("IN receiver %d\n",(int)arg);
    lst = (struct lst *) arg; 

    if ((sock=socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP))==-1) {
	dprintf("can not create socket\n");
	return;
    }
    dprintf ("PORT %d\n",lst->port);
    memset((char *) &s_in, 0, slen);
    s_in.sin_family = AF_INET;
    //s_in.sin_port = htons(port);
    s_in.sin_port = htons(lst->port);
    s_in.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(sock, &s_in, slen)==-1) {
	perror("_receiver:");
	if (errno != EADDRINUSE) {
	    dprintf("can not bind socket\n");
	    return;
	}
    }
    while (1) {
	dprintf("receiver: waiting\n");
	rcvd = recvfrom(sock, &buf, MAX_PACKET_SIZE, flags, &client, &slen);
	dprintf("receiver: got it %d\n", rcvd);

	if (rcvd > 0) {
	    dprintf("_receiver %d\n",rcvd);
	    pthread_mutex_lock(lst->lock);
	    n = _new_node(lst, rcvd);
	    dprintf("new node %d\n",n);
	    if (n == NULL) {
		dprintf("No memory\n");
		break;
	    }
	    //pthread_mutex_lock(lst->lock);

	    memcpy(n->el->message, &buf, rcvd);
	    dprintf("RCVD %s\n",n->el->message);
	    n->el->client.addr = inet_ntoa(client.sin_addr);
	    n->el->client.port = ntohs(client.sin_port);
	    dprintf("from %s %d\n",n->el->client.addr, n->el->client.port);  
	    lst->size++;
	    pthread_mutex_unlock(lst->lock);

	    //dprintf("from %s %d\n",n->el->client.addr, n->el->client.port);  
	}
	else if (rcvd == -1)
	    perror("receive");
    }
}

/*
int main(void){
    dprintf("%d %d %d\n",sizeof(struct sockaddr), sizeof(socklen_t), sizeof(struct msg));
}

*/
/* Python API
*/

static char fmt[]="O";

static char rawUDP_Doc[] = "raw UDP module";
static char get_Doc[] = "get(void *list)";
static char create_list_Doc[] = "void *create_list()";
static char delete_Doc[] = "delete(void *node)";
static char receiver_Doc[] = "receiver(int socket, void *list)";

/*  Module Methods table. 
 *
 *  There is one entry with four items for for each method in the module
 *
 *  Entry 1 - the method name as used  in python
 *        2 - the c implementation function
 *        3 - flags 
 *	  4 - method documentation string
 */

static PyMethodDef rawUDP_Methods[] = {
    { "get",  get,  1, get_Doc},
    { "create_list", create_list, 1, create_list_Doc},
    { "delete", delete, 1, delete_Doc},
    { "receiver", receiver, 1, receiver_Doc},
    { 0, 0}        /* Sentinel */
};


static PyObject *ErrObject;
static PyObject *
raise_exception(char *msg)
{
        PyObject	*v;
        int		i = errno;

#   ifdef EINTR
    if ((i==EINTR) && PyErr_CheckSignals()) return NULL;
#   endif

    /* note: format should be the same as in FTT.c */
    v = Py_BuildValue("(s,i,s,i)", msg, i, strerror(i), getpid());
    if (v != NULL)
    {   PyErr_SetObject(ErrObject, v);
	Py_DECREF(v);
    }
    return NULL;
}

static PyObject * get(PyObject *self, PyObject *args)
{
    PyObject * p;
    struct list *lst;
    struct msg *ret;
    struct node *cur;
    int status;
    PyObject	*v; 
 
    //dprintf("get\n");
    status = PyArg_ParseTuple(args, fmt, &p);
    //dprintf("get: status %d\n", status);
    if (!status){
	return(raise_exception("invalid parameter"));
    }
    lst = PyCObject_AsVoidPtr(p);
    
    if (lst->head == NULL){
	Py_INCREF(Py_None);
	return Py_None;
    }

    pthread_mutex_lock(lst->lock);
    cur = lst->head;
    ret = cur->el;
    if (lst->size > 0)
      lst->size--;
    dprintf("ret:  client [%s %d] message %s\n", ret->client.addr, ret->client.port, ret->message);
    v = Py_BuildValue("sisi", ret->client.addr, 
		      ret->client.port, 
		      ret->message,
		      lst->size);
    lst->head = lst->head->next;

    _delete(lst, cur);
     pthread_mutex_unlock(lst->lock);

    return v;
}
/*
static PyObject * get(PyObject *self, PyObject *args)
{
    PyObject * p;
    struct list *lst;
    void *ret;
    int status;
 
    dprintf("get\n");
    status = PyArg_ParseTuple(args, fmt, &p);
    dprintf("get: status %d\n", status);

    lst = PyCObject_AsVoidPtr(p);
    if (!status)
	{
	    dprintf("convert returned %d\n", status);
	    return(NULL);
	}
    
    ret = _get(lst);
    dprintf("get %d\n", ret);
    if (ret)
	return PyLong_AsVoidPtr(ret);
    else
	return Py_None;
}
*/

static PyObject * create_list(PyObject *self, PyObject *args)
{

    int port, status;
    struct list *lst;
    PyObject	*v; 
   
    dprintf("rawUDP: create list\n");
    status = PyArg_ParseTuple(args, "i", &port);
    if (!status) {
	return(raise_exception("invalid parameter"));
    }
    lst = _create_list(port);
    dprintf("rawUDP: create list done %i\n", lst);
    if (lst) {
	v = PyCObject_FromVoidPtr((void *) lst, NULL);
	dprintf("rawUDP: value created\n");
	return v;
    }
    else
	return NULL;
}


static PyObject * delete(PyObject *self, PyObject *args)
{
    PyObject *p,*l;
    struct node *n;
    struct list *lst;
    int status;

    status = PyArg_ParseTuple(args, fmt, &l,&p);
    if (!status) {
	return(raise_exception("invalid parameter"));
    }
    n = PyCObject_AsVoidPtr(p);
    lst = PyCObject_AsVoidPtr(l);
    pthread_mutex_lock(lst->lock);

    _delete(lst, n);
    if (lst->size > 0)
      lst->size--;
    pthread_mutex_unlock(lst->lock);
    return(NULL);
}
    
static PyObject * receiver(PyObject *self, PyObject *args)
{
    PyObject * p;
    struct list *lst;
    int status;
    pthread_t tid;
    //pthread_attr_t attr;
    int rc;

    dprintf("starting receiver\n");
    //status = PyArg_ParseTuple(args, "iO", &port, &p);
    status = PyArg_ParseTuple(args, "O", &p);
    if (!status) {
	return(raise_exception("invalid parameter"));
    }

    lst = PyCObject_AsVoidPtr(p); 
    dprintf("GOT PORT %d\n", lst->port);
    //pthread_attr_init(&attr);
    //pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    
    rc = pthread_create(&tid, 
			NULL, 
			_receiver, 
			(void *) lst);

    //_receiver(port, lst);
    dprintf("receiver started\n");
    Py_INCREF(Py_None);
    return Py_None;

    
}

/***************************************************************************
 * inititalization
 **************************************************************************
 *   Module initialization.   Python call the entry point init<module name>
 *   when the module is imported.  This should the only non-static entry point
 *   so it is exported to the linker.
 *
 *   First argument must be a the module name string.
 *   
 *   Second       - a list of the module methods
 *
 *   Third	- a doumentation string for the module
 * 
 *   Fourth & Fifth - see Python/modsupport.c
 */
	    
void initrawUDP()
{
    PyObject	*module, *dict;
    
    module = Py_InitModule4("rawUDP", rawUDP_Methods, rawUDP_Doc, 
		       (PyObject*)NULL, PYTHON_API_VERSION);
    dict = PyModule_GetDict(module);
    ErrObject = PyErr_NewException("rawUDP.error:", NULL, NULL);
    /*
    EXErrObject = PyErr_NewException("EXfer.error", NULL, NULL);
    if (EXErrObject != NULL)
	PyDict_SetItemString(d,"error",EXErrObject);
    */
    /* add methods to class */
    /*
    for (def = rawUDPMethods; def->ml_name != NULL; def++) {
	PyObject *func = PyCFunction_New(def, NULL);
	PyObject *method = PyMethod_New(func, NULL, fooClass);
	PyDict_SetItemString(classDict, def->ml_name, method);
	Py_DECREF(func);
	Py_DECREF(method);
    }
    */
}
