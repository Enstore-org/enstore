#include <Python.h>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <string.h>
#include <sys/stat.h>
#include <limits.h>
#include <errno.h>

/* A little hack for SunOS to use the BSD SIOC* ioctl(). */
#if defined(__sun) && !defined(BSD_COMP)
#  define BSD_COMP
#endif

#include <sys/socket.h>
#include <net/if.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/ioctl.h>


/***************************************************************************
 * globals and constants
 **************************************************************************/

/* Not all systems define these constants. */
#ifndef INET_ADDRSTRLEN
#define INET_ADDRSTRLEN 16
#endif
#ifndef INET6_ADDRSTRLEN
#define INET6_ADDRSTRLEN 46
#endif

/***************************************************************************
 * prototypes
 **************************************************************************/

void initInterfaces(void);
static PyObject * raise_exception(char *msg);
static struct ifconf * interfaces(void);
static PyObject * Interfaces_info(PyObject *self, PyObject *args);
static PyObject * interfacesGet(void);

static PyObject *InterfacesErrObject;

static char Interfaces_Doc[] =  "Interfaces accesses NICs.";

static char Interfaces_get_Doc[] = "\
fd_xfer(fr_fd, to_fd, no_bytes, blk_siz, crc_flag[, crc])";

/*  Module Methods table. 
 *
 *  There is one entry with four items for for each method in the module
 *
 *  Entry 1 - the method name as used  in python
 *        2 - the c implementation function
 *        3 - flags 
 *	  4 - method documentation string
 */

static PyMethodDef Interfaces_Methods[] = {
    { "interfacesGet", Interfaces_info,  1, Interfaces_get_Doc},
    { 0, 0}        /* Sentinel */
};

/***************************************************************************
 * python defined functions
 **************************************************************************/

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
    {   PyErr_SetObject(InterfacesErrObject, v);
	Py_DECREF(v);
    }
    return NULL;
}

#if 0
static PyObject *
raise_exception2(struct transfer *rtn_val)
{
    PyObject	*v;
    int		i = rtn_val->errno_val;
  
#   ifdef EINTR
    if ((i==EINTR) && PyErr_CheckSignals()) return NULL;
#   endif

    /* note: format should be the same as in FTT.c */
    /* What does the above comment mean??? */
    v = Py_BuildValue("(s,i,s,i,O,O,O,s,i)",
		      rtn_val->msg, i, strerror(i), getpid(),
		      PyLong_FromLongLong(rtn_val->bytes),
		      PyFloat_FromDouble(rtn_val->transfer_time),
		      PyFloat_FromDouble(rtn_val->transfer_time),
		      rtn_val->filename, rtn_val->line);
    if (v != NULL)
    {   PyErr_SetObject(EXErrObject, v);
	Py_DECREF(v);
    }
    return NULL;
}
#endif /* 0 */

static PyObject *
Interfaces_info(PyObject *self, PyObject *args)
{
  int sts;
  
  /* Get the parameter. */
  sts = PyArg_ParseTuple(args, "", NULL);
  if (!sts)
     return(raise_exception("interfaces info - invalid parameter"));

  errno = 0;
  return interfacesGet();

#if 0
  if(errno == 0)
     return PyLong_FromUnsignedLong(crc);
  else
     return raise_exception("interfaces info - error");
#endif
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

void
initInterfaces()
{
    PyObject	*m, *d;
    
    m = Py_InitModule4("Interfaces", Interfaces_Methods, Interfaces_Doc, 
		       (PyObject*)NULL, PYTHON_API_VERSION);
    d = PyModule_GetDict(m);
    InterfacesErrObject = PyErr_NewException("Interfaces.error", NULL, NULL);
    if (InterfacesErrObject != NULL)
	PyDict_SetItemString(d, "error", InterfacesErrObject);
}



static struct ifconf * interfaces(void)
{
   int sock;
   void *buf;
   int guess, lastlen = 0;
   struct ifconf *intf_confs;

   intf_confs = malloc(sizeof(struct ifconf));
   if(intf_confs == NULL)
   {
      return NULL;
   }
   
   guess = 100 * sizeof(struct ifreq);

   if((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
   {
      free(intf_confs);
      return NULL;
   }

   for( ; ; )
   {
      if((buf = malloc(guess)) == NULL)
      {
	 free(intf_confs);
	 (void) close(sock);
	 return NULL;
      }

      intf_confs->ifc_len = guess;
      intf_confs->ifc_buf = buf;
      if(ioctl(sock, SIOCGIFCONF, intf_confs) < 0)
      {
	 if(errno != EINVAL || lastlen != 0)
	 {
	    free(intf_confs);
	    free(buf);
	    (void) close(sock);
	    return NULL;
	 }
      }
      else
      {
	 if(intf_confs->ifc_len == lastlen)
	 {
	    break;
	 }
	 else
	    lastlen = intf_confs->ifc_len;
      }
      guess += 10 * sizeof(struct ifreq);
      free(buf);
   }

   (void) close(sock);
   return intf_confs;
}

static int get_ip_addr(char *intf, char* ip)
{
   int sock;
   struct ifreq if_info;
   struct sockaddr_in ip_info;

   if((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
   {
      return -1;
   }

   /* Need to copy the interface name so the kernel knows what interface
    * we are asking about. */
   strncpy(if_info.ifr_name, intf, IFNAMSIZ);
   
   if(ioctl(sock, SIOCGIFADDR, &if_info) < 0)
   {
      (void) close(sock);
      return -1;
   }

   memcpy(&ip_info, &(if_info.ifr_addr), INET_ADDRSTRLEN);
   memcpy(ip, inet_ntoa(ip_info.sin_addr), 16);  /* No defined errors? */

   (void) close(sock);
   return 0;
   
}

static int get_brd_addr(char *intf, char* brd_ip)
{
   int sock;
   struct ifreq if_info;
   struct sockaddr_in brd_info;

   if((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
   {
      return -1;
   }

   /* Need to copy the interface name so the kernel knows what interface
    * we are asking about. */
   strncpy(if_info.ifr_name, intf, IFNAMSIZ);
   
   if(ioctl(sock, SIOCGIFBRDADDR, &if_info) < 0)
   {
      (void) close(sock);
      return -1;
   }

   memcpy(&brd_info, &(if_info.ifr_addr), INET_ADDRSTRLEN);
   memcpy(brd_ip, inet_ntoa(brd_info.sin_addr), 16);  /* No defined errors? */

   (void) close(sock);
   return 0;
   
}

static int get_netmask(char *intf, char* nm)
{
   int sock;
   struct ifreq if_info;
   struct sockaddr_in nm_info;

   if((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
   {
      return -1;
   }

   /* Need to copy the interface name so the kernel knows what interface
    * we are asking about. */
   strncpy(if_info.ifr_name, intf, IFNAMSIZ);
   
   if(ioctl(sock, SIOCGIFNETMASK, &if_info) < 0)
   {
      (void) close(sock);
      return -1;
   }

   memcpy(&nm_info, &(if_info.ifr_broadaddr), INET_ADDRSTRLEN);
   memcpy(nm, inet_ntoa(nm_info.sin_addr), 16);  /* No defined errors? */

   (void) close(sock);
   return 0;
   
}

static int get_hw_addr(char *intf, char* hw)
{
   int sock;
   struct ifreq if_info;
#ifdef SIOCRPHYSADDR
   struct ifdevea pa_info;  /* OSF1? */
#endif
   struct sockaddr hw_info;

   if((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
   {
      return -1;
   }

   /* Need to copy the interface name so the kernel knows what interface
    * we are asking about. */
   strncpy(if_info.ifr_name, intf, IFNAMSIZ);

#ifdef SIOCGIFHWADDR  /* Linux */
   if(ioctl(sock, SIOCGIFHWADDR, &if_info) < 0)
#elif defined(SIOCGENADDR) /* SunOS, IRIX */
   if(ioctl(sock, SIOCGENADDR, &if_info) < 0)
#elif defined(SIOCRPHYSADDR)
   if(ioctl(sock, SIOCRPHYSADDR, &pa_info) < 0) /* OSF1? */
#endif
   {
      (void) close(sock);
      return -1;
   }

#ifdef SIOCRPHYSADDR
   memcpy(&hw_info, &(pa_info.current_pa), INET_ADDRSTRLEN); /* OSF1? */
#elif defined(SIOCGENADDR)
   memcpy(&hw_info, &(if_info.ifr_enaddr), INET_ADDRSTRLEN);/* SunOS, IRIX */
#else 
   memcpy(&hw_info, &(if_info.ifr_hwaddr), INET_ADDRSTRLEN);
#endif
   sprintf(hw, "%02hhX:%02hhX:%02hhX:%02hhX:%02hhX:%02hhX",
	   ((char*)(&(hw_info.sa_data)))[0],
	   ((char*)(&(hw_info.sa_data)))[1],
	   ((char*)(&(hw_info.sa_data)))[2],
	   ((char*)(&(hw_info.sa_data)))[3],
	   ((char*)(&(hw_info.sa_data)))[4],
	   ((char*)(&(hw_info.sa_data)))[5]
      );

   (void) close(sock);
   return 0;
   
}      


PyObject * interfacesGet(void)
{
   struct ifconf *intf_confs;
   int number_of_items;
   int i;
   PyObject *interface_list;
   PyObject *interface_dict;
   char buffer[1024]; /* better be big enough... */
   
   intf_confs = interfaces();

   interface_list = PyDict_New();

   if(intf_confs == NULL)
      return interface_list;
   
   number_of_items = intf_confs->ifc_len / sizeof(struct ifreq);

   for(i = 0; i < number_of_items; i++)
   {
      interface_dict = PyDict_New(); /* Get a new dict for each iteration. */

      /*Add the interface name to the dictionary.  Even though it will
	also be the key in the interface_list dictionary, include it here
	for completeness. */
      PyDict_SetItemString(interface_dict, "interface",
	 PyString_FromString(intf_confs->ifc_req[i].ifr_name));

      /* Add ip address, hw address, etc to the dictionary here. */

      if(get_ip_addr(intf_confs->ifc_req[i].ifr_name, buffer) >= 0)
      {
	 PyDict_SetItemString(interface_dict, "ip",
			      PyString_FromString(buffer));
      }

      if(get_brd_addr(intf_confs->ifc_req[i].ifr_name, buffer) >= 0)
      {
	 PyDict_SetItemString(interface_dict, "broadcast",
			      PyString_FromString(buffer));
      }

      if(get_netmask(intf_confs->ifc_req[i].ifr_name, buffer) >= 0)
      {
	 PyDict_SetItemString(interface_dict, "netmask",
			      PyString_FromString(buffer));
      }

      if(get_hw_addr(intf_confs->ifc_req[i].ifr_name, buffer) >= 0)
      {
	 PyDict_SetItemString(interface_dict, "hwaddr",
			      PyString_FromString(buffer));
      }
      
      PyDict_SetItemString(interface_list, intf_confs->ifc_req[i].ifr_name,
			   interface_dict);
   }

   free(intf_confs->ifc_buf);
   free(intf_confs);
   return interface_list;
}
