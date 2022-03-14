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
#include <net/if_arp.h>
#include <netdb.h>


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
static PyObject * Arp_info(PyObject *self, PyObject *args);

static PyObject * interfacesGet(void);
static PyObject * arpGet(char*);

static PyObject *InterfacesErrObject;

static char Interfaces_Doc[] =  "Interfaces accesses NICs.";
/*static char Arp_Doc[] =  "Interfaces accesses ARP.";*/

static char Interfaces_get_Doc[] = "\
fd_xfer(fr_fd, to_fd, no_bytes, blk_siz, crc_flag[, crc])";
static char Arp_get_Doc[] = "\
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
    { "arpGet", Arp_info,  1, Arp_get_Doc},
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
}

static PyObject *
Arp_info(PyObject *self, PyObject *args)
{
  int sts;
  char *address; 
  
  /* Get the parameter. */
  sts = PyArg_ParseTuple(args, "s", &address);
  if (!sts)
     return(raise_exception("arp info - invalid parameter"));

  errno = 0;
  return arpGet(address);
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

/***************************************************************************
 * Functions for interfaces.
 **************************************************************************/

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
#elif defined(SIOCRPHYSADDR) /* OSF1? */
   if(ioctl(sock, SIOCRPHYSADDR, &pa_info) < 0)
#elif defined(SIOCGIFADDR) /* MacOS X */
   if(ioctl(sock, SIOCGIFADDR, &if_info) < 0)
#endif
   /* If none of the #ifdefs are true, then we close the socket anyway. */
   {
      (void) close(sock);
      return -1;
   }

#ifdef SIOCRPHYSADDR
   memcpy(&hw_info, &(pa_info.default_pa), INET_ADDRSTRLEN); /* OSF1? */
#elif defined(SIOCGENADDR)
   memcpy(&hw_info, &(if_info.ifr_enaddr), INET_ADDRSTRLEN);/* SunOS, IRIX */
#elif defined(SIOCGIFADDR)
   memcpy(&hw_info, &(if_info.ifr_addr), INET_ADDRSTRLEN); /* MacOS */
#elif defined(SIOCGIFHWADDR)
   memcpy(&hw_info, &(if_info.ifr_hwaddr), INET_ADDRSTRLEN); /* Linux */
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

/***************************************************************************
 * Functions for arp.
 **************************************************************************/

/*
 * Helper function.  Since, hostname can have multiple IPs, we pass the
 * address here and find the hardware/ethernet/mac address.
 */
#ifdef __APPLE__
static PyObject* __arp(struct in_addr *ip, PyObject *arp_list)
{
   /* MacOS does not support the SIOCGARP ioctl().  For compatiblity,
    * return the empty list passed in. */
   return arp_list;
}
#else
static PyObject* __arp(struct in_addr *ip, PyObject *arp_list)
{
   struct arpreq arp_msg;
   struct sockaddr_in addr, addr_dummy;
   char dummy[64]; /*holder for the hardware addr*/
   unsigned char *dummy_ptr;  /*used to point to dummy*/
   int as;   /*arping socket*/
#ifdef __linux__
   struct ifconf *intf_confs;
   int number_of_items;
   int k;
#endif /* __linux__ */

   PyObject *arp_dict;
   
   /*arping socket*/

   as = socket(AF_INET, SOCK_DGRAM, 0);

   (void) memset(&arp_msg, 0, sizeof(struct arpreq));
   (void) memset(dummy, 0 , sizeof(dummy));   

   /*
    * Stuff the arp struct with the information it needs.
    */
   
   addr.sin_family = AF_INET;
   addr.sin_addr = *ip; /**((struct in_addr*) host->h_addr_list[0]);*/
   (void) memcpy(&(arp_msg.arp_pa), &addr, sizeof(addr));
   /*
    * Set the address family for the hardware address.  Stevens says that
    * this should be AF_UNSPEC, however Linux needs ARPHRD_ETHER instead.
    */
#ifdef __linux__
   arp_msg.arp_ha.sa_family = ARPHRD_ETHER;
#else
   arp_msg.arp_ha.sa_family = AF_UNSPEC;
#endif /* __linux__ */

   /* Linux insists that you need to specify the interface name.  Other
    * systems don't requrire this extra step.  Since, there can be multiple
    * interfaces lets check them all and let the caller determine which
    * one they want.
    */
#ifdef __linux__
   intf_confs = interfaces();

   if(intf_confs == NULL)
      return arp_list;
   
   number_of_items = intf_confs->ifc_len / sizeof(struct ifreq);

   for(k = 0; k < number_of_items; k++)
#endif /* __linux__ */
   {
#ifdef __linux__
       /*
	* For Linux, we need to set the interface here.  These are values
	* like "eth0" or "eg1".
	*/
       strcpy(arp_msg.arp_dev, intf_confs->ifc_req[k].ifr_name);
#endif /* __linux__ */

       errno = 0;
       if(ioctl(as, SIOCGARP, &arp_msg) < 0)
       {
	  /* This will only work if the destination is on a different subnet
	   * than every interface this host is on.  Thus, ENETUNREACH should
	   * be ignored when this occurs.
	   * For Linux only:  If the destination ip is on the local machine we
	   * get EINVAL.  On non-Linux machines this is valid.
	   */
	  if(errno != ENETUNREACH
#ifdef __linux__
	     && errno != EINVAL
#endif /* __linux__ */
	 )
	  {
	     continue;
	  }  
       }

       /*
	* Now pack the information into the dictionary.
	*/
       arp_dict = PyDict_New(); /* Get a new dict for each iteration. */

       /*
	* Set these items as values in the new dictionary.
	*/
       /* hardware address */
       dummy_ptr = (unsigned char *)&arp_msg.arp_ha.sa_data[0];
       snprintf(dummy, sizeof(dummy), "%x:%x:%x:%x:%x:%x",
		*(dummy_ptr + 0),
		*(dummy_ptr + 1),
		*(dummy_ptr + 2),
		*(dummy_ptr + 3),
		*(dummy_ptr + 4),
		*(dummy_ptr + 5));
       PyDict_SetItemString(arp_dict, "hwaddr",
			    PyString_FromString(dummy));
       /* protocal address */
       /* Needing to copy the arp_pa structure to addr_dummy should not
	* be needed.  However, the (gcc) complier just refused to cast
	* refused to case arp_msg.arp_pa to "struct sockaddr_in. */
       memcpy(&addr_dummy, &arp_msg.arp_pa, sizeof(struct sockaddr));
       if(inet_ntop(arp_msg.arp_pa.sa_family, &addr_dummy.sin_addr,
		    dummy, sizeof(dummy)) > 0)
       {
	  PyDict_SetItemString(arp_dict, "addr",
			       PyString_FromString(dummy));
       }
       /* flags address */
       PyDict_SetItemString(arp_dict, "flags",
			    PyInt_FromLong(arp_msg.arp_flags));
#ifdef __linux__
       /* interface */
       PyDict_SetItemString(arp_dict, "dev",
			    PyString_FromString(arp_msg.arp_dev));
#endif /* __linux */
       
       /*
	* Add the dictionary to the list.
	*/
       PyList_Append(arp_list, arp_dict);
   }
   
   free(intf_confs->ifc_buf);
   free(intf_confs);
   return arp_list;
}
#endif /* ! __APPLE */


PyObject* arpGet(char *dest)
{
   struct hostent *host;
   char **h_ptr;

   PyObject *arp_list;
   
   arp_list = PyList_New(0);

   /*
    * At some point we need to remove gethostbyname() and instead use
    * getaddrinfo().  getaddrinfo() can do IPv6 which gethostbyname() can
    * not.
    */
   
   /*destination ip addr*/
   host = gethostbyname(dest);

   /* Loop over all the ips that refer to this hostname or ip. */
   h_ptr = host->h_addr_list;
   for(h_ptr = host->h_addr_list; *h_ptr != NULL; h_ptr++)
   {
      arp_list = __arp((struct in_addr*)*h_ptr, arp_list);
   }

   return arp_list;
}
