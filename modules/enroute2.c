/* enroute2.c -- enstore hook to change the route on the fly
 *
 * This is meant to be a setuid command and should only be run from encp
 *
 * usage: enroute key add|del|change destination [gateway]
 *
 *	where key is an encoded key from encp and the reset is the same
 *	as defined in route (1M).
 *
 * This implementation uses routing socket, as defined in route(7).
 * route(7) is the new standard supported in IRIX, OFS1 and Solaris.
 * Unforunately, it is not implemented in Linux yet
 *
 * The return values are:
 *
 *  0 successful(?)
 *
 * errors: (no route change at all)
 *
 * 1 was not launched from encp
 * 2 fail to open a routing socket
 * 3 does not have privilege to change route
 * 4 syntax error
 *
 */

/* signature string is to provide a somewhat secured identification of
 * this program because it is supposed to be owned by root and is a
 * suid program. At installation, the limitedly privileged program that
 * changes this ownership and access mode must be sure that the
 * target program, ie. "enroute2", is not a phony one by the same name.
 *
 * The '\n's inside are to fool "strings" command and the spaces are
 * there for a purpose, too.
 */

static char *signature = "Enstore \nSignature: \n2nst4r2 \n2etuorne ";

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <ctype.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <net/if_arp.h>
#include <net/route.h>
#include <netinet/in.h>
#include <netinet/if_ether.h>
#include <netdb.h>

#define SEQ 1

#include "enrouteError.h"

static int valid_key(char *key);
static int do_routing(char *cmd, char *dest, char *gw, char *if_name);
static int do_arping(char *cmd, char *dest, char *dest_hwaddr);

main(argc, argv)
int argc;
char **argv;
{
  /* handle simple syntax and security check */
  if ((argc < 4) || (argc > 6))
  {
     (void) fprintf(stderr, "syntax error\n");
     exit(SyntaxError);
  }
  
  if (!valid_key(argv[1]))
  {
     (void) fprintf(stderr,
		    "can only be launched by enstore/encp\n");
     exit(IllegalExecution);
  }

  (void) setuid(0);	/* to show who the boss is */
  if (getuid())
  {
     /* do not have enough privilege */

     exit(NoPrivilege);
  }

  if(!strcmp(argv[2], "add") || !strcmp(argv[2], "del") ||
     !strcmp(argv[2], "change"))
  {
     if(argc == 4)
	return do_routing(argv[2], argv[3], NULL, NULL);
     if(argc == 5)
	return do_routing(argv[2], argv[3], argv[4], NULL);
     if(argc == 6)
	return do_routing(argv[2], argv[3], argv[4], argv[5]);

     return(SyntaxError);
  }
  else if(!strcmp(argv[2], "arp"))
  {
     if(argc == 5)
	return do_arping(argv[2], argv[3], argv[4]);

     return(SyntaxError);
  }

  return(FailedExecution);
}


#ifdef __linux__

/*
 * For now, Linux is an exception and main() returns 5
 * future implementation depends on whether route(7) will be implemented
 * in Linux. If not, we can still implement it in ioctl(2).
 *
 * Update: It is now implimented in ioctl(2).
 */

static do_routing(char *cmd, char *dest, char *gw, char *if_name)
{
  struct rtentry rt_msg, rt_change_msg;
  struct hostent *host;
  struct sockaddr_in addr;
  int rs;   /*routing socket*/
   
  /*routing socket*/
  
  rs = socket(PF_INET, SOCK_DGRAM, 0);

  /*clear these stuctures*/
  
  memset(&addr, 0, sizeof(struct sockaddr_in));
  memset(&rt_msg, 0, sizeof(struct rtentry));

  /*Set the flags*/
  
  rt_msg.rt_flags = RTF_UP | RTF_HOST;

  /* destination */
  
  host = gethostbyname(dest);
  addr.sin_family = AF_INET;
  addr.sin_addr = *((struct in_addr*) host->h_addr_list[0]);
  (void) memcpy(&(rt_msg.rt_dst), &addr, sizeof(addr));

  /* gateway, if any */
  
  if (gw != NULL)
  {
    host = gethostbyname(gw);
    addr.sin_family = AF_INET;
    addr.sin_addr = *((struct in_addr*) host->h_addr_list[0]);
    (void) memcpy(&(rt_msg.rt_gateway), &addr, sizeof(addr));

    rt_msg.rt_flags |= RTF_GATEWAY;
  }

  if(!strcmp(cmd, "add"))
  {
    /*
     * The rt_dev field allows us to specify the actuall interface to be used.
     * This works even if the systems interfaces are on the same subnet.
     *
     * Only set this field when adding.  If this field is set for a route
     * deletion we would get errno 19 (No such device).
     */
    rt_msg.rt_dev = if_name;

    /* Add the new route to the routing table. */
    if(ioctl(rs, SIOCADDRT, &rt_msg) < 0)
    {
       if(errno != EEXIST)
       {
	  (void) fprintf(stderr,
			 "enroute2(SIOCADDRT): errno %d: %s\n",
			 errno, strerror(errno));
	  exit(RSOpenFailure);
       }
    }

  }
  else if(!strcmp(cmd, "del"))
  {
    /* Delete the specified route from the routing table. */
    if(ioctl(rs, SIOCDELRT, &rt_msg) < 0)
    {
       /*
	* It is not an error if the route we want to delete already
	* does not exist.  Thus skip on ESRCH.  Using errno ESRCH
	* in this capacity might be Linux specific.
	*/
       if(errno != ESRCH)
       {
	  (void) fprintf(stderr,
			 "enroute2(SIOCDELRT): errno %d: %s\n",
			 errno, strerror(errno));
	  exit(RSOpenFailure);
       }
    }
  }
  else if(!strcmp(cmd, "change"))
  {
    /* Do not use the gateway when deleting a route; it will not match. */
    (void) memcpy(&rt_change_msg, &rt_msg, sizeof(rt_msg));
    (void) memset(&(rt_msg.rt_gateway), 0, sizeof(addr));
    if(ioctl(rs, SIOCDELRT, &rt_change_msg) < 0)
    {
       if(errno != ESRCH)
       {
	  (void) fprintf(stderr,
			 "enroute2(SIOCDELRT): errno %d: %s\n",
			 errno, strerror(errno));
	  exit(RSOpenFailure);
       }
    }
    if(ioctl(rs, SIOCADDRT, &rt_msg) < 0)
    {
       if(errno != EEXIST)
       {
	  (void) fprintf(stderr,
			 "enroute2(SIOCADDRT): errno %d: %s\n",
			 errno, strerror(errno));
	  exit(RSOpenFailure);
       }
    }
  }

  close(rs);
  return(OK);
}


#else

/*
 * This section of code is for OSes that support BSD routing sockets.
 */

int do_routing(char *cmd, char *dest, char *gw, char *if_name)
{
	struct hostent *host;
	int rs;   /*routing socket*/
	char buf[512];	/* no memory allocation, no memory leak */

	/* define pointers to access its parts */

	struct rt_msghdr *rtm = (struct rt_msghdr *) buf;
	struct sockaddr_in *addr1 = (struct sockaddr_in *) (rtm + 1);
	struct sockaddr_in *addr2 = (struct sockaddr_in *) (addr1 + 1);
	
	/* open the routing socket */
	
	if ((rs = socket(PF_ROUTE, SOCK_RAW, 0)) < 0)
	{
		(void) fprintf(stderr, "can not open routing socket\n");
		exit(RSOpenFailure);
	}

	/* pre-fill rtm, mostly standard stuff */

	(void) memset((char *) buf, 0, sizeof(buf));

	rtm->rtm_version = RTM_VERSION;
	rtm->rtm_pid = getpid();
	rtm->rtm_seq = SEQ;
	rtm->rtm_flags = RTF_UP | RTF_GATEWAY | RTF_HOST;

	/* pre-fill address(es) */

	/* destination */

	host = gethostbyname(dest);
	addr1->sin_family = AF_INET;
	addr1->sin_addr = *((struct in_addr*) host->h_addr_list[0]);

	/* gateway, if any */

	if (gw != NULL)
	{
		host = gethostbyname(gw);
		addr2->sin_family = AF_INET;
		addr2->sin_addr = *((struct in_addr*) host->h_addr_list[0]);
	}

	/* routing message type specific information */

	if (!strcmp(cmd, "add"))
	{
		rtm->rtm_msglen = sizeof(struct rt_msghdr) + 2 * sizeof(struct sockaddr);
		rtm->rtm_type = RTM_ADD;
		rtm->rtm_addrs = RTA_DST | RTA_GATEWAY;
		if (if_name != NULL)
		{
			/*
			 * Force the interface that is to be used.  This
			 * might allow for different interfaces to be on the
			 * same subnet and still route as we want.
			 */
			rtm->rtm_index = if_nametoindex(if_name);
		}
	}
	else if (!strcmp(cmd, "del"))
	{
		rtm->rtm_msglen = sizeof(struct rt_msghdr) + sizeof(struct sockaddr);
		rtm->rtm_type = RTM_DELETE;
		rtm->rtm_addrs = RTA_DST;
	}
	else if (!strcmp(cmd, "change"))
	{
		rtm->rtm_msglen = sizeof(struct rt_msghdr) + 2 * sizeof(struct sockaddr);
		rtm->rtm_type = RTM_CHANGE;
		rtm->rtm_addrs = RTA_DST | RTA_GATEWAY;
		if (if_name != NULL)
		{
			/*
			 * Force the interface that is to be used.  This
			 * might allow for different interfaces to be on the
			 * same subnet and still route as we want.
			 */
			rtm->rtm_index = if_nametoindex(if_name);
		}
	}
	else
	{
	        exit(SyntaxError);
	}

	/* can not check the error, if any */

	(void) write(rs, rtm, rtm->rtm_msglen);

	(void) close(rs);
	return(OK);
}

#endif

static int do_arping(char *cmd, char *dest, char *dest_hwaddr)
{
   struct arpreq arp_msg;
   struct hostent *host;
   struct sockaddr_in addr;
   char dummy[32]; /*holder for the hardware addr*/
   int as;   /*arping socket*/
   unsigned int i, j = 0U;
   unsigned char *a_ptr;  /*array pointer*/
   unsigned short holder; /*holder since C99 hh modifier is not everywhere*/

   if(strcmp(cmd, "arp"))
   {
      exit(SyntaxError);
   }
   
   /*arping socket*/

   as = socket(PF_INET, SOCK_DGRAM, 0);

   (void) memset(&arp_msg, 0, sizeof(struct arpreq));
   (void) memset(dummy, 0 , 32);   

   /*destination ip addr*/

   host = gethostbyname(dest);
   addr.sin_family = AF_INET;
   addr.sin_addr = *((struct in_addr*) host->h_addr_list[0]);
   (void) memcpy(&(arp_msg.arp_pa), &addr, sizeof(addr));

   /*destination hardware addr*/

   for(i = 0U; i < strlen(dest_hwaddr) && j < 32U; /* blank */ )
   {
      /* Hardware addresses are typically represented like AA:BB:CC:DD:EE:FF,
       * A:BB:C:D:EE:FF or AABBCCDDEEFF.  The first if handles the first and
       * third situations.  The second if is for the second situation.  The
       * trailing else is almost always used.
       */
      
      if(isxdigit(dest_hwaddr[i]) && isxdigit(dest_hwaddr[i + 1]))
      {
	 dummy[j] = dest_hwaddr[i];
	 dummy[j + 1] = dest_hwaddr[i + 1];
	 j += 2;
	 i += 2;
      }
      if(isxdigit(dest_hwaddr[i]) && dest_hwaddr[i + 1] == ':')
      {
	 dummy[j] = '0'; /* Insert the implied 0. */
	 dummy[j + 1] = dest_hwaddr[i];
	 j += 2;
	 i += 2;
      }
      else /* This is most likely dest_hwaddr[i] == ":" */
	 i++;
   }
   a_ptr = (void*)&(arp_msg.arp_ha.sa_data); /* shorthand pointer */
   /*
    * Copy each byte into the correct location in the hardware address's
    * location in memory.  C99 compilant systems could do this much more
    * efficently, since the hh modifier could then be used.
    *
    *   if(sscanf(dummy, "%2hhx%2hhx%2hhx%2hhx%2hhx%2hhx",
    *	     a_ptr, (a_ptr + 1), (a_ptr + 2),
    *	     (a_ptr + 3), (a_ptr + 4), (a_ptr + 5)) == EOF)
    *   {
    *      return(SyntaxError);
    *   }
    *
    */
   if(sscanf(dummy + 0, "%2hx", &holder) == EOF) { return(SyntaxError); }
   *(a_ptr + 0) = (unsigned char) holder;
   if(sscanf(dummy + 2, "%2hx", &holder) == EOF) { return(SyntaxError); }
   *(a_ptr + 1) = (unsigned char) holder;
   if(sscanf(dummy + 4, "%2hx", &holder) == EOF) { return(SyntaxError); }
   *(a_ptr + 2) = (unsigned char) holder;
   if(sscanf(dummy + 6, "%2hx", &holder) == EOF) { return(SyntaxError); }
   *(a_ptr + 3) = (unsigned char) holder;
   if(sscanf(dummy + 8, "%2hx", &holder) == EOF) { return(SyntaxError); }
   *(a_ptr + 4) = (unsigned char) holder;
   if(sscanf(dummy + 10, "%2hx", &holder) == EOF) { return(SyntaxError); }
   *(a_ptr + 5) = (unsigned char) holder;

   /*
    * Set the address family for the hardware address.  Stevens says that
    * this should be AF_UNSPEC, however Linux needs ARPHRD_ETHER instead.
    */
#ifdef __linux__
   arp_msg.arp_ha.sa_family = ARPHRD_ETHER;
#else
   arp_msg.arp_ha.sa_family = AF_UNSPEC;
#endif
   arp_msg.arp_flags = ATF_PUBL;

   errno = 0;
   if(ioctl(as, SIOCSARP, &arp_msg) < 0)
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
#endif
	 )
      {
	 (void) fprintf(stderr,
			"enroute2(SIOCSARP): errno %d: %s\n",
			errno, strerror(errno));
	 exit(RSOpenFailure);
      }
   }

   return(OK);
}

static int valid_key(key)
char *key;
{
	int pid;
	int i, l, m;
	char c;

	/* reverse the key string */

	l = strlen(key);
	m = l / 2;
	
	for (i = 0; i < m; i++)
	{
		/* swaping key[i] and key[l-i-1] */

		c = key[i];
		key[i] = key[l-i-1];
		key[l-i-1] = c;
	}

	pid = atoi(key);
 	if (pid == getpid())
	{
		return(1);
	}
	
	return(0);
}


