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
#include <sys/types.h>
#include <unistd.h>
#include <sys/socket.h>
#include <net/if.h>
#include <net/if_arp.h>
#include <net/route.h>
#include <netinet/in.h>
#include <netinet/if_ether.h>
#include <netdb.h>
#include <errno.h>
#include <sys/stat.h>
#include <limits.h>

#define SEQ 1

#include "enrouteError.h"

static int valid_key(char *key);
static int is_exe(char *path);
static char *getexecpath(char *path);
static int force_arp_update(char *dest_addr, char *local_intf);


#ifdef __linux__
/*
 * For now, Linux is an exception and main() returns 5
 * future implementation depends on whether route(7) will be implemented
 * in Linux. If not, we can still implement it in ioctl(2).
 *
 * Update: It is now implimented in ioctl(2).
 */

#include <sys/ioctl.h>

main(argc, argv)
int argc;
char **argv;
{
  struct rtentry rt_msg, rt_change_msg;
  struct hostent *host;
  struct sockaddr_in addr;
  int rs;   /*routing socket*/
  int rtn;
  struct arpreq arp_msg;
  
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
  
  /*routing socket*/
  rs = socket(PF_INET, SOCK_DGRAM, 0);

  /*clear these stuctures*/
  memset(&addr, 0, sizeof(struct sockaddr_in));
  memset(&rt_msg, 0, sizeof(struct rtentry));
  memset(&arp_msg, 0, sizeof(struct arpreq));

  /*Set the flags*/
  rt_msg.rt_flags = RTF_UP | RTF_HOST;

  /* destination */
  host = gethostbyname(argv[3]);
  addr.sin_family = AF_INET;
  addr.sin_addr = *((struct in_addr*) host->h_addr_list[0]);
  memcpy(&(rt_msg.rt_dst), &addr, sizeof(addr));

  /* gateway */
  if (argc > 4)
  {
    host = gethostbyname(argv[4]);
    addr.sin_family = AF_INET;
    addr.sin_addr = *((struct in_addr*) host->h_addr_list[0]);
    memcpy(&(rt_msg.rt_gateway), &addr, sizeof(addr));

    rt_msg.rt_flags |= RTF_GATEWAY;
  }


  if(!strcmp(argv[2], "add"))
  {
    /*
     * The rt_dev field allows us to specify the actuall interface to be used.
     * This works even if the systems interfaces are on the same subnet.
     *
     * Only set this field when adding.  If this field is set for a route
     * deletion we would get errno 19 (No such device).
     */
    rt_msg.rt_dev = argv[5];

    /* Add the new route to the routing table. */
    if((rtn = ioctl(rs, SIOCADDRT, &rt_msg)) < 0)
    {
       if(errno != EEXIST)
       {
	  fprintf(stderr,
		  "enroute2(SIOCADDRT): errno %d: %s\n",
		  errno, strerror(errno));
	  exit(RSOpenFailure);
       }
    }

    /* Set the arp struct. */
    memcpy(&(arp_msg.arp_pa), &addr, sizeof(addr));

    /* Remove from the arp table the old entry. */
    if((argc > 5) && ((rtn = ioctl(rs, SIOCDARP, &arp_msg)) < 0))
    {
       fprintf(stderr,
	    "add enroute2(SIOCDARP): errno %d: %s\n", errno, strerror(errno));
       exit(RSOpenFailure);
    }

    /*
     * The remaining problem is that the other end may decide
     * to use the wrong interface to send to.  By forcing an
     * arp update using the correct interface (the system
     * defaults to the first one it finds) we can avoid this
     * problem when interfaces are on the same subnet.
     */
    (void) force_arp_update(argv[3], argv[5]);    
  }
  else if(!strcmp(argv[2], "del"))
  {
    if((rtn = ioctl(rs, SIOCDELRT, &rt_msg)) < 0)
    {
       /*
	* It is not an error if the route we want to delete already
	* does not exist.  Thus skip on ESRCH.  Using errno ESRCH
	* in this capacity might be Linux specific.
	*/
       if(errno != ESRCH)
       {
	  fprintf(stderr,
		  "enroute2(SIOCDELRT): errno %d: %s\n",
		  errno, strerror(errno));
	  exit(RSOpenFailure);
       }
    }
#if 0
    if((rtn = ioctl(rs, SIOCDARP, &arp_msg)) < 0)
    {
       fprintf(stderr,
	       "enroute2(SIOCDARP): errno %d: %s\n", errno, strerror(errno));
       exit(RSOpenFailure);
    }
#endif
  }
  else if(!strcmp(argv[2], "change"))
  {
    /* Do not use the gateway when deleting a route; it will not match. */
    memcpy(&rt_change_msg, &rt_msg, sizeof(rt_msg));
    memset(&(rt_msg.rt_gateway), 0, sizeof(addr));
    if((rtn = ioctl(rs, SIOCDELRT, &rt_change_msg)) < 0)
    {
       if(errno != ESRCH)
       {
	  fprintf(stderr,
		  "enroute2(SIOCDELRT): errno %d: %s\n",
		  errno, strerror(errno));
	  exit(RSOpenFailure);
       }
    }
    if((rtn = ioctl(rs, SIOCADDRT, &rt_msg)) < 0)
    {
       if(errno != EEXIST)
       {
	  fprintf(stderr,
		  "enroute2(SIOCADDRT): errno %d: %s\n",
		  errno, strerror(errno));
	  exit(RSOpenFailure);
       }
    }

    /*
     * The remaining problem is that the other end may decide
     * to use the wrong interface to send to.  By forcing an
     * arp update using the correct interface (the system
     * defaults to the first one it finds) we can avoid this
     * problem when interfaces are on the same subnet.
     */
    (void) force_arp_update(argv[3], argv[5]);
  }

  close(rs);
  exit(0);
}

#else

main(argc, argv)
int argc;
char **argv;
{
	int rs;		/* routing socket */
	struct hostent *host;

	char buf[512];	/* no memory allocation, no memory leak */

	/* define pointers to access its parts */

	struct rt_msghdr *rtm = (struct rt_msghdr *) buf;
	struct sockaddr_in *addr1 = (struct sockaddr_in *) (rtm + 1);
	struct sockaddr_in *addr2 = (struct sockaddr_in *) (addr1 + 1);

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

	host = gethostbyname(argv[3]);
	addr1->sin_family = AF_INET;
	addr1->sin_addr = *((struct in_addr*) host->h_addr_list[0]);

	/* gateway, if any */

	if (argc > 4)
	{
		host = gethostbyname(argv[4]);
		addr2->sin_family = AF_INET;
		addr2->sin_addr = *((struct in_addr*) host->h_addr_list[0]);
	}

	/* routing message type specific information */

	if (!strcmp(argv[2], "add"))
	{
		rtm->rtm_msglen = sizeof(struct rt_msghdr) + 2 * sizeof(struct sockaddr);
		rtm->rtm_type = RTM_ADD;
		rtm->rtm_addrs = RTA_DST | RTA_GATEWAY;
		if (argc > 5)
		{
			/*
			 * Force the interface that is to be used.  This
			 * might allow for different interfaces to be on the
			 * same subnet and still route as we want.
			 */
			rtm->rtm_index = if_nametoindex(argv[5]);
		}
	}
	else if (!strcmp(argv[2], "del"))
	{
		rtm->rtm_msglen = sizeof(struct rt_msghdr) + sizeof(struct sockaddr);
		rtm->rtm_type = RTM_DELETE;
		rtm->rtm_addrs = RTA_DST;
	}
	else if (!strcmp(argv[2], "change"))
	{
		rtm->rtm_msglen = sizeof(struct rt_msghdr) + 2 * sizeof(struct sockaddr);
		rtm->rtm_type = RTM_CHANGE;
		rtm->rtm_addrs = RTA_DST | RTA_GATEWAY;
		if (argc > 5)
		{
			/*
			 * Force the interface that is to be used.  This
			 * might allow for different interfaces to be on the
			 * same subnet and still route as we want.
			 */
			rtm->rtm_index = if_nametoindex(argv[5]);
		}
	}
	else
	{
	        exit(SyntaxError);
	}

	/* can not check the error, if any */

	(void) write(rs, rtm, rtm->rtm_msglen);

	(void) close(rs);

	if (!strcmp(argv[2], "add") || (!strcmp(argv[2], "change")))
	{
	        /*
		 * The remaining problem is that the other end may decide
		 * to use the wrong interface to send to.  By forcing an
		 * arp update using the correct interface (the system
		 * defaults to the first one it finds) we can avoid this
		 * problem when interfaces are on the same subnet.
		 */
	        (void) force_arp_update(argv[3], argv[5]);
	}
	
	exit(0);
}

#endif

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


/*
 *  is_exe(path) -- check if path is an executable
 */

static int is_exe(char *path)
{
        struct stat stbuf;

        if (stat(path, &stbuf))
        {
                return(0);
        }

	
        if(stbuf.st_uid == 0) /* Shortcut for root. */
	       return (stbuf.st_mode & (S_IXUSR | S_IXGRP | S_IXOTH));
	else if(stbuf.st_mode & S_IXOTH) /* all executable */
	       return (stbuf.st_mode & S_IXOTH);
	else if(stbuf.st_uid == geteuid()) /* user privledge */
	       return (stbuf.st_mode & S_IXUSR);
	else if(stbuf.st_gid == getegid()) /* group privledge */
	       return (stbuf.st_mode & S_IXGRP);

	return(0);
}

/*
 *  getexecpath(path) -- set path for arping, which will be at either
 *		/sbin/arping or /usr/local/bin/arping
 *
 *	path is a buffer supplied by caller
 *	getexecpath() returns path or NULL, if none is found
 */

static char *getexecpath(char *path)
{
	char *p;

	/* try ARPING first so that user may override defaults */

	if ((p = getenv("ARPING")) != NULL)
	{
		(void) strcpy(path, p);
		if (is_exe(path))
		{
			return(path);
		}
	}

	/* try /usr/local/sbin/arping */

	(void) strcpy(path, "/usr/local/sbin/arping");
	if (is_exe(path))
	{
		return(path);
	}
	
	/* try /usr/local/bin/arping */

	(void) strcpy(path, "/usr/local/bin/arping");
	if (is_exe(path))
	{
		return(path);
	}

	/* try /sbin/arping */

	(void) strcpy(path, "/sbin/arping");
	if (is_exe(path))
	{
		return(path);
	}

	return(NULL);
}

/*
 * force_arp_update() takes two arguments:
 * 1) the destination ip address/hostname of the remote host
 * 2) the interface name on the local host to use
 *
 * If the arping program is installed on the local system, the arp tables
 * should be set correctly on both nodes.  If it is not there, then
 * nothing will happen and the transfer will continue as best luck
 * will provide.
 */
static int force_arp_update(char *dest_addr, char *local_intf)
{
	char path[PATH_MAX + 1];
	char arping[PATH_MAX + 512 + 1];

	if (getexecpath(path) == NULL)
	{
		return(NoPrivilege);
	}
   
	sprintf(arping, "%s -q -I %s -c 1 %s", path, local_intf, dest_addr);
	if(system(arping))
	{
		fprintf(stderr, "ARP update failed.\n");
		return(FailedExecution);
	}

	return(0);
}
