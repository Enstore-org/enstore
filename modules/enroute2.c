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
#include <net/route.h>
#include <netinet/in.h>
#include <netdb.h>

#define SEQ 1

#include "enrouteError.h"

static int valid_key(char *key);

#ifdef __linux__
/* for now, Linux is an exception and main() returns 5
 * future implementation depends on whether route(7) will be implemented
 * in Linux. If not, we can still implement it in ioctl(2)
 */

main(argc, argv)
int argc;
char **argv;
{
	exit(NoEnroute2);
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

	if ((argc < 4) || (argc > 5))
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
	
	(void) setuid(0);	/* to show who the boss is */
	if (getuid())
	{
		/* do not have enough privilege */

		exit(NoPrivilege);
	}

	if (!strcmp(argv[2], "add"))
	{
	        rtm->rtm_msglen = sizeof(struct rt_msghdr) + 2 * sizeof(struct sockaddr);
		rtm->rtm_type = RTM_ADD;
		rtm->rtm_addrs = RTA_DST | RTA_GATEWAY;
		if (argc < 5)
		{
			exit(SyntaxError);
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
		if (argc < 5)
		{
		        exit(SyntaxError);
		}
	}
	else
	{
	        exit(SyntaxError);
	}

	/* can not check the error, if any */

	(void) write(rs, rtm, rtm->rtm_msglen);

	(void) close(rs);
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
