/* enroute -- python interface to limited setuid-ed route commands
 *
 *    routeAdd(destination, gateway) -- add destination through gateway
 *    routeDel(destination) -- delate destination
 */

#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <string.h>

#include "enrouteError.h"

/* errstr(errno) -- return error message for the errno */

char *errstr(errno)
int errno;
{
	switch(errno)
	{
	case OK: return("OK");
	case IllegalExecution: return("enroute2 was not launched from ENCP");
	case RSOpenFailure: return("fail to open routing socket");
	case NoPrivilege: return("not enough privilege to change route");
	case SyntaxError: return("syntax error");
	case NoEnroute2: return("no enroute2");
	}
	return("unknown error");
}

/* getexecpath(path) -- set path for enroute2, which will be at either
 *		$ENCP_DIR/enroute2 or $ENSTORE_DIR/sbin/renroute2
 *
 *	path is a buffer supplied by caller
 *	getexecpath() returns path or NULL, if none is found
 */

static char *getexecpath(path)
char *path;
{
	char *p;
#if 0
	/* modified to have enrout2 at /usr/local/bin/enroute2 */
	if ((p = getenv("ENCP_DIR")) != NULL)
	{
		strcpy(path, p);
		strcat(path, "/enroute2");
	}
	else if ((p = getenv("ENSTORE_DIR")) != NULL)
	{
		strcpy(path, p);
		strcat(path, "/sbin/enroute2");
	}
	else	/* no ENCP_DIR nor ENSTORE_DIR defined */
	{
		strcpy(path, "enroute2");
	}
#endif

	strcpy(path, "/usr/local/bin/enroute2");

	if (access(path, X_OK))
	{
		return(NULL);
	}

	return(path);
}

/* keygen(key) -- generate the key to activate enroute2
 *	currently, the key is the reverse of ascii representation
 *	of the pid
 *
 *	keygen shall never fail and it always returns key
 */

static char *keygen(key)
char *key;
{
	int i, l, m;
	char c;

	sprintf(key, "%d", getpid());

	l = strlen(key);	/* length of the key */
	m = l / 2;		/* middle position in key */

	for (i = 0; i < m; i++)
	{
		/* swaping key[i] and key[l-i-1] */

		c = key[i];
		key[i] = key[l-i-1];
		key[l-i-1] = c;
	}

	return(key);
}

/* route() -- python interface to route */

static int route(cmd, dest, gw)
char *cmd;
char *dest;
char *gw;
{
	char path[512];
	int pid, child;
	char key[64];
	int status;

	if (getexecpath(path) == NULL)
	{
		return (NoEnroute2);
	}

	if ((pid = fork()) == 0)	/* child */
	{
		(void) keygen(key);	/* generate a key */
		execl(path, "phantom-encp", key, cmd, dest, gw, NULL);
	}
	else
	{
		child = wait(&status);
	}

	if (WIFEXITED(status))
	{
		return(WEXITSTATUS(status));
	}

	/* This should not happen since enroute2 always calls exit */

	return(OK);
}

int routeAdd(dest, gw)
char *dest;
char *gw;
{
	return route("add", dest, gw);
}

int routeDel(dest)
char *dest;
{
	return route("del", dest, NULL);
}
