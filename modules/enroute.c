/* enroute -- python interface to limited setuid-ed route commands
 *
 *    routeAdd(destination, gateway) -- add destination through gateway
 *    routeDel(destination) -- delate destination
 *    routeChange(destination) -- modify existing destination
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <string.h>
#include <sys/stat.h>
#include <limits.h>
#include "enrouteError.h"

/* errstr(errno) -- return error message for the errno */

char *errstr(int errno)
{
	switch(errno)
	{
	case OK: return("OK");
	case IllegalExecution: return("enroute2 was not launched from ENCP");
	case RSOpenFailure: return("fail to open routing socket");
	case NoPrivilege: return("not enough privilege to change route");
	case SyntaxError: return("syntax error");
	case NoEnroute2: return("no enroute2");
	case FailedExecution: return("unable to run enroute2");
	}
	return("unknown error");
}

/* is_root_setuid_exe(path) -- check if path is an executable owned by
 *				root and its suid bit is set
 */

static int is_root_setuid_exe(char *path)
{
        struct stat stbuf;

        if (stat(path, &stbuf))
        {
                return(0);
        }

        return((stbuf.st_uid == 0) && (stbuf.st_mode & S_ISUID) &&
		!access(path, X_OK));	/* access() does more */
}

/* getexecpath(path) -- set path for enroute2, which will be at either
 *		$ENCP_DIR/enroute2 or $ENSTORE_DIR/sbin/renroute2
 *
 *	path is a buffer supplied by caller
 *	getexecpath() returns path or NULL, if none is found
 */

static char *getexecpath(char *path)
{
	char *p;

	/* try ENROUTE2 first so that user may override defaults */

	if ((p = getenv("ENROUTE2")) != NULL)
	{
		(void) strcpy(path, p);
		if (is_root_setuid_exe(path))
		{
			return(path);
		}
	}

	/* try $ENCP_DIR */

	if ((p = getenv("ENCP_DIR")) != NULL)
	{
		(void) strcpy(path, p);
		(void) strcat(path, "/enroute2");
		if (is_root_setuid_exe(path))
		{
			return(path);
		}
	}

	/* try $ENSTORE_DIR/sbin */

	if ((p = getenv("ENSTORE_DIR")) != NULL)
	{
		(void) strcpy(path, p);
		(void) strcat(path, "/sbin/enroute2");
		if (is_root_setuid_exe(path))
		{
			return(path);
		}
	}

	/* try /usr/local/bin/enroute2 */

	(void) strcpy(path, "/usr/local/bin/enroute2");
	if (is_root_setuid_exe(path))
	{
		return(path);
	}

	/* try /etc/enroute2 */

	(void) strcpy(path, "/etc/enroute2");
	if (is_root_setuid_exe(path))
	{
		return(path);
	}

	return(NULL);
}

/* keygen(key) -- generate the key to activate enroute2
 *	currently, the key is the reverse of ascii representation
 *	of the pid
 *
 *	keygen shall never fail and it always returns key
 */

static char *keygen(char *key)
{
	int i, l, m;
	char c;

	(void) sprintf(key, "%d", getpid());

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

static int route(char *cmd, char *dest, char *gw, char *if_name)
{
	char path[PATH_MAX + 1];
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
		if(execl(path, "phantom-encp", key,
			 cmd, dest, gw, if_name, NULL) < 0)
		{
			return(FailedExecution);
		}
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

int routeAdd(char *dest, char *gw, char *if_name)
{
	return route("add", dest, gw, if_name);
}

int routeDel(char *dest)
{
	return route("del", dest, NULL, NULL);
}

int routeChange(char *dest, char *gw, char *if_name)
{
	return route("change", dest, gw, if_name);
}
