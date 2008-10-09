
/* This file is called from /bin/login to boot the system to user enstore. */

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

int main()
{
   if(execlp("login", "login", "-f", "enstore", 0) < 0)
   {
      fprintf(stderr, "ERROR: %s\n", strerror(errno));
      return 4;
   }
   return 3;
}
