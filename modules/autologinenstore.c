
/* This file is called from /bin/login to boot the system to user enstore. */

#include <unistd.h>

int main()
{
   execlp("login", "login", "-f", "enstore", 0);
}
