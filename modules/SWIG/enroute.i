%module enroute

%include "../enrouteError.h"

int routeAdd(char *dest, char *gw);
int routeDel(char *dest);
char *errstr(int errno);
