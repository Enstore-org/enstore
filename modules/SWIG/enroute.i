%module enroute

%include "../enrouteError.h"

int routeAdd(char *dest, char *gw);
int routeDel(char *dest);
int routeChange(char *dest, char *gw);
char *errstr(int errno);
