%module enroute

%include "../enrouteError.h"

extern int routeAdd(char *dest, char *gw);
extern int routeDel(char *dest);
extern int routeChange(char *dest, char *gw);
extern char *errstr(int errno);
