%module enroute

%include "../enrouteError.h"

extern int routeAdd(char *dest, char *gw, char *if_name);
extern int routeDel(char *dest);
extern int routeChange(char *dest, char *gw, char *if_name);
extern char *errstr(int errno);
