%module enroute

%include "../enrouteError.h"

extern int routeAdd(char *dest, char *gw, char *if_name);
extern int routeDel(char *dest);
extern int routeChange(char *dest, char *gw, char *if_name);
extern int arpAdd(char *dest, char *dest_hwaddr);
extern char *errstr(int errno);
