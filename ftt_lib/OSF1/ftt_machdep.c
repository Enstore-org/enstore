#include <sys/utsname.h>
#include <string.h>
#include <ctype.h>
#include <stdio.h>
#include <ftt_private.h>


int
ftt_status(ftt_descriptor d, int time_out) {
    static ftt_stat block;
    int res;
    char *p;

    return -1;
}

int
ftt_set_hwdens(ftt_descriptor d, int hwdens) {
    /* ignore hwdens, 'cause we opened the right device node */
   return 0;
}

int
ftt_set_compression(ftt_descriptor d, int compression) {
   return 0;
}
int
ftt_set_blocksize(ftt_descriptor d, int blocksize) {
    static struct mtop buf;
    static int recursing = 0;
    int res;

    /* ZZZ */
    return 0;
}

int
ftt_get_hwdens(ftt_descriptor d) {
    int res;

    res = d->devinfo[d->which_is_default].hwdens;
    return res;
}
