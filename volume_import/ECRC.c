/*--------------------------------------------------------------------------*/
/* adler32.c -- compute the Adler-32 checksum of a data stream
 * Copyright (C) 1995-1998 Mark Adler
 * For conditions of distribution and use, see copyright notice in zlib.h
 */


#define BASE 65521L /* largest prime smaller than 65536 */
#define NMAX 5552
/* NMAX is the largest n such that 255n(n+1)/2 + (n+1)(BASE-1) <= 2^32-1 */

#define DO1(buf,i)  {s1 += buf[i]; s2 += s1;}
#define DO2(buf,i)  DO1(buf,i); DO1(buf,i+1);
#define DO4(buf,i)  DO2(buf,i); DO2(buf,i+2);
#define DO8(buf,i)  DO4(buf,i); DO4(buf,i+4);
#define DO16(buf)   DO8(buf,0); DO8(buf,8);


unsigned long adler32(adler, buf, len)
    unsigned long adler;
    unsigned char *buf;
    unsigned int len;
{
    unsigned long s1 = adler & 0xffff;
    unsigned long s2 = (adler >> 16) & 0xffff;
    int k;

    if (buf == (unsigned char*)0) return 1L;

    while (len > 0) {
        k = len < NMAX ? len : NMAX;
        len -= k;
        while (k >= 16) {
            DO16(buf);
            buf += 16;
            k -= 16;
        }
        if (k != 0) do {
            s1 += *buf++;
            s2 += s1;
        } while (--k);
        s1 %= BASE;
        s2 %= BASE;
    }
    return (s2 << 16) | s1;
}

#ifdef TESTING
#include <stdlib.h>
#include <stdio.h>
main(int argc, char **argv)
{
    char *fname, *buf;
    unsigned long val,len;
    FILE *fp;

    if (argc!=4){
	fprintf(stderr,"Usage: %s file len init-val\n", argv[0]);
	exit(-1);
    }
    fname=argv[1];
    len=strtoul(argv[2],0,10);
    val=strtoul(argv[3],0,10);
    buf=(char*)malloc(len);
    fp=fopen(fname,"r");
    fread(buf, 1, len, fp);
    fclose(fp);
    printf("adler32(%u,%s,%d)=%u\n",
	   val,buf,len,adler32(val,buf,len));
}

#endif

	   
