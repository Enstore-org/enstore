/*
 * malloc for debugging -- allocates via sbrk and tracks stuff, does diag dump
 * if things appear to be screwed up.					(bsa)
 */
#if defined(__STDC__)
extern int _malldmp(void);
extern char *malloc(register unsigned );
extern int free(register char *);
extern char *realloc(register char *, register unsigned);
extern int _mallchk(char *);
extern char *calloc(register unsigned , register unsigned );
extern int cfree(char *);
#else
extern int _malldmp();
extern char *malloc();
extern int free();
extern char *realloc();
extern int _mallchk();
extern char *calloc();
extern int cfree();
#endif
