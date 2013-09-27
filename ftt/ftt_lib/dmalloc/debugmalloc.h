
#undef malloc
#undef free
/*
 * malloc for debugging -- allocates via sbrk and tracks stuff, does diag dump
 * if things appear to be screwed up.					(bsa)
 */
#if defined(__STDC__)
extern int _malldmp(void);
#if 0
extern char *malloc(register unsigned );
extern int free(register char *);
extern char *realloc(register char *, register unsigned);
extern char *calloc(register unsigned , register unsigned );
extern int cfree(char *);
#endif
extern int _mallchk(char *);
extern char *malloc_debug(register unsigned, char *, int );
extern int free_debug(register char *, char *, int);
#else
extern int _malldmp();
#if 0
extern char *malloc();
extern int free();
extern char *realloc();
extern char *calloc();
extern int cfree();
#endif
extern int _mallchk();
extern char *malloc_debug();
extern int free_debug();
#endif

/*
 * Now get real debugging info, file and line of mallocs/frees
 */
#define  malloc(n)    malloc_debug(n,__FILE__,__LINE__)
#define    free(p)      free_debug(p,__FILE__,__LINE__)
#define realloc(p,n) realloc_debug(p,n,__FILE__,__LINE__)
#define   cfree(p)      free_debug(p,__FILE__,__LINE__)
