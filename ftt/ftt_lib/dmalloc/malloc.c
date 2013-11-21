/*
 * malloc for debugging -- allocates via sbrk and tracks stuff, does diag dump
 * if things appear to be screwed up
 *
 * mengel@fnal.gov Fri Jan 23 11:36:26 CST 1998
 *  	- Added trailing magic byte, and malloc_debug free_debug calls that
 *	  take line numbers.  
 *	- Put magic cookie between pointers and allocated memory.
 *	- added m_file and m_line so you know where memory was allocated/
 * 	  freed.
 *	- added m_wanted so you know how much of m_size the user actually
 *	  allocated.
 *	- added DEBUG_MALLOC_LEVEL environment var in _mall_init, so we 
 *	  can turn debugging on and off without recompiling.
 */

#include "malloc.h"
#include <signal.h>

#define MAGIC_COOKIE 	0x0FF51DE5
#define END_BYTE	0x05
extern char *sbrk();
/* extern char *memcpy(); */
/* extern char *memset(); */
extern char etext[];
extern char edata[];
extern char end[];

#define ARR_BYTES 	8
struct _Dmi {
	struct _Dmi *m_next;
	struct _Dmi *m_prev;
	char *m_file;
	int m_line;
	long m_size;
	long m_wanted;
	long int m_cookie;
	char m_blk[ARR_BYTES];
};
#define HEADSIZE	((sizeof(struct _Dmi))-ARR_BYTES + 1)

static struct _Dmi *_fab = (struct _Dmi *) 0;
static struct _Dmi *_ffb = (struct _Dmi *) 0;
static char *_xbrk = 0;
static long int _in_malloc = 0;
static long int _st_malloc = 0;
/* 
 * options: 0 = fast, dumb, and quiet
 *          1 = checks, but quiet
 *          2 = verbose, checks
 * Overridden in _mall_init if DEBUG_MALLOC_LEVEL is set in the environment
 */
long int _mall_opt = 0;

/*
 * initialize stuff, we want to _malldmp() on a bus/seg error
 */

static _mall_sig(sig)
{
	if (sig == SIGSEGV) {
		_malldstr("\nsegmentation violation\n\n");
	} else if (sig == SIGBUS) {
		_malldstr("\nbus error\n\n");
	} else if (sig == SIGSYS) {
		_malldstr("\ninvalid syscall arg\n\n");
	} else {
		_malldstr("\nsignal ");
		_malldptr(sig);
		_malldstr("\n\n");
	}
	_malldmp();
	kill(getpid(), sig);
}

static _mall_init()
{
	char *p;

	if (_st_malloc)
		return;
	p = getenv("DEBUG_MALLOC_LEVEL");
	if ( p ) {
	     _mall_opt = atoi(p);
	}
	signal(SIGSEGV, _mall_sig);
	signal(SIGBUS, _mall_sig);
	_st_malloc = 1;
}

/*
 * figure out which allocation block this pointer came from
 * return NULL if none
 */

static struct _Dmi *_mallgb(s)
char *s;
{
	register struct _Dmi *blk;

	for (blk = _fab; blk != (struct _Dmi *) 0; blk = blk->m_next)
		if (blk->m_blk == s)
			break;
	return blk;
}

/*
 * internal: write a pointer in hex without using stdio
 */

static _malldptr(pc)
char *pc;
{
	register unsigned int x;
	auto char buf[20];
	static char hex[] = "0123456789abcdef";
	register long int dx;
	register char *p;

	x = (unsigned int) pc;
	if (x == 0)
		return _malldstr("0x0(0)");
	_malldstr("0x");
	p = buf;
	dx = x;
	while (x > 0)
		*p++ = hex[x % 16], x = x / 16;
	while (p != buf)
		write(2, --p, 1);
	_malldstr("(");
	p = buf;
	x = dx;
	while (x > 0)
		*p++ = hex[x % 10], x /= 10;
	while (p != buf)
		write(2, --p, 1);
	_malldstr(")");
}

/*
 * internal: dump a string
 */

static _malldstr(s)
register char *s;
{
	register long int len;

	for (len = 0; s[len] != '\0'; len++)
		;
	write(2, s, len);
}

/*
 * dump arena; can be called externally, and is non-destructive
 */

_malldmp()
{
	register struct _Dmi *blk;
	long int oldf;

	oldf = _in_malloc;
	_in_malloc = 0;
	_malldstr("brk = ");
	_malldptr(sbrk(0));
	_malldstr("  xbrk = ");
	_malldptr(_xbrk);
	_malldstr("\n_fab = ");
	_malldptr(_fab);
	_malldstr("  _ffb = ");
	_malldptr(_ffb);
	_malldstr("  blksiz = ");
	_malldptr(HEADSIZE);
	_malldstr("\netext = ");
	_malldptr(etext);
	_malldstr("  edata = ");
	_malldptr(edata);
	_malldstr("  end = ");
	_malldptr(end);
	if (_fab == (struct _Dmi *) 0) {
		_malldstr("no allocated blocks\n");
	} else {
		_malldstr("\nallocated blocks\n");
		for (blk = _fab; blk != (struct _Dmi *) 0 && (char *) blk >= _xbrk && (char *) blk < sbrk(0); blk = blk->m_next) {
			_malldstr("(");
			_malldptr(blk);
			_malldstr(") ");
			_malldstr(blk->m_file);
			_malldstr(":");
			_malldptr(blk->m_line);
			_malldstr(" ");
			_malldptr(blk->m_prev);
			_malldstr(" <");
			_malldptr(blk->m_size);
			_malldstr(">");
			_malldptr(blk->m_next);
			if (MAGIC_COOKIE != blk->m_cookie) {
				_malldstr(" cookie==");
				_malldptr(blk->m_cookie);
			}
			_malldstr("\n");
		}
		if (blk != (struct _Dmi *) 0)
			_malldstr("(subsequent block pointers corrupted)\n");
	}
	if (_ffb == (struct _Dmi *) 0) {
		_malldstr("\nno free blocks\n");
	} else {
		_malldstr("\nfree blocks\n");
		for (blk = _ffb; blk != (struct _Dmi *) 0 && (char *) blk >= _xbrk && (char *) blk < sbrk(0); blk = blk->m_next) {
			_malldstr("(");
			_malldptr(blk);
			_malldstr(")  ");
			_malldstr(blk->m_file);
			_malldstr(":");
			_malldptr(blk->m_line);
			_malldstr(" ");
			_malldptr(blk->m_prev);
			_malldstr("<  ");
			_malldptr(blk->m_size);
			_malldstr("  >");
			_malldptr(blk->m_next);
			_malldstr("cookie==");
			_malldptr(blk->m_cookie);
			_malldstr("\n");
		}
		if (blk != (struct _Dmi *) 0)
			_malldstr("(subsequent block pointers corrupted)\n");
	}
	_in_malloc = oldf;
}

/*
 * internal error routine: print error message (without using stdio) and
 * drop core
 */

static 
_mallerr(fn, s, ptr)
char *fn, *s;
long ptr;
{
	_malldstr(fn);
	_malldstr(": ");
	_malldstr(s);
	_malldptr(ptr);
	_malldstr("\n");
	_malldmp();
	signal(SIGQUIT, SIG_DFL);
	kill(getpid(), SIGQUIT);
}
	
static int
m_round(n)
int n;
{
	n |= 7;
	n++;
	return n;
} 


char *malloc_debug(n,file,line)
register unsigned n;
char *file;
int line;
{
	register struct _Dmi *blk;

	n = m_round(n);
	_in_malloc = 1;
	if (!_st_malloc) {
	   _mall_init();
	}
	if ( _mall_opt > 0) {
		if (_mall_opt > 1)
			_malldstr("called malloc("), _malldptr(n), _malldstr(")\n");
		_mallchk("malloc");
		if (n == 0) {
			_malldstr("malloc(0) is illegal!\n");
			_mall_sig(SIGSYS);
		}
	}
	for (blk = _ffb; blk != (struct _Dmi *) 0; blk = blk->m_next)
		if (blk->m_size >= n) {
			if (blk->m_next != (struct _Dmi *) 0)
				blk->m_next->m_prev = blk->m_prev;
			if (blk->m_prev != (struct _Dmi *) 0)
				blk->m_prev->m_next = blk->m_next;
			if (blk == _ffb)
				_ffb = blk->m_next;
			blk->m_next = _fab;
			blk->m_prev = (struct _Dmi *) 0;
			if (_fab != (struct _Dmi *) 0)
				_fab->m_prev = blk;
			_fab = blk;
			_in_malloc = 0;
			blk->m_file = file;
			blk->m_line = line;
			blk->m_wanted = n;
			blk->m_blk[blk->m_wanted] = END_BYTE;
			return (char *)blk->m_blk;
		}
	if ((blk = (struct _Dmi *) sbrk(HEADSIZE + n + 8)) == (struct _Dmi *) -1) {
		_in_malloc = 0;
		return (char *) 0;	/* no space */
	}
        if (((long int)blk) & 0x07) {
        	blk = (((long int)blk) | 0x07)+1;
	}
	if (_xbrk == (char *) 0)
		_xbrk = (char *) blk;
	blk->m_next = _fab;
	blk->m_prev = (struct _Dmi *) 0;
	blk->m_cookie = MAGIC_COOKIE;
	blk->m_size = n;
	if (_fab != (struct _Dmi *) 0)
		_fab->m_prev = blk;
	_fab = blk;
	_in_malloc = 0;
	blk->m_file = file;
	blk->m_line = line;
	blk->m_wanted = n;
	blk->m_blk[blk->m_wanted] = END_BYTE;
	return blk->m_blk;
}

char *malloc(n) 
unsigned int n;
{
    return malloc_debug(n, "unknown", 0);
}

/* The free-block list is sorted in size order */

free_debug(s,file,line)
register char *s;
char *file;
int line;
{
	register struct _Dmi *blk, *fblk;
	long int didit;

	_in_malloc = 1;
	if (_mall_opt > 0) {
		_mall_init();
		if (_mall_opt > 1) {
			_malldstr("called free("), _malldptr(s), _malldstr(")\n");
		}
		_mallchk("free");
		if (s == (char *) 0) {
			_malldstr("free((char *) 0) is illegal!\n");
			_mall_sig(SIGSYS);
		}
	}
	if ((blk = _mallgb(s)) == (struct _Dmi *) 0)
		_mallerr("non-allocated pointer passed to free(): ", s);
	if (_mall_opt > 1) {
		_malldstr("allocated: ");
		_malldstr(blk->m_file);
		_malldptr(blk->m_line);
		_malldstr("freed: ");
		_malldstr(file);
		_malldptr(line);
		_malldstr("\n");
	}
	blk->m_file = file;
	blk->m_line = line;
	if (blk->m_prev != (struct _Dmi *) 0)
		blk->m_prev->m_next = blk->m_next;
	if (blk->m_next != (struct _Dmi *) 0)
		blk->m_next->m_prev = blk->m_prev;
	if (blk == _fab)
		_fab = blk->m_next;
	if (_ffb == (struct _Dmi *) 0) {
		_ffb = blk;
		blk->m_next = (struct _Dmi *) 0;
		blk->m_prev = (struct _Dmi *) 0;
		goto crunch;
	}
	for (fblk = _ffb; fblk->m_next != (struct _Dmi *) 0; fblk = fblk->m_next)
		if (fblk->m_next->m_size >= blk->m_size)
			break;
	blk->m_next = fblk->m_next;
	if (fblk->m_next != (struct _Dmi *) 0)
		fblk->m_next->m_prev = blk;
	blk->m_prev = fblk;
	fblk->m_next = blk;

/*
 * crunch the free list by dropping consecutive end-of-brk until we hit xbrk
 * or a "hole" (i.e. allocated block).  coalescing is possible but not supp-
 * orted in malloc, so we don't bother here.
 */

crunch:
	didit = 1;
	while (_ffb != (struct _Dmi *) 0 && didit) {
		didit = 0;
		for (fblk = _ffb; fblk != (struct _Dmi *) 0; fblk = fblk->m_next)
			if ((char *) fblk + HEADSIZE + fblk->m_size - 1 == sbrk(0)) {
				didit = 1;
				if (fblk->m_next != (struct _Dmi *) 0)
					fblk->m_next->m_prev = fblk->m_prev;
				if (fblk->m_prev != (struct _Dmi *) 0)
					fblk->m_prev->m_next = fblk->m_next;
				if (fblk == _ffb)
					_ffb = fblk->m_next;
				sbrk(- fblk->m_size);
				break;
			}
	}
	_in_malloc = 0;
}

free(s)
register char *s;
{
    return free_debug(s,"unknown",0);
}


char *realloc_debug(s, n, file, line)
register char *s;
register unsigned n;
char *file;
int line;
{
	register char *s1, *d, *d1;
	register struct _Dmi *blk;

        if (_mall_opt > 0) {
		if (_mall_opt > 1)
			_malldstr("called realloc("), _malldptr(s), _malldstr(", "), _malldptr(n), _malldstr(")\n");
		_mallchk("realloc");
		if (s == (char *) 0) {
			_malldstr("realloc((char *) 0, size) is illegal!\n");
			_mall_sig(SIGSYS);
		}
		if (n == 0) {
			_malldstr("realloc(ptr, 0) is illegal!\n");
			_mall_sig(SIGSYS);
		}
	}
	n = m_round(n);
	if ((blk = _mallgb(s)) == (struct _Dmi *) 0)
		_mallerr("non-allocated pointer passed to realloc(): ", s);
	if ((s1 = malloc_debug(n,file,line)) == (char *) 0)
		return (char *) 0;
	if (blk->m_size < n)
		n = blk->m_size;
	d1 = s1;
	d = s;
	while (n-- != 0)
		*d1++ = *d++;
	free_debug(s,file,line);
	return s1;
}

char *realloc(s, n)
register char *s;
register unsigned n;
{
     return realloc_debug(s,n, "unknown", 0);
}

/*
 * _mallchk() is global, so external routines can do discreet checks on the
 * arena.  If the arena is detectibly corrupted, it will abort().
 */

_mallchk(fn)
char *fn;
{
	register struct _Dmi *blk, *cblk;
	register char *send;
	register long int cnt;

	send = sbrk(0);
	cblk = (struct _Dmi *) 0;
	for (blk = _fab; blk != (struct _Dmi *) 0; cblk = blk, blk = blk->m_next) {
		if ((char *) blk < _xbrk || (char *) blk >= send )
			_mallerr(fn, "allocated block list corrupted: blkptr = ", blk);
		if (blk->m_cookie != MAGIC_COOKIE )
			_mallerr(fn, "allocated block list corrupted, bad magic cookie: ", blk->m_cookie);
		if (blk->m_prev != cblk)
			_mallerr(fn, "allocated block list corrupted: back pointer incorrect blk ", blk);
		if (blk->m_size < 0)
			_mallerr(fn, "allocated block list corrupted: blk->m_size = ", blk->m_size);
	        if (blk->m_blk[blk->m_wanted] != END_BYTE) 
			_mallerr(fn, "allocated block list corrupted: bad end byte = ", blk->m_blk[blk->m_size]);
	}
	cblk = (struct _Dmi *) 0;
	for (blk = _ffb; blk != (struct _Dmi *) 0; cblk = blk, blk = blk->m_next) {
		if ((char *) blk < _xbrk || (char *) blk >= send)
			_mallerr(fn, "free block list corrupted: blkptr = ", blk);
		if (blk->m_cookie != MAGIC_COOKIE )
			_mallerr(fn, "free block list corrupted, bad magic cookie", blk->m_cookie);
		if (blk->m_prev != cblk)
			_mallerr(fn, "free block list corrupted: back pointer incorrect blk ", blk);
		if (blk->m_size < 0)
			_mallerr(fn, "free block list corrupted: blk->m_size = ", blk->m_size);
	}
	for (blk = _fab; blk != (struct _Dmi *) 0; blk = blk->m_next) {
		if ((char *) blk + HEADSIZE + blk->m_size - 1 > send) {
			_malldstr("(brk = ");
			_malldptr(send);
			_malldstr(", eblk = ");
			_malldptr((char *) blk + HEADSIZE + blk->m_size - 1);
			_malldstr(")\n");
			_mallerr(fn, "allocated block extends past brk: ", blk);
		}
		cnt = 0;
		for (cblk = _fab; cblk != (struct _Dmi *) 0; cblk = cblk->m_next) {
			if (blk == cblk)
				if (cnt++ == 0)
					continue;
				else
					_mallerr(fn, "block allocated twice: ", blk);
			if (blk > cblk && (char *) blk < (char *) cblk + HEADSIZE + cblk->m_size - 1) {
				_malldstr("(blk = ");
				_malldptr(blk);
				_malldstr(", cblk = ");
				_malldptr((char *) cblk + HEADSIZE + cblk->m_size - 1);
				_malldstr(")\n");
				_mallerr(fn, "nested block in allocated list: ", blk);
			}
		}
		for (cblk = _ffb; cblk != (struct _Dmi *) 0; cblk = cblk->m_next) {
			if (blk == cblk)
				_mallerr(fn, "block on allocated and free lists: ", blk);
			if (blk > cblk && (char *) blk < (char *) cblk + HEADSIZE + cblk->m_size - 1) {
				_malldstr("(blk = ");
				_malldptr(blk);
				_malldstr(", cblk = ");
				_malldptr((char *) cblk + HEADSIZE + cblk->m_size - 1);
				_malldstr(")\n");
				_mallerr(fn, "allocated block nested in free block: ", blk);
			}
		}
	}
	for (blk = _ffb; blk != (struct _Dmi *) 0; blk = blk->m_next) {
		if ((char *) blk + HEADSIZE + blk->m_size - 1 > send) {
			_malldstr("(brk = ");
			_malldptr(send);
			_malldstr(", eblk = ");
			_malldptr((char *) blk + HEADSIZE + blk->m_size - 1);
			_malldstr(")\n");
			_mallerr(fn, "free block extends past brk: ", blk);
		}
		cnt = 0;
		for (cblk = _ffb; cblk != (struct _Dmi *) 0; cblk = cblk->m_next) {
			if (blk == cblk)
				if (cnt++ == 0)
					continue;
				else
					_mallerr(fn, "block freed twice: ", blk);
			if (blk > cblk && (char *) blk < (char *) cblk +HEADSIZE + cblk->m_size - 1) {
				_malldstr("(blk = ");
				_malldptr(blk);
				_malldstr(", cblk = ");
				_malldptr((char *) cblk + HEADSIZE + cblk->m_size - 1);
				_malldstr(")\n");
				_mallerr(fn, "nested block in free list: ", blk);
			}
		}
		for (cblk = _fab; cblk != (struct _Dmi *) 0; cblk = cblk->m_next) {
			if (blk == cblk)
				_mallerr(fn, "block on allocated and free lists: ", blk);
			if (blk > cblk && (char *) blk < (char *) cblk + HEADSIZE + cblk->m_size - 1) {
				_malldstr("(blk = ");
				_malldptr(blk);
				_malldstr(", cblk = ");
				_malldptr((char *) cblk + HEADSIZE + cblk->m_size - 1);
				_malldstr(")\n");
				_mallerr(fn, "free block nested in allocated block: ", blk);
			}
		}
	}
}

/*
 * malloc objects and zero storage
 */

char *calloc(n, size)
register unsigned n, size; {
	register char *s, *s1;

	if (_mall_opt)
		_malldstr("called calloc("), _malldptr(n), _malldstr(", "), _malldptr(size), _malldstr(")\n");
	n *= size;
	if ((s = malloc(n)) == (char *) 0)
		return (char *) 0;
	for (s1 = s; n != 0; n--)
		*s1++ = 0;
	return s;
}


/*
 * for some reason this is in /lib/libc.a(calloc.o)
 */

cfree(s)
char *s; {
	free(s);
}

/*
 * hooks for Tcl...
 */

char *
Tcl_DbCkalloc(size, file, line) 
unsigned int size;
char *file;
int line;
{
	return malloc_debug(size, file, line);
}

Tcl_DbCkfree(ptr, file, line) 
char *ptr;
char *file;
int line;
{
	return free_debug(ptr,file,line);
}

char *
Tcl_DbCkrealloc(ptr, size, file, line)
unsigned int size;
char *ptr;
char *file;
int line;
{
	return realloc_debug(ptr,size,file,line);
}
