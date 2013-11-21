/* 
** Block descriptor silliness
** For AIT drives, we currently need to enable block descriptors
** because we can't do a mode select without one, so we do our
** mode sense calls with DBD off, and leave more room for the
** block descriptor.
**
*/
#define DO_DBD 
#ifdef DO_DBD
# define DBD 0x00
# define BD_SIZE 0xC
#else
# define DBD 0x08
# define BD_SIZE 0x4
#endif

