#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include <errno.h>
#include "dcap.h"

#define PNFS_ROOT getenv("DCACHE_ROOT")?getenv("DCACHE_ROOT"):"/pnfs/ktev/migrated_from_dlt/"
#define FILENAME_LEN 1024
#define O_BINARY 0
/* for getopt */
extern char *optarg;
extern int optind;

char *volumeName = NULL;

int getRecord(char *buffer, int buffer_size, int src)
{
  int n;
  int record_length;
  n = dc_read(src, &record_length, sizeof(int));
  n = dc_read(src, buffer, record_length);
  n = dc_read(src, &record_length, sizeof(int));
  return n>0?record_length:n;
}

int putRecord(char *buffer, int buffer_size, int dest)
{

  int n;
  n = dc_write(dest, &buffer_size, sizeof(int));
  n = dc_write(dest, buffer, buffer_size);
  n = dc_write(dest, &buffer_size, sizeof(int));
  return n>0?buffer_size:n;
}

void setVolumeName(const char *s)
{

   if( (s == NULL) || (getenv("VOLUME_NAME") != NULL) )
        return;

   if( volumeName != NULL )
        free(volumeName);
		
    volumeName = (char *)strdup(s);       
}

int verscmp( const void *     d1,      const void *     d2)
{
  return(__strverscmp((*(struct dirent **)d1)->d_name, (*(struct dirent **)d2)->d_name));
}

/* Compare S1 and S2 as strings holding indices/version numbers,
   returning less than, equal to or greater than zero if S1 is less than,
   equal to or greater than S2 (for more info, see the texinfo doc).
*/

int
__strverscmp (s1, s2)
     const char *s1;
     const char *s2;
{
#define  S_N    0x0
#define  S_I    0x4
#define  S_F    0x8
#define  S_Z    0xC

/* result_type: CMP: return diff; LEN: compare using len_diff/diff */
#define  CMP    2
#define  LEN    3
  const unsigned char *p1 = (const unsigned char *) s1;
  const unsigned char *p2 = (const unsigned char *) s2;
  unsigned char c1, c2;
  int state;
  int diff;

  /* Symbol(s)    0       [1-9]   others  (padding)
     Transition   (10) 0  (01) d  (00) x  (11) -   */
  static const unsigned int next_state[] =
  {
      /* state    x    d    0    - */
      /* S_N */  S_N, S_I, S_Z, S_N,
      /* S_I */  S_N, S_I, S_I, S_I,
      /* S_F */  S_N, S_F, S_F, S_F,
      /* S_Z */  S_N, S_F, S_Z, S_Z
  };

  static const int result_type[] =
  {
      /* state   x/x  x/d  x/0  x/-  d/x  d/d  d/0  d/-
                 0/x  0/d  0/0  0/-  -/x  -/d  -/0  -/- */

      /* S_N */  CMP, CMP, CMP, CMP, CMP, LEN, CMP, CMP,
                 CMP, CMP, CMP, CMP, CMP, CMP, CMP, CMP,
      /* S_I */  CMP, -1,  -1,  CMP, +1,  LEN, LEN, CMP,
                 +1,  LEN, LEN, CMP, CMP, CMP, CMP, CMP,
      /* S_F */  CMP, CMP, CMP, CMP, CMP, LEN, CMP, CMP,
                 CMP, CMP, CMP, CMP, CMP, CMP, CMP, CMP,
      /* S_Z */  CMP, +1,  +1,  CMP, -1,  CMP, CMP, CMP,
                 -1,  CMP, CMP, CMP
  };

  if (p1 == p2)
    return 0;

  c1 = *p1++;
  c2 = *p2++;
  /* Hint: '0' is a digit too.  */
  state = S_N | ((c1 == '0') + (isdigit (c1) != 0));

  while ((diff = c1 - c2) == 0 && c1 != '\0')
    {
      state = next_state[state];
      c1 = *p1++;
      c2 = *p2++;
      state |= (c1 == '0') + (isdigit (c1) != 0);
    }

  state = result_type[state << 2 | (((c2 == '0') + (isdigit (c2) != 0)))];

  switch (state)
  {
    case CMP:
      return -diff;

    case LEN:
      while (isdigit (*p1++))
        if (!isdigit (*p2++))
          return -1;

      return isdigit (*p2) ? 1 : -diff;

    default:
      return -state;
  }
}


int GetFile(char *volumeName, char fileName[FILENAME_LEN])
{
  char *path = malloc(strlen(volumeName) + strlen(PNFS_ROOT) + 11);
  char firstTwo[2], firstFour[4];
  static DIR  *dir = 0;
  struct dirent *dirent;
  static struct dirent **namelist;
  int static n_ent=-1;
  int i,j;
  struct stat stat_buf;

  strncpy(firstTwo, volumeName, 2);
  strncpy(firstFour, volumeName, 4);

  strcpy(path, PNFS_ROOT);
  strcat(path, "/");
  strncat(path, firstTwo, 2);
  strcat(path, "/");
  strncat(path, firstFour, 4);
  strcat(path, "/");
  strcat(path, volumeName);
  strcat(path, "/");
  if (n_ent == -1) /* first time access */
  {
    /* check if dir exists */
    if (i=stat(path, &stat_buf)!=0) {
	perror(path);
	free(path);
	return -1;
    }
    n_ent = scandir(path, &namelist, 0, verscmp);
    if (n_ent < 0) 
    {
      perror("scandir");
      n_ent = -1;
      free(path);
      return -1;
    }
    else {
      j=0;
      for (i = 0; i < n_ent; i++) {
	if ((strcmp(namelist[i]->d_name,".")==0)||(strcmp(namelist[i]->d_name,"..")==0)) {
	  free(namelist[i]);
	  j++;
	}
      }
      n_ent = n_ent - j;
      n_ent--;
    }
  }
  if (n_ent >= 0) {
    strncpy(fileName, path, FILENAME_LEN);
    strncat(fileName, namelist[n_ent]->d_name, FILENAME_LEN);
    free(namelist[n_ent]);
    n_ent--;
    if (n_ent == -1) n_ent--;
    free(path);
    return 1;
  }
  else {
    free(namelist);
    free(path);
    return 0;
  }

}


int PutFile(char *volumeName, char fileName[FILENAME_LEN])
{
  char *path = malloc(strlen(volumeName) + strlen(PNFS_ROOT) + 11);
  char firstTwo[2], firstFour[4];
  static int fileNum=0;

  strncpy(firstTwo, volumeName, 2);
  strncpy(firstFour, volumeName, 4);

  strcpy(path, PNFS_ROOT);
  strcat(path, "/");
  strncat(path, firstTwo, 2);
  if( !opendir(path) ) {
    if( mkdir(path, S_IRUSR|S_IWUSR|S_IXUSR) < 0) {
      free(path);
      return -1;
    }
  }
  strcat(path, "/");
  strncat(path, firstFour, 4);
  if( !opendir(path) ) {
    if( mkdir(path, S_IRUSR|S_IWUSR|S_IXUSR) < 0) {
      free(path);
      return -1;
    }
  }
  strcat(path, "/");
  strcat(path, volumeName);
  if( !opendir(path) ) {
    if( mkdir(path, S_IRUSR|S_IWUSR|S_IXUSR) < 0) {
      free(path);
      return -1;
    }
  }
  strcat(path, "/");
  sprintf(firstFour, "f%d", fileNum);
  strcat(path, firstFour);
  strncpy(fileName, path, FILENAME_LEN);
  fileNum ++;
  free(path);
  return 0;
}
