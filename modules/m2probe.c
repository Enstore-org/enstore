#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include "ftt.h"

#define DEVICE "/dev/rmt/tps2d0n"

typedef struct m2_status
{
	char dump_type[44];
	char firmware_revision[12];
	char firmware_release_date[36];
	char boot_version[36];
	char id[50];
	char configuration[14];
	char serial_number[28];
	char status[36];
}	M2Status;

void show_m2_status(s)
M2Status *s;
{
	printf("            Dump Type: %s\n", s->dump_type);
	printf("    Firmware Revision: %s\n", s->firmware_revision);
	printf("Firmware Release Date: %s\n", s->firmware_release_date);
	printf("         Boot Version: %s\n", s->boot_version);
	printf("Configuration Version: %s\n", s->configuration);
	printf("        Serial Number: %s\n", s->serial_number);
	if (!strncmp(s->status, "\006-\007", 3))
	{
		*(s->status) = 'o';
		*(s->status+2) = 'o';
	}
	printf("               Status: %s\n",
		strncmp(s->status, "    ", 4)?s->status:s->status+4);
}

/* Log Entry */

typedef struct log_entry	/* log entry structure */
{
	unsigned char code;
	char *name;
	int len;
}	LogEntry;

/* The index in tape_history_parameters is the parameter code! */

LogEntry tape_history_parameters[] =
{
	{0x00, "Length of this list",			40},
	{0x01, "Tape ID",				8},
	{0x02, "Current Blocks Written",		4},
	{0x02, "Current Blocks Rewritten",		4},
	{0x04, "Current Blocks Read",			4},
	{0x05, "Current Blocks ECC'd",			4},
	{0x06, "Current Write Retries",			2},
	{0x07, "Current Read Retries",			2},
	{0x08, "Current Tracking Retries",		2},
	{0x09, "Current Data Underruns",		2},
	{0x0a, "Current Data Overruns",			2},
	{0x0b, "Current Rewinds",			2},
	{0x0c, "Current Max Temperature",		1},
	{0x0d, "Current Drive Serial Number",		4},
	{0x0e, "Previous Blocks Written",		4},
	{0x0f, "Previous Blocks Rewritten",		4},
	{0x10, "Previous Blocks Read",			4},
	{0x11, "Previous Blocks ECC'd",			4},
	{0x12, "Previous Write Retries",		2},
	{0x13, "Previous Read Retries",			2},
	{0x14, "Previous Tracking Retries",		2},
	{0x15, "Previous Data Underruns",		2},
	{0x16, "Previous Data Overruns",		2},
	{0x17, "Previous Rewinds",			2},
	{0x18, "Previous Max Temperature",		1},
	{0x19, "Previous Drive Serial Number",		4},
	{0x1a, "Lifetime Blocks Written",		5},
	{0x1b, "Lifetime Blocks Rewritten",		5},
	{0x1c, "Lifetime Blocks Read",			5},
	{0x1d, "Lifetime Blocks ECC'd",			5},
	{0x1e, "Lifetime Write Retries",		4},
	{0x1f, "Lifetime Read Retries",			4},
	{0x20, "Lifetime Tracking Retries",		4},
	{0x21, "Lifetime Data Underruns",		4},
	{0x22, "Lifetime Data Overruns",		4},
	{0x23, "Lifetime Rewinds",			4},
	{0x24, "Lifetime Max Temperature",		1},
	{0x25, "Lifetime Load Count",			4},
	{0x26, "Lifetime Maximum Tape Pass Count",	4},
	{0x27, "Lifetime SmartClean Cycles",		4}
};

/* Drive Usage */

LogEntry drive_usage_parameters[] =
{
	{0x00, "Length of this list",		21},
	{0x01, "Total Blocks Written",		8},
	{0x02, "Total Blocks Rewritten",	8},
	{0x03, "Total Blocks Read",		8},
	{0x04, "Total ECC Corrections",		8},
	{0x05, "Total Blocks Reread",		8},
	{0x06, "Total Load Count",		3},
	{0x07, "Minutes Since Last Clean",	3},
	{0x08, "Minutes of Powered Time",	3},
	{0x09, "Minutes of Tensioned Time",	3},
	{0x0a, "Cleaning Count",		2},
	{0x0b, "Vendor Unique",			2},
	{0x0c, "Vendor Unique",			2},
	{0x0d, "Vendor Unique",			2},
	{0x0e, "Vendor Unique",			2},
	{0x0f, "Vendor Unique",			2},
	{0x10, "Vendor Unique",			2},
	{0x11, "Time to Clean",			1},
	{0x12, "Vendor Unique",			1},
	{0x13, "Reserved",			3},
	{0x14, "Reserved",			3}
};

/* Write Errors */

LogEntry write_error_parameters[] =
{
	{0x00, "Length of this list",		7},
	{0x01, "dummy",			0},
	{0x02, "Total Rewrites",		3},
	{0x03, "Total Errors Corrected",	3},
	{0x04, "Total Times Errors Processed",	3},
	{0x05, "Total Bytes Processed",		5},
	{0x06, "Total Unrecoverable Errors",	2}
};

/* Read Errors */

LogEntry read_error_parameters[] =
{
	{0x00, "Length of this list",		7},
	{0x01, "dummy",			0},
	{0x02, "Total Rereads",			3},
	{0x03, "Total Errors Corrected",	3},
	{0x04, "Total Times Errors Processed",	3},
	{0x05, "Total Bytes Processed",		5},
	{0x06, "Total Unrecoverable Errors",	2}
};

/* show_tape_history(buf) -- print parameters of tape history */

void show_tape_history(buf)
unsigned char *buf;
{
	unsigned char *ep;
	LogEntry *tp;
	int code;

	if (*buf != 0x35)	/* not a tape history log page */
		return;

	ep = buf + n_byte2int(buf+2, 2);

	buf += 4;		/* skip Parameter List Header */

	while (buf < ep)
	{
		code = n_byte2int(buf, 2);
		tp = &tape_history_parameters[code];
		printf("%32s: ", tp->name);
		if (code == 1)	/* Tape ID */
		{
			printf("%08d-%03d\n", n_byte2int(buf+8, 4),
				n_byte2int(buf+4, 4));
		}
		else
		{
			printf("%d\n", n_byte2int(buf+4, tp->len));
		}
		buf += tp->len + 4;
	}
	printf("\n");
}
		
/* show_log(buf, parameter_list) -- print parameters of generic log */

void show_log(buf, parameter_list)
unsigned char *buf;
LogEntry parameter_list[];
{
	unsigned char *ep;
	LogEntry *tp;
	int code;
	char *title;

	switch(*buf)
	{
	case 0x02:
		title = "Write Error Counters";
		break;
	case 0x03:
		title = "Read Error Counters";
		break;
	case 0x2e:
		title = "Tape Alert";
		break;
	case 0x35:
		title = "Tape History Log";
		break;
	case 0x39:
		title = "Data Compression";
		break;
	case 0x3c:
		title = "Drive Usage Information";
		break;
	case 0x3e:
		title = "Drive Temperature";
		break;
	defaults:
		return;		/* not a valid log page */
	}

	printf("%s:\n", title);

	ep = buf + n_byte2int(buf+2, 2);

	buf += 4;		/* skip Parameter List Header */

	while (buf < ep)
	{
		code = n_byte2int(buf, 2);
		tp = &parameter_list[code];
		printf("%32s: ", tp->name);
		printf("%d\n", n_byte2int(buf+4, tp->len));
		buf += tp->len + 4;
	}
	printf("\n");
}
		
/* Command Descriptor Block */

/* READ BUFFER DISCRIPTOR */
static unsigned char ftt_cdb_read_buffer_descriptor[] =
	{0x3c, 0x03, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00};

/* READ BUFFER */
static unsigned char ftt_cdb_read_buffer[] =
	{0x3c, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00};

/* M2 STATUS is just a READ BUFFER taking the first 256 bytes */
static unsigned char ftt_cdb_m2_status[] =
	{0x3c, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00};

/* INQUIRY */
static unsigned char ftt_cdb_inquiry[] =
	{0x12, 0x00, 0x00, 0x00, 106, 0x00};

/* REQUEST SENSE */
static unsigned char ftt_cdb_request_sense[] =
	{0x03, 0x00, 0x00, 0x00, 0x20, 0x00};

/* LOG SENSE */
static unsigned char ftt_cdb_log_sense[] =
	{0x4d, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00};

/* TAPE HISTORY -- a LOG SENSE on tape history */

static unsigned char ftt_cdb_tape_history[] =
	{0x4d, 0x00, 0x75, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00};

/* DRIVE USAGE -- a LOG SENSE on drive usage */
static unsigned char ftt_cdb_drive_usage[] =
	{0x4d, 0x00, 0x7c, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00};

/* WRITE ERRORS -- a LOG SENSE on write errors */
static unsigned char ftt_cdb_write_errors[] =
	{0x4d, 0x00, 0x42, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00};

/* READ ERRORS -- a LOG SENSE on read errors */
static unsigned char ftt_cdb_read_errors[] =
	{0x4d, 0x00, 0x43, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00};

/* ERASE */
static unsigned char ftt_cdb_erase[] =
	{0x19, 0x01, 0x00, 0x00, 0x00, 0x00};

/* REWIND */
static unsigned char ftt_cdb_rewind[] =
	{0x01, 0x00, 0x00, 0x00, 0x00, 0x00};

/* UNLOAD */
static unsigned char ftt_cdb_unload[] =
	{0x1b, 0x00, 0x00, 0x00, 0x00, 0x00};

/* LOAD */
static unsigned char ftt_cdb_load[] =
	{0x1b, 0x00, 0x00, 0x00, 0x01, 0x00};

/* SEND DIAGNOSTIC */
static unsigned char ftt_cdb_send_diagnostic[] =
	{0x1d, 0x04, 0x00, 0x00, 0x00, 0x00};

/* TEST UNIT READY */
static unsigned char ftt_cdb_test_unit_ready[] =
	{0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

/* RECEIVE DISGNOSTIC RESULTS */
static unsigned char ftt_cdb_receive_diagnostic_results[] =
	{0x1c, 0x00, 0x00, 0x01, 0x00, 0x00};

/* three_byte2int(s) -- convert 3 byte integer value into int */

int three_byte2int(s)
unsigned char *s;
{
	return ((s[0] * 256) + s[1]) * 256 + s[2];
}

/* n_byte2int(s, n) -- convert n byte integer value into int */

int n_byte2int(s, n)
unsigned char *s;
int n;
{
	int res = 0;

	for (; n; n--)
		res = res * 256 + *s++;

	return(res);
}

/* int2three_byte(s, n) -- convert int into 3 byte integer value */

unsigned char *int2three_byte(s, n)
unsigned char *s;
int n;
{
	*(s) = (unsigned char) (n / 65536);
	*(s+1) = (unsigned char) ((n % 65536) / 256);
	*(s+2) = (unsigned char) (n % 256);

	return(s);
}

void hexdump(buf, len)
unsigned char *buf;
int len;
{
	int i;
	char ascii[17];
	unsigned char c;

	ascii[16] = '\0';
	for (i = 0; i < len; i++)
	{
		if ((i % 16) == 0)
		{
			printf("%08x --", i);
		}
		printf(" %02x", (c = *(buf+i)));
		ascii[i % 16] = isprint(c)?c:'.';
		if ((i % 16) == 15)
			printf(" -- %s\n", ascii);
	}
	printf("\n");
}

void show_scsi_command(buf, len)
char *buf;
int len;
{
	int i;

	printf("SCSI Command: ");
	for (i = 0; i < len; i++)
	{
		printf(" %02x", (unsigned char) buf[i]);
	}
	printf("\n\n");
}

/* dump_code(d, f) -- have a code dump of the drive into file f */

#define BUFSIZE 2048
#define DUMP_PREFIX "Fermilab"

void dump_code(d, f, prefix)
ftt_descriptor d;
char *f;
char *prefix;
{
	char buf[8192];
	int fd, res, len, offset, l2;
	char of[256];			/* output file */
	char serial[256];
	time_t t;
	struct tm *t1;

	if (!prefix)	/* use default */
	{
		prefix = (char *) DUMP_PREFIX;
	}

	/* figure out output file name */

	if (!f)
	{
		/* get time */

		t = time(0);
		t1 = localtime(&t);

		/* get serial number */

		res = ftt_do_scsi_command(d, "Inquiry", ftt_cdb_inquiry,
			6, buf, 128, 10, 0);

		strncpy(serial, buf+96, 10);
		serial[10] = '\0';

		sprintf(of, "%s-%s-%02d%02d%02d-%02d%02d.dmp",
			prefix, serial, t1->tm_year-100,
			t1->tm_mon+1, t1->tm_mday, t1->tm_hour,
			t1->tm_min);
		f = of;
	}

	res = ftt_do_scsi_command(d, "Read Buffer Descriptor",
		ftt_cdb_read_buffer_descriptor, 10, buf, 256, 10, 0);

	len = three_byte2int(buf+1);

	fd = open(f, O_WRONLY|O_CREAT, S_IRWXU|S_IRGRP|S_IROTH);

	offset = 0;
	while (len > 0)
	{
		l2 = (len > BUFSIZE)?BUFSIZE:len;

		int2three_byte(ftt_cdb_read_buffer+3, offset);
		int2three_byte(ftt_cdb_read_buffer+6, l2);
		res = ftt_do_scsi_command(d, "Read Buffer",
			ftt_cdb_read_buffer, 10, buf, l2, 10, 0);
		write(fd, buf, res);
		offset += res;
		len -= res;
	}
	printf("%d bytes dumped to %s\n", offset, f);
	close(fd);
}
	
/* is_m2(d) -- to see if device d is a Mammoth-2 drive */

int is_m2(d)
ftt_descriptor d;
{
	char buf[256];
	int res;

	/* do a scsi inquiry to see the vendor and model */

	res = ftt_do_scsi_command(d, "Inquiry", ftt_cdb_inquiry, 6, buf,
		128, 10, 0);

	if (strncmp(buf+8, "EXABYTE ", 8) ||
		strncmp(buf+16, "Mammoth2        ", 16))
	{
		return(0);
	}

	return(1);
}

/* get_status(d, buf) -- get status of drive d */

M2Status *get_status(d, buf)
ftt_descriptor d;
char *buf;
{
	int res;

	res = ftt_do_scsi_command(d, "Read Buffer", ftt_cdb_read_buffer,
		10, buf, 256, 10, 0);

	/* fix the end of dump type */

	*strchr(buf+16, ' ') = '\0';

	return (M2Status *) buf;
}

/* get_tape_history(d, buf) -- a sense log on tape history */

unsigned char *get_tape_history(d, buf)
ftt_descriptor d;
unsigned char *buf;
{
	int res;

	res = ftt_do_scsi_command(d, "Sense Log", ftt_cdb_tape_history,
		10, buf, 1024, 10, 0);

	return (buf);
}

/* get_drive_usage(d, buf) -- a sense log on drive usage */

unsigned char *get_drive_usage(d, buf)
ftt_descriptor d;
unsigned char *buf;
{
	int res;

	res = ftt_do_scsi_command(d, "Sense Log", ftt_cdb_drive_usage,
		10, buf, 1024, 10, 0);

	return (buf);
}

/* get_log(d, buf, pcode) -- generic sense log on pcode */

unsigned char *get_log(d, buf, pcode)
ftt_descriptor d;
unsigned char *buf, pcode;
{
	unsigned char ftt_cdb_log_sense[] =
		{0x4d, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00};
	int res;

	ftt_cdb_log_sense[2] |= pcode;
	
	res = ftt_do_scsi_command(d, "Sense Log", ftt_cdb_log_sense,
		10, buf, 1024, 10, 0);

	return (buf);
}

/* get_tape_id(d, buf) -- a sense log on tape history */

char *get_tape_id(d, buf)
ftt_descriptor d;
char *buf;
{
	int res;
	unsigned char buf2[256];

	res = ftt_do_scsi_command(d, "Sense Log", ftt_cdb_tape_history,
		10, buf2, 256, 10, 0);

	sprintf(buf, "%08d-%03d", n_byte2int(buf2+12, 4),
		n_byte2int(buf2+8, 4));

	return (buf);
}

/* try_scsi(d, cmd, len, l2) -- show scsi command and dump its result
 *				This is meant for a debugging function
 */

void try_scsi(d, cmd, len, l2)
ftt_descriptor d;
char *cmd;
int len, l2;
{
	unsigned char buffer[1048576];
	int res;

	memset(buffer, 0, l2);
	show_scsi_command(cmd, len);
	res = ftt_do_scsi_command(d, "Generic SCSI command", cmd, len, buffer,
		l2, 10, 0);
	hexdump(buffer, l2);
}

/* usage() -- show usage */

void usage(s)
char *s;
{
	printf("\nusage:\n\n%s [-d] [-t] [-u] [-r] [-w] [-p prefix] [-o dump_file] [device]\n", s);
	printf("	-h		print usage\n");
	printf("	-d		make a dump\n");
	printf("	-t		show tape history\n");
	printf("	-u		show drive usage infromation\n");
	printf("	-w		show write errors\n");
	printf("	-r		show read errors\n");
	printf("	-p prefix	use explicit prefix for dump file\n");
	printf("	-o dump_file	use explicit dump file name\n");
	printf("	device		use explicit device name\n\n");
	printf("Default device is the first of /dev/rmt/tps*n\n");
	printf("Default prefix is %s\n", DUMP_PREFIX);
	printf("Default dump file is <prefix>-<serial_number>-<yymmdd-hhmm>.dmp\n\n");
}

main(argc, argv)
int argc;
char **argv;
{
	int res;
	char *device, *output_file, *prefix;
	char devpath[256];
	char of[256];
	char prefix_buf[256];
	unsigned char buf[8192];
	struct tm *t1;
	time_t t;
	ftt_descriptor d;
	DIR *dir;
	struct dirent *dp;
	int c;
	int make_dump, list_tape_history, drive_info, read_error, write_error;

	device = NULL;
	output_file = NULL;
	prefix = NULL;
	make_dump = list_tape_history = drive_info = read_error = write_error = 0;
/*
	if (argc > 1) device = argv[1];
	if (argc > 2) output_file = argv[2];
*/
	while ((c = getopt(argc, argv, "dhturwp:o:")) != -1)
	{
		switch(c)
		{
		case 'd':
			make_dump = 1;
			break;
		case 'u':
			drive_info = 1;
			break;
		case 't':
			list_tape_history = 1;
			break;
		case 'h':
			usage(argv[0]);
			exit(0);
			break;
		case 'r':
			read_error = 1;
			break;
		case 'w':
			write_error = 1;
			break;
		case 'p':
			strcpy(prefix_buf, optarg);
			prefix = prefix_buf;
			break;
		case 'o':
			strcpy(of, optarg);
			output_file = of;
			break;
		default:
			usage(argv[0]);
			exit(1);
		}
	}

	if (optind < argc)
		device = argv[optind];

	if (device == NULL)	/* find the first /dev/rmt/tps*n */
	{
		if ((dir = opendir("/dev/rmt")) == NULL)
		{
			printf("%s: can not find /dev/rmt\n", argv[0]);
			exit(1);
		}

		while ((dp = readdir(dir)) != NULL)
		{
			if (!strncmp(dp->d_name, "tps", 3) &&
				(dp->d_name[strlen(dp->d_name)-1] == 'n'))
			{
				sprintf(devpath, "/dev/rmt/%s", dp->d_name);
				device = (char *) devpath;
				break;
			}
		}
	}

	if (device == NULL)
	{
		printf("%s: can not find a proper device\n", argv[0]);
		exit(1);
	}

	if (access(device, R_OK|W_OK))
	{
		printf("%s: can not open \"%s\"\n", argv[0], device);
		exit(1);
	}

	d = ftt_open(device, FTT_RDONLY);

	if (!is_m2(d))
	{
		printf("%s is not a Mammoth-2 drive\n", device);
		ftt_close(d);
		exit(1);
	}

	show_m2_status(get_status(d, buf));
	printf("              Tape ID: %s\n", get_tape_id(d, buf));
	printf("\n");

	if (make_dump)
	{
		dump_code(d, output_file, prefix);
	}

	if (list_tape_history)
	{
		printf("Tape history:\n");
		show_tape_history(get_tape_history(d, buf));
	}

	if (drive_info)
	{
		show_log(get_log(d, buf, 0x3c), drive_usage_parameters);
	}

	if (read_error)
	{
		show_log(get_log(d, buf, 0x03), read_error_parameters);
	}

	if (write_error)
	{
		show_log(get_log(d, buf, 0x02), write_error_parameters);
	}

	ftt_close(d);
	exit(0);
	try_scsi(d, ftt_cdb_request_sense, 6, 256);
	try_scsi(d, ftt_cdb_read_buffer, 10, 256);
	try_scsi(d, ftt_cdb_inquiry, 6, 256);
	try_scsi(d, ftt_cdb_send_diagnostic, 6, 256);
	try_scsi(d, ftt_cdb_log_sense, 10, 256);

	ftt_close(d);
}
