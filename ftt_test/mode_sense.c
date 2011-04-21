/*
 * Uses sg kernel interface to issue mode sense select commands
 * to read page 0x25 and to set AMC bit for T10000C tape drive
 *
 */

#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/ioctl.h>
#include <scsi/sg.h>
#include <scsi/scsi.h>
 
#define MODE_SENSE_REPLY_LEN 0x1e + 4
#define MODE_SENSE_CMD_CODE 0x1a
#define MODE_SENSE_CMD_LEN 6
#define DEVICE_STATUS_PAGE 0x25
#define DATA_HDR_LENGTH 0x4
#define MODE_SELECT_CMD_CODE 0x15
#define AMC_OFFSET 5
#define DFD_OFFSET 8

#define MODE_SELECT_CMD_CODE 0x15


void print_buf(unsigned char *buf, int len) {
  int k;
  for (k=0; k < len; ++k) {
	if (k>0 && (0 == (k % 16))) {
	  fprintf(stderr, "\n");
	}
	fprintf(stderr, "%02x ", buf[k]);
  }
  fprintf(stderr, "\n");
}



int main(int argc, char **argv) {
  int sg_fd, k;
  unsigned char modSenseCmdBlk[MODE_SENSE_CMD_LEN] = 
    {MODE_SENSE_CMD_CODE, 0x8, DEVICE_STATUS_PAGE, 0, 0x1e + 4, 0};
  unsigned char modSelectCmdBlk[MODE_SENSE_CMD_LEN] = 
    {MODE_SELECT_CMD_CODE, 0x10, 0, 0, 0x1e + 6, 0};
  /*    {MODE_SELECT_CMD_CODE, 0x10, 0, 0, 0x1e + 4, 0}; */

  unsigned char modSenseBuff[MODE_SENSE_REPLY_LEN];
  unsigned char sense_buffer[32];
  sg_io_hdr_t io_hdr;
  unsigned short length;
  unsigned char pageCode;

  if (argc < 2) {
    fprintf(stderr, "Usage: %s sg_device\n", argv[0]);
    exit(1);
  }

  if (argc == 3) {
    sscanf(argv[2], "%i", &pageCode);
    fprintf(stderr, "Setting Page Code = %02x\n", pageCode);
    modSenseCmdBlk[2] = pageCode;
  }

  printf("AM: openinig %s\n",argv[1]); 
  /*  if ((sg_fd = open(argv[1], O_RDONLY)) < 0) { */
  if ((sg_fd = open(argv[1], O_RDWR | O_NONBLOCK)) < 0) {
    perror("Error opening scsi device");
    exit(1);
  }
  printf("AM:AA\n");
  if ((ioctl(sg_fd, SG_GET_VERSION_NUM, &k) < 0) || (k < 30000)) {
    fprintf(stderr, "%s is not an sg device, or old sg driver\n", argv[1]);
    exit(1);
  }
  printf("AM:BB\n");
  memset(&io_hdr, 0, sizeof(sg_io_hdr_t));
  io_hdr.interface_id = 'S';
  io_hdr.cmd_len = sizeof(modSenseCmdBlk);
  io_hdr.mx_sb_len = sizeof(sense_buffer);
  io_hdr.dxfer_direction = SG_DXFER_FROM_DEV;
  io_hdr.dxfer_len = MODE_SENSE_REPLY_LEN;
  io_hdr.dxferp = modSenseBuff;
  io_hdr.cmdp = modSenseCmdBlk;
  io_hdr.sbp = sense_buffer;
  io_hdr.timeout = 2000;         /* 2000 millsecs = 2 seconds */

  printf("AM: sending cmd\n");
  print_buf(modSenseCmdBlk, 6);
  if (ioctl(sg_fd, SG_IO, &io_hdr) < 0) {
    perror("sg Request Sense SG_IO ioctl error");
    exit(1);
  }
  printf("AM: sending cmd done\n");
  
  if ((io_hdr.info & SG_INFO_OK_MASK) != SG_INFO_OK) {
    printf("AM: io_hdr.sb_len_wr %d\n", io_hdr.sb_len_wr);
    if (io_hdr.sb_len_wr > 0) {
      fprintf(stderr, "REQUEST SENSE sense data: \n ");
      for (k=0; k < io_hdr.sb_len_wr; ++k) {
	if (k>0 && (0 == (k % 10))) {
	  fprintf(stderr, "\n ");
	}
	fprintf(stderr, "%02x ", sense_buffer[k]);
      }
      fprintf(stderr, "\n");
    }
    else {
      fprintf(stderr, "Problem with REQUEST SENSE\n");
    }
    exit(1);
  }

  printf("Mode Sense Buf\n");
  print_buf(modSenseBuff, MODE_SENSE_REPLY_LEN);
  printf("Sense Buf\n");
  print_buf(sense_buffer, 32);

  /* read changeable values */
  modSenseCmdBlk[2] = modSenseCmdBlk[2] | 0x40;
  printf("AM: sending cmd\n");
  print_buf(modSenseCmdBlk, 6);

  if (ioctl(sg_fd, SG_IO, &io_hdr) < 0) {
    perror("sg Request Sense SG_IO ioctl error");
    exit(1);
  }
  printf("AM: sending cmd done\n");
  
  if ((io_hdr.info & SG_INFO_OK_MASK) != SG_INFO_OK) {
    printf("AM: io_hdr.sb_len_wr %d\n", io_hdr.sb_len_wr);
    if (io_hdr.sb_len_wr > 0) {
      fprintf(stderr, "REQUEST SENSE sense data: \n ");
      for (k=0; k < io_hdr.sb_len_wr; ++k) {
	if (k>0 && (0 == (k % 10))) {
	  fprintf(stderr, "\n ");
	}
	fprintf(stderr, "%02x ", sense_buffer[k]);
      }
      fprintf(stderr, "\n");
    }
    else {
      fprintf(stderr, "Problem with REQUEST SENSE\n");
    }
    exit(1);
  }

  printf("Mode Sense Buf\n");
  print_buf(modSenseBuff, MODE_SENSE_REPLY_LEN);
  printf("Sense Buf\n");
  print_buf(sense_buffer, 32);



  printf("Sending Mode Select\n");
  
  modSenseBuff[0] = 0;
  modSenseBuff[1] = 0;
  modSenseBuff[2] = 0x10;
  modSenseBuff[3] = 0x00;
  modSenseBuff[DATA_HDR_LENGTH+AMC_OFFSET] = 0x01;
  modSenseBuff[DATA_HDR_LENGTH+DFD_OFFSET] = 0x00;
  print_buf(modSenseBuff, 0x1e + 4);

  io_hdr.dxfer_direction = SG_DXFER_TO_DEV;
  /*  io_hdr.dxfer_len = 0x1e + 6; */
  io_hdr.dxfer_len = 0x1e + 6;
  io_hdr.cmdp = modSelectCmdBlk;

  print_buf(modSelectCmdBlk, 6);
  if (ioctl(sg_fd, SG_IO, &io_hdr) < 0) {
    perror("sg Request Sense SG_IO ioctl error");
    exit(1);
  }
  printf("AM: sending cmd done\n");
  
  if ((io_hdr.info & SG_INFO_OK_MASK) != SG_INFO_OK) {
    printf("AM: io_hdr.sb_len_wr %d\n", io_hdr.sb_len_wr);
    if (io_hdr.sb_len_wr > 0) {
      fprintf(stderr, "REQUEST SENSE sense data: \n ");
      for (k=0; k < io_hdr.sb_len_wr; ++k) {
	if (k>0 && (0 == (k % 10))) {
	  fprintf(stderr, "\n ");
	}
	fprintf(stderr, "%02x ", sense_buffer[k]);
      }
      fprintf(stderr, "\n");
    }
    else {
      fprintf(stderr, "Problem with REQUEST SENSE\n");
    }
    exit(1);
  }

  sleep(3);
  printf("AM: sending Mode Sense cmd\n");
  modSenseCmdBlk[2] = DEVICE_STATUS_PAGE;
  io_hdr.dxfer_direction = SG_DXFER_FROM_DEV;
  io_hdr.dxfer_len = 0x1e + 4;
  io_hdr.cmdp = modSenseCmdBlk;
  print_buf(modSenseCmdBlk, 6);
  if (ioctl(sg_fd, SG_IO, &io_hdr) < 0) {
    perror("sg Request Sense SG_IO ioctl error");
    exit(1);
  }
  printf("AM: sending cmd done\n");
  
  if ((io_hdr.info & SG_INFO_OK_MASK) != SG_INFO_OK) {
    printf("AM: io_hdr.sb_len_wr %d\n", io_hdr.sb_len_wr);
    if (io_hdr.sb_len_wr > 0) {
      fprintf(stderr, "REQUEST SENSE sense data: \n ");
      for (k=0; k < io_hdr.sb_len_wr; ++k) {
	if (k>0 && (0 == (k % 10))) {
	  fprintf(stderr, "\n ");
	}
	fprintf(stderr, "%02x ", sense_buffer[k]);
      }
      fprintf(stderr, "\n");
    }
    else {
      fprintf(stderr, "Problem with REQUEST SENSE\n");
    }
    exit(1);
  }


  printf("Mode Sense Buf\n");
  print_buf(modSenseBuff, MODE_SENSE_REPLY_LEN);
  printf("Sense Buf\n");
  print_buf(sense_buffer, 32);


  printf("Sending Mode Select\n");
  
  modSenseBuff[0] = 0;
  modSenseBuff[1] = 0;
  modSenseBuff[2] = 0x10;
  modSenseBuff[3] = 0x00;
  modSenseBuff[DATA_HDR_LENGTH+AMC_OFFSET] = 0x00;
  modSenseBuff[DATA_HDR_LENGTH+DFD_OFFSET] = 0x00;
  print_buf(modSenseBuff, 0x1e + 4);

  io_hdr.dxfer_direction = SG_DXFER_TO_DEV;
  /*  io_hdr.dxfer_len = 0x1e + 6; */
  io_hdr.dxfer_len = 0x1e + 6;
  io_hdr.cmdp = modSelectCmdBlk;

  print_buf(modSelectCmdBlk, 6);
  if (ioctl(sg_fd, SG_IO, &io_hdr) < 0) {
    perror("sg Request Sense SG_IO ioctl error");
    exit(1);
  }
  printf("AM: sending cmd done\n");
  
  if ((io_hdr.info & SG_INFO_OK_MASK) != SG_INFO_OK) {
    printf("AM: io_hdr.sb_len_wr %d\n", io_hdr.sb_len_wr);
    if (io_hdr.sb_len_wr > 0) {
      fprintf(stderr, "REQUEST SENSE sense data: \n ");
      for (k=0; k < io_hdr.sb_len_wr; ++k) {
	if (k>0 && (0 == (k % 10))) {
	  fprintf(stderr, "\n ");
	}
	fprintf(stderr, "%02x ", sense_buffer[k]);
      }
      fprintf(stderr, "\n");
    }
    else {
      fprintf(stderr, "Problem with REQUEST SENSE\n");
    }
    exit(1);
  }

 
  return 0;
}
	
