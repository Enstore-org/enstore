/*
 ****************************Copyright Notice***********************************
 *             Copyright (c)1992 Universities Research Association, Inc.,      *
 *                   Lauri Loebel and Marc W. Mengel                           *
 *                         All Rights Reserved                                 *
 *******************************************************************************
 ***********************Government Sponsorship Notice***************************
 * This material resulted from work developed under a Government Contract and  *
 * is subject to the following license:  The Government retains a paid-up,     *
 * nonexclusive, irrevocable worldwide license to reproduce, prepare derivative*
 * works, perform publicly and display publicly by or for the Government,      *
 * including the right to distribute to other Government contractors.  Neither *
 * the United States nor the United States Department of Energy, nor any of    *
 * their employees, makes any warrenty, express or implied, or assumes any     *
 * legal liability or responsibility for the accuracy, completeness, or        *
 * usefulness of any information, apparatus, product, or process disclosed, or *
 * represents that its use would not infringe privately owned rights.          *
 *******************************************************************************
 */
#include "ftt_private.h"


/*+
 * ftt_scsi_open
 *\subnam
 *      ftt_scsi_open
 *\subcall
 *      ftt_scsi_open(pcFile, pn)
 *\subcalend
 *\subtxt
 *
 *      Open a channel to the scsi device, return
 *      the channel number from the $ASSIGN system service.
 *
 *\arglist
 *\argn pcFile Pointer to the name of the device.
 *\argn pn     Pointer to the filehandle (channel number) we will return.
 *\arglend
-*/
scsi_handle
ftt_scsi_open(char *pcDevice)
{
	scsi_handle n;

        $DESCRIPTOR(gk_device_desc, pcDevice);              /* declare a string descriptor for passed device name */
        long status;                                            /* status return */

        gk_device_desc.dsc$w_length = strlen(pcDevice);     /* byte-count */
        gk_device_desc.dsc$a_pointer = pcDevice;            /* address of the device name */

/*
 *      Allocate the jukebox device for our exclusive use.
 *      Check the status returns to see if we can continue.
 */
        DEBUG2(stderr,"now trying to ALLOCATE device %s\n",pcDevice);
        status = SYS$ALLOC( &gk_device_desc, 0, 0, 0, 0 );      /* allocate this device. */
        switch( status ) {
          case SS$_DEVALLOC:                                    /* device already allocated to another process */
          case SS$_DEVMOUNT:                                    /* device is mounted and cannot be allocated */
          case SS$_NODEVAVL:                                    /* no such generic device exists to be allocated */
          case SS$_DEVOFFLINE:                                  /* device is marked off-line */
                return 0;
                break;

          case SS$_IVDEVNAM:                                    /* invalid device name */
          case SS$_IVLOGNAM:                                    /* invalid logical name */
          case SS$_NOSUCHDEV:                                   /* no such device */
                return 0;
                break;

          default:
                CHECK(status);                                  /* check all other status returns for successful completion */
                if ( !(status&1) ) 
                  return 0;
                break;
        }


/*
 *      Assign an i/o channel (filehandle) to the jukebox.
 *      Check the status returns to see if we can continue.
 */
        DEBUG2(stderr,"now trying to ASSIGN a channel to %s\n",pcDevice);
        status = SYS$ASSIGN( &gk_device_desc,           /* assign a channel to the jukebox device */
                              &n,  		        /* put this channel into the filehandle field */
                                0, 0, 0 );
        switch(status) {
          case SS$_IVDEVNAM:                            /* invalid device name */
          case SS$_IVLOGNAM:                            /* invalid logical name */
          case SS$_NOIOCHAN:
          case SS$_NOSUCHDEV:
                return 0;
                break;

          default:
                CHECK(status);                          /* check for sucessful completion */
                if ( !(status&1) )                      /* any bad status -> fail. */
                  return 0;
                break;
        }

        return n;
}

/*+
 * ftt_scsi_close
 *\subnam
 *      ftt_scsi_close
 *\subcall
 *      ftt_scsi_close(n)
 *\subcalend
 *\subtxt
 *
 *      Close the device channel and
 *      deassign it.
 *\arglist
 *\argn n      Channel to be closed.
 *\arglend
-*/
long 
ftt_scsi_close(char *pcDevice, long n)
{
        long status;                                            /* status return */
        $DESCRIPTOR(gk_device_desc, pcDevice);              /* allocate string descriptor for device name */

/*
 *      Deassign the i/o channel to this jukebox:
 */
        DEBUG2(stderr,"now DEASSIGNING channel to %s\n",pcDevice);
        status = SYS$DASSGN(n);                    /* deassign the channel */
        CHECK(status);                                          /* check the status return */
        if ( !(status&1) ) {
          return 0;
        }

/*
 *      Deallocate the jukebox device:
 */
        gk_device_desc.dsc$w_length = strlen(pcDevice);     /* byte-count of device name */
        gk_device_desc.dsc$a_pointer = pcDevice;            /* address of device name */
        DEBUG2(stderr,"now DEALLOCATING device %s\n",pcDevice);
        status = SYS$DALLOC(&gk_device_desc, 0);                /* deallocate the device */
        CHECK(status);                                          /* check the status return */

        if ( !(status&1) ) {
          return -1;
        }
        else {
          return 0;
        }
}

/*+
 * ftt_scsi_command
 *\subnam
 *      ftt_scsi_command
 *\subcall
 *      ftt_scsi_command(n, pcOp, pcCmd, nCmd, pcRepl, nRepl, delay, writeflag)
 *\subcalend
 *\subtxt
 *      Use the SYS$QIOW system service to send a generic SCSI command
 *      to the [previously assigned] channel and receive any 
 *      replies from the jukebox device.  We will DISCONNECT from the
 *      bus during the $QIOW call; the command must complete within
 *      the delay value.
 *\arglist
 *\argn n       Channel upon which to issue the SCSI command.
 *\argn pcOp    Specific operation code for the SCSI device (text string for debugging)
 *\argn pcCmd   Address of the buffer containing the SCSI command information.
 *\argn nCmd    Number of bytes in the SCSI command.
 *\argn pcRepl  Buffer to receive any information from the SCSI device.
 *\argn nRepl   Number of bytes received.
 *\argn delay   DISCONNECT Timeout value (seconds)
 *\arglend
-*/
int 
ftt_scsi_command(scsi_handle n, char *pcOp, char *pcCmd, int nCmd, char *pcRdWr, int nRdWr, int delay, int writeflag)
{
        GK_DESC gk_desc;                                /* declare storage for the scsi frame descriptor */
        GK_IOSB gk_iosb;                                /* declare storage for the i/o status return values */
        long status;                                    /* VMS status returns */
        long scsi_status;                               /* secondary scsi status value */
        short i;                                        /* loop counter for initializations */


       /*
        * Initialize the descriptor to be passed to the SCSI
        * bus:
        */

        gk_desc[OPCODE] = 1;                            /* ALWAYS! for VMS using GKDRIVER w/generic SCSI devices */
        gk_desc[FLAGS] = FLAGS_DPR + FLAGS_DISCONNECT;  /* ALWAYS disconnect from the bus, and DON'T retry the command. */
        if ( nRdWr != 0 ) {
	  if ( writeflag ) {
            gk_desc[FLAGS] = gk_desc[FLAGS] + FLAGS_WRITE; /* If we're writing data, set the WRITE data bit. */
          } else {
            gk_desc[FLAGS] = gk_desc[FLAGS] + FLAGS_READ;  /* If we're expecting data, set the READ data bit. */
          }
        }
        gk_desc[COMMAND_ADDRESS] = pcCmd;               /* Address of the command buffer */
        gk_desc[COMMAND_LENGTH] = nCmd;                 /* Size of the command buffer */
        gk_desc[DATA_ADDRESS] = pcRdWr;                 /* Address of the receive buffer */
        gk_desc[DATA_LENGTH] = nRdWr;                   /* Size of the receive buffer */
        gk_desc[PAD_LENGTH] = 0;                        /* No padding here. */
        gk_desc[PHASE_TIMEOUT] = 0;                     /* Phase change timeout -- use the default value. */
        gk_desc[DISCONNECT_TIMEOUT] = delay;            /* Disconnect from the bus!!  Don't hog the bus!! */

/*
 *      Make sure that the reserved fields are
 *      zeroed:
 */
        for ( i = BEGIN_RESERVED; i < SCSI_DESCRIPTOR_SIZE; i++ )
           gk_desc[i] = 0;                              /* Clear reserved fields */

/*
 *      Issue the $QIO to send the SCSI command, and receive any results:
 */
        DEBUG2(stderr,"sending scsi frame:\n");
        status = SYS$QIOW(0, n, IO$_DIAGNOSE, 
                   &gk_iosb, 0, 0, &gk_desc, SCSI_DESCRIPTOR_SIZE*4, 0, 0, 0, 0);

/*
 *      Check the VMS status return to see if the $QIOW
 *      executed properly.
 */
        switch(status) {
          case SS$_DEVOFFLINE:                                  /* device is offline -- what?!?!?  */
                return JGP_DEV_FAILED;
                break;

          default:
                CHECK(status);                                  /* check VMS status returns */
                if ( !(status&1) ) {
                  return JGP_ERROR;                             /* some other error */
                }
        }

/*
 *      Check the scsi status byte to see if we were
 *      successful in completion of the command:
 */
        if ( gk_iosb.scsists != 0 ) {
          DEBUG2(stderr,"scsi status byte says NOPE, in hex = %x\n",gk_iosb.scsists);
          scsi_status = ftt_scsi_check(n, pcOp, gk_iosb.scsists);
        } else {
          DEBUG2(stderr,"scsi status byte ok.\n");
          DEBUG2(stderr,"iosts = %d\n",gk_iosb.iosts);
          DEBUG2(stderr,"iocnt = %d\n",gk_iosb.iocnt);
          scsi_status = JGP_SUCCESS;
        }
        
        return scsi_status;                                     /* return the translated scsi status byte!! */
}
