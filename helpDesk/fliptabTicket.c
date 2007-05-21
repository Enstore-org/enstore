/*  File:  create_entry.c  3/8/99  */
/*  Modified 7/2001 by Mike Stolz, with help from Marc Mengel, in order to
    have it create AR/Remedy tickets into the Fermilab Helpdesk database */

			/* Unix System include files */
#include    <stdio.h>
#include    <stdlib.h>
#include    <string.h>
			/* AR System include files */
#include    <ar.h>
#include    <arextern.h>
#include    <arstruct.h>
#include    <arfree.h>

#include    "fermistruct.h"

#ifdef NT
#include    <nt.h>
#include    <ntfree.h>
#include    <ntsextrn.h>
#endif

#include    <api.h>
#include    <gen_util.h>
#include    <util.h>

#include    <apiclass.h>

#include    <arerrno.h>

#define     NARGS		11	/* number of arguments */
#define     NFIELDS		18	/* number of fields in list */
#define     SHORT_MESSAGE_SIZE	100	/* recommended by Help Desk */

/**********************************************************************/
/*                                                                    */
/*                            main block                              */
/*                                                                    */
/**********************************************************************/
/*                                                                    */
/*   Description:  Program to take server and a name as input, and    */
/*      submit a new entry to the hard-coded sample schema.           */
/*                                                                    */
/*   Usage:                                                           */
/*      a.out  system-name  short-message long-message                */
/*                                                                    */
/*   Modification History:                                            */
/*      Date       Engineer     Description                           */
/*      --------   ----------   ------------                          */
/*      09/21/94   d.lancaster  program created                       */
/*      09/18/97   Remedy Ed.   updated to new class library          */
/*	04/30/99   S. Basham	rewritten for AR System 4.x	      */
/*      08/  /01   M. Stolz	altered to create Fermilab AR ticket  */
/*      03/05/02   M. Stolz     added condition field to AR ticket    */
/*                                                                    */
/**********************************************************************/

int main( int argc, char *argv[] )
{
   static ARControlStruct    control;	      /* control record for API      */
   static ARFieldValueList   fieldList;	      /* data for submitting	     */
   static AREntryIdType      entryId;	      /* OUT; holds returned entryid */
   static ARStatusList       status;	      /* status of AR System op      */
   static ARFieldValueStruct fields[NFIELDS]; /* to hold field/value info    */

   /* Positional arguments are expected as follows:			*/
   /*	server-name	name of the remedy server			*/
   /*	system-name	name of the system affected by the request	*/
   /*	short-message	short description of the request		*/
   /*	long-message	full description of the request			*/
   /*	submitter	submitter of the request			*/
   /*	user		remedy user					*/
   /*	password	password for user				*/
   /*	category	category of the request				*/
   /*	type		type within the category			*/
   /*	item		item within the type				*/

   const int		SERVERINDEX = 1;
   const int		SYSTEMINDEX = 2;
   const int		SHORTMSGINDEX = 3;
   const int		LONGMSGINDEX = 4;
   const int		SUBMITTERINDEX = 5;
   const int		USERINDEX = 6;
   const int		PASSWORDINDEX = 7;
   const int		CATEGORYINDEX = 8;
   const int		TYPEINDEX = 9;
   const int		ITEMINDEX = 10;
   char 		system_name[AR_MAX_NAME_SIZE];
   char 		short_message[SHORT_MESSAGE_SIZE];
   char 		long_message[AR_MAX_MESSAGE_SIZE];
   ARNameType		submitter;
   char 		category[AR_MAX_NAME_SIZE];
   char 		type[AR_MAX_NAME_SIZE];
   char 		item[AR_MAX_NAME_SIZE];
   int			rtn;		/* return value from AR routine */

   hack_init();

   if (argc != NARGS) {
      (void) printf("Usage: %s  server-name  system-name  short-message  long-message  submitter  user  password  category  type  item\n",
		    argv[0]);
      exit(1);
      }

   (void) strncpy(control.server, argv[SERVERINDEX], AR_MAX_SERVER_SIZE);
   control.server[AR_MAX_NAME_SIZE] = '\0';

   (void) strncpy(system_name,	    argv[SYSTEMINDEX],	  AR_MAX_NAME_SIZE);
   (void) strncpy(short_message,    argv[SHORTMSGINDEX],  SHORT_MESSAGE_SIZE);
   (void) strncpy(long_message,     argv[LONGMSGINDEX],   AR_MAX_MESSAGE_SIZE);
   (void) strncpy(submitter,	    argv[SUBMITTERINDEX], AR_MAX_NAME_SIZE);
   (void) strncpy(category,	    argv[CATEGORYINDEX],  AR_MAX_NAME_SIZE);
   (void) strncpy(type, 	    argv[TYPEINDEX],	  AR_MAX_NAME_SIZE);
   (void) strncpy(item, 	    argv[ITEMINDEX],	  AR_MAX_NAME_SIZE);

   system_name[AR_MAX_NAME_SIZE] = '\0';
   short_message[SHORT_MESSAGE_SIZE] = '\0';
   long_message[AR_MAX_MESSAGE_SIZE] = '\0';
   submitter[AR_MAX_NAME_SIZE] = '\0';
   category[AR_MAX_NAME_SIZE] = '\0';
   type[AR_MAX_NAME_SIZE] = '\0';
   item[AR_MAX_NAME_SIZE] = '\0';

   (void) printf("new entry created by %s\n", submitter);
   (void) printf("new entry created on %s\n", control.server);
   (void) printf("new message is %s\n", short_message);

   /*******************************************************************/
   /* Establish the control structure.                                */
   /*******************************************************************/

   control.cacheId = 0;			/* initialize cache id to 0	*/

   (void) strncpy(control.user, argv[USERINDEX], AR_MAX_NAME_SIZE);
   control.user[AR_MAX_NAME_SIZE] = '\0';

   (void) strncpy(control.password, argv[PASSWORDINDEX], AR_MAX_NAME_SIZE);
   control.password[AR_MAX_NAME_SIZE] = '\0';

   (void) printf("Remedy user is %s\n", control.user);
   (void) printf("Remedy description is %s\n", long_message);

   /*******************************************************************/
   /* MUST initialize Remedy session.                                 */
   /*******************************************************************/

   rtn = ARInitialization(&control, &status);

   if_arerror_exit(&control,"**** ARInitialization ****", rtn, &status);

   /*******************************************************************/
   /* Fill in the data structures for the required fields.            */
   /* The field-ids of the core fields are defined in arstruct.h      */
   /*******************************************************************/

/* these fields are directly associated with the Help Desk's customized Remedy screen */

   fieldList.numItems = NFIELDS;	/* count of 'active' fields written */
   fieldList.fieldValueList = fields;

   fields[0].fieldId = AR_CORE_SUBMITTER;	/* submitter field */
   fields[0].value.u.charVal = (char *)strdup((const char *)submitter);
   fields[0].value.dataType = AR_DATA_TYPE_CHAR;

   fields[1].fieldId = AR_FERMI_INCIDENT_TIME;
   fields[1].value.u.timeVal = time(0);
   fields[1].value.dataType = AR_DATA_TYPE_TIME;

   fields[2].fieldId = AR_CORE_STATUS;		/* status field */
   fields[2].value.u.enumVal = 0;              /* 0 = NEW, 1 = ASSIGNED */
   fields[2].value.dataType = AR_DATA_TYPE_ENUM;

   fields[3].fieldId = AR_CORE_SHORT_DESCRIPTION;
   fields[3].value.u.charVal = (char *)strdup(short_message);
   fields[3].value.dataType = AR_DATA_TYPE_CHAR;

   fields[4].fieldId = AR_FERMI_FERMI_ID;
   fields[4].value.u.charVal = "06102N";
   fields[4].value.dataType = AR_DATA_TYPE_CHAR;

   fields[5].fieldId = AR_FERMI_FIRST_NAME;
   fields[5].value.u.charVal = "David";
   fields[5].value.dataType = AR_DATA_TYPE_CHAR;

   fields[6].fieldId = AR_FERMI_LAST_NAME;
   fields[6].value.u.charVal = "Berg";
   fields[6].value.dataType = AR_DATA_TYPE_CHAR;

   fields[7].fieldId = AR_FERMI_PHONE;
   fields[7].value.u.charVal = "3021";
   fields[7].value.dataType = AR_DATA_TYPE_CHAR;

   fields[8].fieldId = AR_FERMI_EMAIL;
   fields[8].value.u.charVal = "ssa-group@fnal.gov";
   fields[8].value.dataType = AR_DATA_TYPE_CHAR;

   fields[9].fieldId = AR_FERMI_CATEGORY;
   fields[9].value.u.charVal = (char *)strdup((const char *)category);
   fields[9].value.dataType = AR_DATA_TYPE_CHAR;

   fields[10].fieldId = AR_FERMI_TYPE;
   fields[10].value.u.charVal = (char *)strdup((const char *)type);
   fields[10].value.dataType = AR_DATA_TYPE_CHAR;

   fields[11].fieldId = AR_FERMI_ITEM;
   fields[11].value.u.charVal = (char *)strdup((const char *)item);
   fields[11].value.dataType = AR_DATA_TYPE_CHAR;

   fields[12].fieldId = AR_FERMI_PROBLEM_DESCRIPTION;
   fields[12].value.u.charVal = (char *)strdup(long_message);
   fields[12].value.dataType = AR_DATA_TYPE_CHAR;

   fields[15].fieldId = AR_FERMI_ASSIGNED_TO_GROUP;
   fields[15].value.u.charVal = "HelpDesk";
   fields[15].value.dataType = AR_DATA_TYPE_CHAR;

   fields[13].fieldId = AR_CORE_ASSIGNED_TO; /* who is assigned to the problem */
   fields[13].value.u.charVal = "HelpDesk";
   fields[13].value.dataType = AR_DATA_TYPE_CHAR;

   fields[14].fieldId = AR_FERMI_PRIORITY;
   fields[14].value.u.enumVal = 2;           /* 0 = URGENT; 1 = HIGH */
   fields[14].value.dataType = AR_DATA_TYPE_ENUM;

   fields[16].fieldId = AR_FERMI_SOURCE;
   fields[16].value.u.enumVal = 5;           /* 0 = E-Mail, 5 = API */
   fields[16].value.dataType = AR_DATA_TYPE_ENUM;

   fields[17].fieldId = AR_FERMI_SYSTEM_NAME;
   fields[17].value.u.charVal = (char *)strdup(system_name);
   fields[17].value.dataType = AR_DATA_TYPE_CHAR;

   /*******************************************************************/
   /* "Heart" of this routine - creating an entry on the schema.      */
   /*******************************************************************/
   /* PrintARFieldValueList(&fieldList);  */

   rtn=ARCreateEntry(&control, "CD:HelpDesk", &fieldList, entryId, &status);

   printf("Incident time is %i\n", fields[1].value.u.timeVal);

   if_arerror_exit(&control,"**** ARCreateEntry ****", rtn, &status);

   printf("\nEntry created with id = %s\n\n", entryId);

   /*******************************************************************/
   /* Remedy cleanup.                                                 */
   /*******************************************************************/

   rtn = ARTermination(&control,&status);
   if_arerror_exit(&control,"**** ARTermination ****", rtn, &status);
   return 0;
}
