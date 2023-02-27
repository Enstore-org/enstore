###############################################################################
#

import unittest
import types
import errno
import os
import sys
from e_errors import *


class TestEnstoreError(unittest.TestCase):

    def setUp(self):
        self.e_errors_list = [
            ABOVE_THRESHOLD,
            ALARM,
            BAD_FILE_SIZE,
            BELOW_THRESHOLD,
            BFID_EXISTS,
            BROKEN,
            CANTRESTART,
            CLEANUDP_EXCEPTION,
            CONFIGDEAD,
            CONFLICT,
            CRC_DCACHE_ERROR,
            CRC_ECRC_ERROR,
            CRC_ENCP_ERROR,
            CRC_ERROR,
            CRC_ERROR_IN_WRITE_CLIENT,
            DATABASE_ERROR,
            DEFAULT_ROOT_ERROR,
            DEFAULT_SEVERITY,
            DELETED,
            DEVICE_ERROR,
            DISMOUNTFAILED,
            DOESNOTEXIST,
            DOESNOTEXISTSTILLDONE,
            DRIVEERROR,
            EMAIL,
            ENCP_GONE,
            ENCP_STUCK,
            ENSTOREBALLRED,
            EOV1_ERROR,
            EPROTO,
            ERROR,
            EnstoreError,
            FILESYSTEM_CORRUPT,
            FILE_CLERK_ERROR,
            FILE_MODIFIED,
            IGNORE,
            INFO,
            INFO_SERVER_ERROR,
            INPROGRESS,
            INVALID_ACTION,
            INVALID_WRAPPER,
            IOERROR,
            KEYERROR,
            LMD_WRONG_TICKET_FORMAT,
            LOCKED,
            MALFORMED,
            MC_DRVNOTEMPTY,
            MC_DRVNOTFOUND,
            MC_FAILCHKDRV,
            MC_FAILCHKVOL,
            MC_NONE,
            MC_QUEUE_FULL,
            MC_VOLNOTFOUND,
            MC_VOLNOTHOME,
            MEDIAERROR,
            MEDIA_IN_ANOTHER_DEVICE,
            MEMORY_ERROR,
            MISC,
            MOUNTFAILED,
            MOVERLOCKED,
            MOVER_BUSY,
            MOVER_CRASH,
            MOVER_STUCK,
            NET_ERROR,
            NOACCESS,
            NOALARM,
            NOMOVERS,
            NOREAD,
            NOSPACE,
            NOTALLOWED,
            NOT_ALWD_EXCEPTION,
            NOT_SUPPORTED,
            NOVOLUME,
            NOWORK,
            NOWRITE,
            NO_CRC_RETURNED,
            NO_FC_EXCEPTION,
            NO_FILE,
            NO_FILES,
            NO_PNFS_EXCEPTION,
            NO_SG,
            NO_VOLUME,
            OK,
            OSERROR,
            PAUSE,
            PNFS_ERROR,
            POSITIONING_ERROR,
            POSIT_EXCEPTION,
            QUOTAEXCEEDED,
            READ_BADLOCATE,
            READ_BADMOUNT,
            READ_BADSWMOUNT,
            READ_EOD,
            READ_EOT,
            READ_ERROR,
            READ_NODATA,
            READ_NOTAPE,
            READ_TAPEBUSY,
            READ_VOL1_MISSING,
            READ_VOL1_READ_ERR,
            READ_VOL1_WRONG,
            RECYCLE,
            REJECT,
            RESTRICTED_SERVICE,
            RESUBMITTING,
            RETRY,
            SERVERDIED,
            TCP_EXCEPTION,
            TCP_HUNG,
            TIMEDOUT,
            TOOMANYSUSPVOLS,
            TOO_MANY_FILES,
            TOO_MANY_RESUBMITS,
            TOO_MANY_RETRIES,
            TOO_MANY_VOLUMES,
            UNCAUGHT_EXCEPTION,
            UNKNOWN,
            UNKNOWNMEDIA,
            UNLOCKED,
            USERERROR,
            USER_ERROR,
            VERSION_MISMATCH,
            VM_CONF_EXCEPTION,
            VM_ENSTORE_EXCEPTION,
            VM_PNFS_EXCEPTION,
            VOLUME_CLERK_ERROR,
            VOLUME_EXISTS,
            VOL_SET_TO_FULL,
            WARNING,
            WRITE_BADMOUNT,
            WRITE_BADSPACE,
            WRITE_BADSWMOUNT,
            WRITE_EOT,
            WRITE_ERROR,
            WRITE_NOTAPE,
            WRITE_TAPEBUSY,
            WRITE_VOL1_MISSING,
            WRITE_VOL1_READ_ERR,
            WRITE_VOL1_WRONG,
            WRONGPARAMETER,
            WRONG_FORMAT]

        self.e0 = EnstoreError(0, "test error 0", 0, {})
        self.e1 = EnstoreError(errno.EMSGSIZE, "test error 1", WRONGPARAMETER)
        self.e2 = EnstoreError(errno.ETIMEDOUT, "test error 2", TIMEDOUT)
        self.e3 = EnstoreError("test error 3", 1, 2, 3)

    def test_init(self):
        self.assertEqual(self.e0.errno, None)
        self.assertEqual(self.e0.ticket, {
                         'status': ('UNKNOWN', 'test error 0')})
        self.assertEqual(self.e0.message_attribute_name, "e_message")
        self.assertEqual(self.e0._string(), "test error 0")
        self.assertEqual(self.e0.__str__(), "test error 0")
        self.assertEqual(self.e0.type, 'UNKNOWN')
        self.assertTrue(isinstance(self.e0, EnstoreError))

    def test___repr__(self):
        self.assertEqual(self.e0.__repr__(), "EnstoreError")

    def test_is_ok(self):
        self.assertEqual(is_ok(self.e0), 0)
        self.assertEqual(is_ok(self.e1), 0)
        self.assertEqual(is_ok(self.e2), 0)
        self.assertEqual(is_ok(OK), 1)

    def test_is_timedout(self):
        self.assertEqual(is_timedout(self.e0), 0)
        self.assertEqual(is_timedout(self.e1), 0)
        self.assertEqual(is_timedout(self.e2), 0)
        self.assertEqual(is_timedout(self.e3), 0)
        self.assertEqual(is_timedout(TIMEDOUT), 1)

    def test_is_retriable(self):
        """Test some errors are they retriable?
            NOTE THAT is_retriable(OK) is FALSE
        """
        self.assertEqual(is_retriable(self.e0), 1)
        self.assertEqual(is_retriable(self.e1), 1)
        self.assertEqual(is_retriable(self.e2), 1)
        self.assertEqual(is_retriable(self.e3), 1)
        self.assertEqual(is_retriable(NOACCESS), 0)
        self.assertEqual(is_retriable(TIMEDOUT), 1)
        # bug?
        self.assertEqual(
            is_retriable(OK), 0, "expected %s got %s" %
            (0, is_retriable(OK)))
        for e in non_retriable_errors:
            self.assertFalse(is_retriable(e), e)

    def test_is_non_retriable(self):
        """Test some errors are they retriable?
            NOTE THAT is_non_retriable(OK) is FALSE
            AND is_retriable(OK) is also FALSE
        """
        self.assertEqual(is_non_retriable(self.e0), 0)
        self.assertEqual(is_non_retriable(self.e1), 0)
        self.assertEqual(is_non_retriable(self.e2), 0)
        self.assertEqual(is_non_retriable(self.e3), 0)
        self.assertEqual(is_non_retriable(NOACCESS), 1)
        self.assertEqual(is_non_retriable(TIMEDOUT), 0)
        self.assertEqual(is_non_retriable(OK), 0,
                         "expected %s got %s" % (0, is_non_retriable(OK)))
        for e in non_retriable_errors:
            self.assertTrue(is_non_retriable(e), e)

    def test_retriable_discrepancy(self):
        """Test that is_retriable and is_non_retriable are mutually exclusive.

              This test is not exhaustive.  It only tests the error codes
              HOWEVER it does document that is_retriable(OK) and is_non_retriable(OK)
              return the same value.  This is a bug.

        """
        fail_list = [OK]
        # uncomment the following line to test all error codes and show the bug
        #fail_list = []
        for e in self.e_errors_list:
            if is_retriable(e):
                self.assertFalse(is_non_retriable(e), e)
            if is_non_retriable(e):
                self.assertFalse(is_retriable(e), e)
            if e not in fail_list:
                r = is_retriable(e)
                n = is_non_retriable(e)
                #print "testing ", e, "is_retriable=", r, "is_non_retriable=", n
                self.assertEqual(
                    r + n, 1, "expected %s got %s testing %s" % (1, r + n, e))

    def test_is_alarmable(self):
        self.assertEqual(is_alarmable(self.e0), 0)
        self.assertEqual(is_alarmable(self.e1), 0)
        self.assertEqual(is_alarmable(self.e2), 0)
        self.assertEqual(is_alarmable(self.e3), 0)
        for e in self.e_errors_list:
            if e in raise_alarm_errors:
                self.assertTrue(is_alarmable(e), e)
            elif e in email_alarm_errors:
                self.assertTrue(is_alarmable(e), e)
            else:
                self.assertFalse(is_alarmable(e), e)

    def test_is_emailable(self):
        self.assertEqual(is_emailable(self.e0), 0)
        self.assertEqual(is_emailable(self.e1), 0)
        self.assertEqual(is_emailable(self.e2), 0)
        self.assertEqual(is_emailable(self.e3), 0)
        for e in self.e_errors_list:
            if e in email_alarm_errors:
                self.assertTrue(is_emailable(e), e)
            else:
                self.assertFalse(is_emailable(e), e)

    def test_is_resendable(self):
        self.assertEqual(is_resendable(self.e0), 0)
        self.assertEqual(is_resendable(self.e1), 0)
        self.assertEqual(is_resendable(self.e2), 0)
        self.assertEqual(is_resendable(self.e3), 0)
        for e in self.e_errors_list:
            if e in [RETRY, RESUBMITTING]:
                self.assertTrue(is_resendable(e), e)
            else:
                self.assertFalse(is_resendable(e), e)

    def test_is_media(self):
        self.assertEqual(is_media(self.e0), 0)
        self.assertEqual(is_media(self.e1), 0)
        self.assertEqual(is_media(self.e2), 0)
        self.assertEqual(is_media(self.e3), 0)
        media_errors = [
            WRITE_NOTAPE,
            WRITE_TAPEBUSY,
            WRITE_BADMOUNT,
            WRITE_BADSWMOUNT,
            WRITE_BADSPACE,
            WRITE_ERROR,
            WRITE_EOT]
        media_errors.extend([READ_NOTAPE,
                             READ_TAPEBUSY,
                             READ_BADMOUNT,
                             READ_BADSWMOUNT,
                             READ_BADLOCATE,
                             READ_ERROR,
                             READ_EOT,
                             READ_EOD,
                             READ_NODATA])
        media_errors.extend([READ_VOL1_READ_ERR,
                             WRITE_VOL1_READ_ERR,
                             READ_VOL1_MISSING,
                             WRITE_VOL1_MISSING,
                             READ_VOL1_WRONG,
                             WRITE_VOL1_WRONG,
                             EOV1_ERROR])
        media_errors.extend([NOACCESS, NOTALLOWED, CRC_ERROR, MEDIAERROR])
        for e in self.e_errors_list:
            if e in media_errors:
                self.assertTrue(is_media(e), e)
            else:
                self.assertFalse(is_media(e), e)


if __name__ == '__main__':
    unittest.main()
