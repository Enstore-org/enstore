
import tempfile
import os

# format the mail


def format_mail(goal, question, metric):
    return "\n\n  GOAL: %s\n\n  QUESTION: %s\n\n  METRIC: %s" % (goal, question,
                                                                 metric)

# send mail


def send_mail(server, message, subject, destination="$ENSTORE_MAIL"):
    """
    #This is the secure version.  However, it requires using python 2.3 or
    # higher.
    (mail_fd, mail_file) = tempfile.mkstemp()

    os.system("date >> %s"%(mail_file,))
    os.system('echo "\n\tFrom: %s\n" >> %s' % (server, mail_file))
    os.system('echo "\t%s" >> %s' % (message, mail_file))
    os.system("/usr/bin/Mail -s \"%s\" %s < %s"%(subject, destination, mail_file,))
    os.system("rm %s"%(mail_file,))

    os.close(mail_fd)
    """

    mail_file = tempfile.mktemp()
    os.system("date >> %s" % (mail_file,))
    os.system('echo "\n\tFrom: %s\n" >> %s' % (server, mail_file))
    os.system('echo "\t%s" >> %s' % (message, mail_file))
    os.system("/usr/bin/Mail -s \"%s\" %s < %s" %
              (subject, destination, mail_file,))
    os.system("rm %s" % (mail_file,))
