"""
Created on Oct. 12 2011

@author: Jason MacWilliams
"""

import subprocess

def sendEmail(addrs, subject, message):
    print("Sending email (%s) to addresses: %s" % (subject, addrs))
    subprocess.Popen('echo "%s" | mailx -s "%s" %s' % (message, subject, addrs), shell=True, executable="/bin/bash")

    # cmd = echo "message" | mailx -s "subject" addresses

    # XXX: we might want to attach the logfile or something else here.  In that case the order of
    # the print statement and the sendmail should be reversed so the print statement doesn't appear
    # in the log

class EmailMessage:
    def __init__(self, subject="", addrs=[]):
        if type(addrs) != list:
            return
        self.subject = subject
        self.addrs = addrs
        self.message = ""

    def addAddress(self, addr):
        if type(addr) == str and not addr in self.addrs:
            self.addrs.append(addr)

    def removeAddress(self, addr):
        if type(addr) == str and addr in self.addrs:
            self.addrs.remove(addr)

    def setSubject(self, subject):
        self.subject = subject

    def clearMessage(self):
        self.message = ""

    def addLine(self, line):
        self.message = self.message + "\n" + line

    def addString(self, string):
        self.message = self.message + string

    def send(self):
        if self.subject and self.addrs:
            sendEmail(" ".join(self.addrs), self.subject, self.message)
        print("Email report sent")
