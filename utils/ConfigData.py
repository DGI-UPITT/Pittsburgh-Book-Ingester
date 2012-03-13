"""
Created on Oct. 12 2011

@author: Jason MacWilliams
"""

import os, sys, pwd
import ConfigParser
from Mailer import *

class ConfigData:
    def __init__(self, saveFile="IngesterState.save"):
        self.cfgFile = "controller.cfg"
        self.dryrun = False
        self.message = EmailMessage()

        self.fedoraUrl = None
        self.fedoraNS = None
        self.fedoraUser = None
        self.fedoraPW = None
        self.inDir = None
        self.hostCollectionName = None
        self.hostCollectionPid = None
        self.hostCollectionIcon = None
        self.myCollectionName = None
        self.myCollectionPid = None
        self.myCollectionIcon = None
        self.tempDir = os.getcwd() # temporary files will go through here

    def parse(self, configFile):
        self.cfgFile = configFile
        # prep the config file for input
        cfgp = ConfigParser.SafeConfigParser()
        cfgp.read(configFile)

        try:
            self.fedoraUrl = cfgp.get("Fedora", "url")
            self.fedoraNS = unicode(cfgp.get("Fedora", "namespace"))
            self.fedoraUser = cfgp.get("Fedora", "username")
            self.fedoraPW = cfgp.get("Fedora", "password")
            self.hostCollectionName = unicode(cfgp.get("Fedora", "host_collection_name"))
            self.hostCollectionPid = unicode(cfgp.get("Fedora", "host_collection_pid"))
            self.myCollectionName = unicode(cfgp.get("Fedora", "my_collection_name"))
            self.myCollectionPid = unicode(cfgp.get("Fedora", "my_collection_pid"))
            self.inDir = os.path.expanduser(cfgp.get("Controller", "input_dir"))
            self.mailTo = cfgp.get("Controller", "mail_to").replace(",", " ")
        except ConfigParser.NoSectionError, nsx:
            print("Error while parsing config file: %s" % nsx)
            return False
        except ConfigParser.NoOptionError, nox:
            print("Error while parsing config file: %s" % nox)
            return False

        try:
            self.hostCollectionIcon = os.path.expanduser(cfgp.get("Controller", "host_collection_thumbnail"))
        except:
            self.hostCollectionIcon = None

        try :
            self.myCollectionIcon = os.path.expanduser(cfgp.get("Controller", "my_collection_thumbnail"))
        except:
            self.myCollectionIcon = None

        return True

    def printSettings(self):
        print("======================================================")
        print("=== Configuration data ===")
        print("\n[Fedora]")
        print("url = %s" % self.fedoraUrl)
        print("namespace = %s" % self.fedoraNS)
        print("username = %s" % self.fedoraUser)
        print("password = %s" % self.fedoraPW)
        print("host_collection_name = %s" % self.hostCollectionName)
        print("host_collection_pid = %s" % self.hostCollectionPid)
        print("my_collection_name = %s" % self.myCollectionName)
        print("my_collection_pid = %s" % self.myCollectionPid)
        print("\n[Controller]")
        print("input_dir = %s" % self.inDir)
        print("host_collection_thumbnail = %s" % self.hostCollectionIcon)
        print("my_collection_thumbnail = %s" % self.myCollectionIcon)
        print("mail_to = %s" % self.mailTo)
        print("======================================================")

    def getConverterCommand(self, fr, to):
        key = "%s2%s" % (fr, to)
        if self.converters.has_key(key):
            return self.converters[key]
        return None

    def fileIsComplete(self, file):
        return os.path.isfile(file.replace(self.inDir, self.outDir))
