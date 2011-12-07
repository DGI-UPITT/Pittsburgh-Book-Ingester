"""
Created on Oct. 12 2011

@author: Jason MacWilliams
"""

import os, sys, pwd
import ConfigParser

class ConfigData:
    def __init__(self, saveFile="IngesterState.save"):
        self.cfgFile = "controller.cfg"
        self.saveFile = saveFile
        self.fedoraUrl = None
        self.fedoraNS = None
        self.fedoraUser = None
        self.fedoraPW = None
        self.solrUrl = None
        self.inDir = None
        self.hostCollectionName = None
        self.hostCollectionPid = None
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
            self.bookCollectionName = unicode(cfgp.get("Fedora", "book_collection_name"))
            self.bookCollectionPid = unicode(cfgp.get("Fedora", "book_collection_pid"))
            self.solrUrl = cfgp.get("Solr", "url")
            self.inDir = os.path.expanduser(cfgp.get("Controller", "input_dir"))
            self.mailTo = cfgp.get("Controller", "mail_to").replace(",", " ")
        except ConfigParser.NoSectionError, nsx:
            print("Error while parsing config file: %s" % nsx)
            return False
        except ConfigParser.NoOptionError, nox:
            print("Error while parsing config file: %s" % nox)
            return False

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
        print("book_collection_name = %s" % self.bookCollectionName)
        print("book_collection_pid = %s" % self.bookCollectionPid)
        print("\n[Solr]")
        print("url = %s" % self.solrUrl)
        print("\n[Controller]")
        print("input_dir = %s" % self.inDir)
        print("mail_to = %s" % self.mailTo)
        print("======================================================")

    def getConverterCommand(self, fr, to):
        key = "%s2%s" % (fr, to)
        if self.converters.has_key(key):
            return self.converters[key]
        return None

    def fileIsComplete(self, file):
        return os.path.isfile(file.replace(self.inDir, self.outDir))

    def getTargetUid(self):
        return self.targetUser[2]

    def getTargetGid(self):
        return self.targetUser[3]
