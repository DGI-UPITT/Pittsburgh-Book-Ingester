#!/usr/bin/python
"""
Created on Oct. 12 2011

@author: Jason MacWilliams
"""

import sys, os, time, glob, signal, subprocess, atexit
import zipfile
from islandoraUtils import fileConverter as converter
from optparse import OptionParser
import pdb

from commonFedora import *
from ConfigData import *
import Mailer

DRYRUN = False

GENERATE_ME = "*"
config = ConfigData()
message = Mailer.EmailMessage()

""" ====== INGEST FILES IN A FOLDER ====== """
def processBookFolder(fedora, folder):
    """
    Create a bunch of fedora objects (1 for each tif in @folder)
    """
    # first make sure folder is a valid folder
    if not os.path.isdir(folder):
        return False

    pages = glob.glob("%s/*.tif" % folder)
    pages.sort()
    count = len(pages)
    print("Build book object with %d pages" % count)

    bookName = os.path.basename(folder)
    bookPid = "%s:%s" % (config.fedoraNS, bookName)
    book = addCollectionToFedora(fedora, unicode(bookName), bookPid, parentPid=config.bookCollectionPid, contentModel="islandora:bookCModel")

    # load the custom datastreams
    streams = [ 'dc', 'marcxml', 'mets', 'mods' ]
    for s in streams:
        streamFile = "%s.%s.xml" % (bookName, s)
        streamSource = os.path.join(folder, streamFile)
        if os.path.isfile(streamSource):
            print("Add custom %s datastream to book (%s)" % (s.upper(), streamSource))
            if not DRYRUN:
                fedoraLib.update_datastream(book, s.upper(), streamSource, label='%s Data' % s.upper(), mimeType=misc.getMimeType("xml"))

    print("Create thumbnail for book using %s" % pages[0])
    # create a TN image using page 1
    if not DRYRUN:
        tnFile = os.path.join(config.tempDir, "tmp.jpg")
        converter.tif_to_jpg(os.path.join(folder, pages[0]), tnFile, imageMagicOpts='TN')
        #add a TN datastream to the book
        fedoraLib.update_datastream(book, "TN", tnFile, label=unicode(config.bookCollectionName+"_TN.jpg"), mimeType=misc.getMimeType("jpg"))
        os.remove(tnFile) # delete it so we can recreate it again for the next thumbnail
        # now tnFile is closed and deleted

    print("Process %d pages for ingest" % count)

    zipName = os.path.join(folder, "%s.ocr.zip" % bookName)
    if zipfile.is_zipfile(zipName):
        ocrzip = zipfile.ZipFile(zipName, "r")
    else:
        ocrzip = None

    for idx, page in enumerate(pages):
        print("Ingesting object %d of %d: %s" % (idx+1, count, page))

        if not DRYRUN:
            # create the object (page)
            try:
                #pagePid = gedora.getNextPID(config.fedoraNS)
                pagePid = "%s-%03d" % (bookPid, idx+1)
                obj = addObjectToFedora(fedora, unicode("%s-%s" % (bookName, os.path.basename(page))), pagePid, bookPid, "islandora:pageCModel")
            except FedoraConnectionException, fcx:
                print("Connection error while trying to add fedora object (%s) - the connection to fedora may be broken", page)
                continue

            # load the tiff
            fedoraLib.update_datastream(obj, "TIFF", os.path.join(folder, page), label=unicode(os.path.basename(page)), mimeType=misc.getMimeType("tiff"))

            # load the ocr if it exists
            if ocrzip:
                # try to find the files' ocr data
                ocrFileName = os.path.splitext(os.path.basename(page))[0] + ".txt"
                if ocrFileName in ocrzip.namelist():
                    ocrFile = ocrzip.extract(ocrFileName, config.tempDir)
                    fedoraLib.update_datastream(obj, "OCR", os.path.join(config.tempDir, ocrFile), label=unicode(ocrFileName), mimeType=misc.getMimeType("txt"))
                    os.remove(os.path.join(config.tempDir, ocrFileName)) # get rid of that temp file

    ocrzip.close()

    return True

# try to handle an abrupt shutdown more cleanly
# we also hit the shutdown handler after this, so don't bother sending it now
def shutdown_handler(signum, frame):
    # is there enough time to save the script state, do we even have to?
    print("Script terminating with signal %d" % signum)
    message.addLine("Script was terminated with signal %d" % signum)
    # we might also have to remove the last object as it may be corrupt
    # need to look into how an interrupt can interfere with shutil.copy, os.chown, and ffmpeg
    sys.exit(1)

def sendReport():
    #message.send()
    print("Email report sent")

""" ====== M A I N ====== """
def main(argv):
    global DRYRUN

    # register handlers so we properly disconnect and reconnect
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # parse the passed in command line options
    optionp = OptionParser()
    optionp.add_option("-c", "--config-file", type="string", dest="configfile", default=config.cfgFile, help="Path of the configuration file.")
    optionp.add_option("-i", "--ignore-save", action="store_true", dest="ignore", default=False, help="Ignore saved script state files when launching.")
    optionp.add_option("-d", "--dry-run", action="store_true", dest="dryrun", default=False, help="Perform a dry run of the script: make folders, but don't move/convert files, and don't create any fedora objects.")
    (options, args) = optionp.parse_args()

    DRYRUN = options.dryrun

    if DRYRUN:
        print("Launch Ingester in SKELETON mode...")
    else:
        print("Launch Ingester...")
    print("argv=%s" % str(argv))

    if not os.path.exists(options.configfile):
        print("Config file %s not found!" % (options.configfile))
        optionp.print_help()
        return 1

    # put the name of the config file in the master dictionary - in case its different
    config.cfgFile = options.configfile

    """ ====== BASE CONFIGURATION FILE ====== """
    # load configuration
    print("Loading configuration file %s" % options.configfile)
    if not config.parse(options.configfile):
        print("*** Error loading configuration file ***")
        return 2

    if not os.path.isdir(config.inDir):
        print("Please verify the source directory: %s" % config.inDir)
        return 3

    for addr in config.mailTo.split(" "):
        message.addAddress(addr)
    message.setSubject("%s report" % argv[0])
    atexit.register(sendReport)

    if DRYRUN:
        message.addLine("Running in SKELETON mode")

    """ ====== ENVIRONMENT VARIABLES ====== """
    # add cli,imageMagick to the path and hope for the best [remove these on production server]
    #os.environ["PATH"] = os.environ["PATH"] + ":/usr/local/ABBYY/FREngine-Linux-i686-9.0.0.126675/Samples/Samples/CommandLineInterface"
    #os.environ["PATH"] = os.environ["PATH"] + ":/usr/local/Linux-x86-64"
    #os.environ["PATH"] = os.environ["PATH"] + ":/usr/local/Exif"
    convertPath = "/usr/local/bin"
    if not os.environ["PATH"].startswith(convertPath):
        os.environ["PATH"] = convertPath + ":" + os.environ["PATH"]#need to prepend this one for precedence over pre-existing convert command

    # display to the user what settings are being used for this run
    config.printSettings()

    """ ====== FEDORA SETUP ====== """
    # connect to fedora
    fedora = connectToFedora(config.fedoraUrl, config.fedoraUser, config.fedoraPW)
    if not fedora:
        print("Error connecting to fedora instance at %s" % config.fedoraUrl)
        message.addLine("Error connecting to fedora instance at %s" % config.fedoraUrl)
        return 5

    collection = addCollectionToFedora(fedora, config.hostCollectionName, myPid=config.hostCollectionPid)
    bookCollection = addCollectionToFedora(fedora, config.bookCollectionName, myPid=config.bookCollectionPid, parentPid=config.hostCollectionPid)

    print("+-Scanning for books to ingest")
    print(" +-Scanning folder: %s" % config.inDir)

    fileList = os.listdir(config.inDir)
    numBooks = 0
    for file in fileList:
        fullDirectory = os.path.join(config.inDir, file)
        if os.path.isdir(fullDirectory):
            # check for datastream source
            print("Found book folder %s" % file)
            if processBookFolder(fedora, fullDirectory):
                numBooks = numBooks + 1
                break

    message.addLine("Script run complete: %d books ingested" % numBooks)
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
