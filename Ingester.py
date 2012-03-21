#!/usr/bin/python
"""
Created on Oct. 12 2011

@author: Jason MacWilliams
"""

import sys, os, time, signal, atexit
from optparse import OptionParser
#import pdb

from utils.commonFedora import connectToFedora
from utils.ConfigData import ConfigData
import Navigator

config = ConfigData()

# try to handle an abrupt shutdown more cleanly
# we also hit the shutdown handler after this, so don't bother sending it now
def shutdown_handler(signum, frame):
    # is there enough time to save the script state, do we even have to?
    print("Script terminating with signal %d" % signum)
    config.message.addLine("Script was terminated with signal %d" % signum)
    # we might also have to remove the last object as it may be corrupt
    # need to look into how an interrupt can interfere with shutil.copy, os.chown, and ffmpeg
    sys.exit(1)

def sendReport():
    config.message.send()

""" ====== M A I N ====== """
def main(argv):

    # register handlers so we properly disconnect and reconnect
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # parse the passed in command line options
    optionp = OptionParser()
    optionp.add_option("-c", "--config-file", type="string", dest="configfile", default=config.cfgFile, help="Path of the configuration file.")
    optionp.add_option("-d", "--dry-run", action="store_true", dest="dryrun", default=False, help="Perform a dry run of the script: make folders, but don't move/convert files, and don't create any fedora objects.")
    (options, args) = optionp.parse_args()
    # args is everything left over after parsing

    config.dryrun = options.dryrun

    if config.dryrun:
        print("Launch Ingester in SKELETON mode...")
    else:
        print("Launch Ingester...")
    print("argv=%s" % str(argv))
    if len(args) > 0:
        print("Unused arguments given: %s" % str(args))

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
        config.message.addAddress(addr)
    config.message.setSubject("%s report" % argv[0])
    atexit.register(sendReport)
    if config.dryrun:
        config.message.addLine("Running in SKELETON mode")

    """ ====== ENVIRONMENT VARIABLES ====== """
    convertPath = "/usr/local/bin"
    if not os.environ["PATH"].startswith(convertPath):
        os.environ["PATH"] = convertPath + ":" + os.environ["PATH"]#need to prepend this one for precedence over pre-existing convert command

    print("Searching for xslt processors for MIX data")
    r = subprocess.call(["which", "xalan"])
    if r == 0:
        config.jhoveCmd = ["xalan", "-xsl", "data/jhove2mix.xslt"]
    else:
        if subprocess.call(["which", "xsltproc"]) == 0:
            config.jhoveCmd = ["xsltproc", "data/jhove2mix.xslt", "-"]
        else:
            config.jhoveCmd = None

    if config.jhoveCmd != None:
        print("Jhove extraction enabled via %s" % config.jhoveCmd[0])
        config.message.addLine("Jhove extraction enabled via %s" % config.jhoveCmd[0])
    else:
        print("Unable to find either xalan or xsltproc - jhove extraction disabled")
        config.message.addLine("Unable to find either xalan or xsltproc - jhove extraction disabled")

# display to the user what settings are being used for this run
    config.printSettings()

    """ ====== FEDORA SETUP ====== """
    # connect to fedora
    fedora = connectToFedora(config.fedoraUrl, config.fedoraUser, config.fedoraPW)
    if not fedora:
        print("Error connecting to fedora instance at %s" % config.fedoraUrl)
        config.message.addLine("Error connecting to fedora instance at %s" % config.fedoraUrl)
        return 5

    print("Begin timer")
    start = time.time()

    # we want the header information to display now instead of mixed up later with the curl status updates, so flush here
    sys.stdout.flush()
    sys.stderr.flush()

    numBooks = Navigator.processFolder(fedora, config)

    config.message.addLine("Script run complete: %d books ingested" % numBooks)
    end = time.time()
    print("Total execution time: %f sec" % (end - start))
    config.message.addLine("Total execution time: %f sec" % (end - start))
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
