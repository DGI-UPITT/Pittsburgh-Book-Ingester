# -*- coding: utf8 -*-
from utils.commonFedora import *
import glob, zipfile, sys
import FileIngester

""" ====== SCAN FOR OBJECTS IN A FOLDER ====== """
def processFolder(fedora, config):
    """
    Create a bunch of fedora objects (1 for each folder in @config.inDir)
    """

    folder = config.inDir

    # first make sure @folder is a valid folder
    if not os.path.isdir(folder):
        return False

    # the collection overhead
    # the host collection (topmost root)
    hostCollection = addCollectionToFedora(fedora, config.hostCollectionName, myPid=config.hostCollectionPid, tnUrl=config.hostCollectionIcon)
    # the aggregate (contains the books)
    myCollection = addCollectionToFedora(fedora, config.myCollectionName, myPid=config.myCollectionPid, parentPid=config.hostCollectionPid, tnUrl=config.myCollectionIcon)

    # this is the list of all folders to search in for books
    baseFileDict = { 'parentPid' : config.myCollectionPid, 'contentModel' : 'islandora:bookCModel' }
    totalFiles = 0
    completeFiles = 0
    for subFolder in os.listdir(folder):
        if os.path.isdir(os.path.join(folder, subFolder)):

            print("Scan Folder %s" % subFolder)
            # the single item trap - very helpful for testing
            #if subFolder != "00awa1617m":
            #    continue

            fileDict = { 'label': subFolder, 'datastreams' : { } }

            def addFileByPattern(label, pattern):
                file = glob.glob("%s" % os.path.join(folder, subFolder, pattern))
                if len(file) > 0:
                    fileDict['datastreams'][label] = file[0]
                    return True
                return False

            addFileByPattern("MODS", "*.mods.xml")
            addFileByPattern("DC", "*.dc.xml")
            addFileByPattern("MARC", "*.marcxml.xml")
            addFileByPattern("METS", "*.mets.xml")

            ocr = glob.glob("%s" % os.path.join(folder, subFolder, "*.ocr.zip"))
            if ocr:
                fileDict['ocr'] = ocr[0]

            chapters = glob.glob("%s" % os.path.join(folder, subFolder, "*.body.xml"))
            if chapters:
                fileDict['chapters'] = chapters[0]
            # creation of the dictionary here might be bad
            fileDict.update(baseFileDict)
            totalFiles = totalFiles + 1
            if FileIngester.createObjectFromFiles(fedora, config, fileDict):
                print("Object (%s) ingested successfully" % subFolder)
                completeFiles = completeFiles + 1

    return completeFiles
