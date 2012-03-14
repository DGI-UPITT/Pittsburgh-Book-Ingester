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

""" ====== INGEST FILES IN A FOLDER ======
def ingestPagesFromFolder(fedora, config, bookFolder):
    # first make sure bookFolder is a valid bookFolder
    if not os.path.isdir(bookFolder):
        return False

    baseFileDict = { 'parentPid' : config.myCollectionPid, 'contentModel' : 'islandora:archiveorg:bookCModel' }
    fileDict = { 'label': bookFolder, 'datastreams' : { } }

    pages = glob.glob("%s/*.tif" % bookFolder)
    pages.sort()
    count = len(pages)
    print("Build book object with %d pages" % count)

    bookName = os.path.basename(bookFolder)
    bookPid = "%s:%s" % (config.fedoraNS, bookName)
    # the actual book is a collection
    book = addCollectionToFedora(fedora, unicode(bookName), bookPid, parentPid=config.myCollectionPid, contentModel="archiveorg:bookCModel")

    # load the custom datastreams
    streams = [ 'DC', 'MARCXML', 'METS', 'MODS' ]
    for s in streams:
        streamFile = "%s.%s.xml" % (bookName, s.lower())
        streamSource = os.path.join(bookFolder, streamFile)
        if os.path.isfile(streamSource):
            print("Add custom %s datastream to book (%s)" % (s, streamSource))
            if not config.dryrun:
                fedoraLib.update_datastream(book, s, streamSource, label='%s Data' % s, mimeType=misc.getMimeType("xml"))

    def addFileByPattern(label, pattern):
        file = glob.glob("%s" % os.path.join(folder, subFolder, pattern))
        if len(file) > 0:
            fileDict['datastreams'][label] = file[0]
            return True
        return False

    if not addFileByPattern("TIFF", "*.tif"):
        if not addFileByPattern("TIFF", "*.tiff"):
            # failed
            print("Could not find base tif file - skipping directory")
            continue # next subFolder
    addFileByPattern("MODS", "*.mods.xml")
    addFileByPattern("TN", "*.thumb.jpg")
    addFileByPattern("DC", "*.dc.xml")


    print("Create thumbnail for book using %s" % pages[0])
    # create a TN image using page 1
    if not config.dryrun:
        tnFile = os.path.join(config.tempDir, "tmp.jpg")
        converter.tif_to_jpg(os.path.join(bookFolder, pages[0]), tnFile, imageMagicOpts='TN')
        #add a TN datastream to the book
        fedoraLib.update_datastream(book, "TN", tnFile, label=unicode(config.myCollectionName+"_TN.jpg"), mimeType=misc.getMimeType("jpg"))
        os.remove(tnFile) # delete it so we can recreate it again for the next thumbnail
        # now tnFile is closed and deleted

    print("Process %d pages for ingest" % count)

    zipName = os.path.join(bookFolder, "%s.ocr.zip" % bookName)
    if zipfile.is_zipfile(zipName):
        ocrzip = zipfile.ZipFile(zipName, "r")
    else:
        ocrzip = None

    for idx, page in enumerate(pages):
        print("\n==========\nIngesting object %d of %d: %s" % (idx+1, count, page))

        if not config.dryrun:
            basePage = os.path.splitext(os.path.basename(page))[0]

            #pagePid = fedora.getNextPID(config.fedoraNS)
            pagePid = "%s-%d" % (bookPid, idx+1)
            # pageCModel doesn't exist - its just here as a placeholder

            extraNamespaces = { 'pageNS' : 'info:islandora/islandora-system:def/pageinfo#' }
            extraRelationships = { fedora_relationships.rels_predicate('pageNS', 'isPageNumber') : str(idx+1) }

            # create the object (page)
            try:
                obj = addObjectToFedora(fedora, unicode("%s-%s" % (bookName, basePage)), pagePid, bookPid, "archiveorg:pageCModel",
                        extraNamespaces=extraNamespaces, extraRelationships=extraRelationships)
            except FedoraConnectionException, fcx:
                print("Connection error while trying to add fedora object (%s) - the connection to fedora may be broken", page)
                continue

            # ingest the tif
            tifFile = os.path.join(bookFolder, page)
            fedoraLib.update_datastream(obj, u"TIFF", tifFile, label=unicode("%s.tif" % basePage), mimeType=misc.getMimeType("tiff"))

            # create a JP2 datastream
            jp2File = os.path.join(config.tempDir, "%s.jp2" % basePage)
            converter.tif_to_jp2(tifFile, jp2File, 'default', 'default')
            fedoraLib.update_datastream(obj, u"JP2", jp2File, label=unicode("%s.jp2" % basePage), mimeType=misc.getMimeType("jp2"))
            os.remove(jp2File) # finished with that

            pdfFile = os.path.join(config.tempDir, "%s.pdf" % basePage)
            converter.tif_to_pdf(tifFile, pdfFile, 'default', 'default')
            fedoraLib.update_datastream(obj, u'PDF', pdfFile, label=unicode("%s_pdf" % basePage), mimeType=misc.getMimeType("pdf"))
            os.remove(pdfFile)

            # ingest the ocr if it exists
            if ocrzip:
                # try to find the files' ocr data
                ocrFileName = "%s.txt" % basePage
                if ocrFileName in ocrzip.namelist():
                    ocrFile = ocrzip.extract(ocrFileName, config.tempDir)
                    fedoraLib.update_datastream(obj, u"OCR", os.path.join(config.tempDir, ocrFile), label=unicode(ocrFileName), mimeType=misc.getMimeType("txt"))
                    os.remove(os.path.join(config.tempDir, ocrFileName)) # get rid of that temp file
        sys.stdout.flush()
        sys.stderr.flush()

    ocrzip.close()

    # now lets check for chapter information

    return True
"""
