from islandoraUtils import fileConverter as converter
from islandoraUtils import fileManipulator as manipulator
from utils.commonFedora import *
import subprocess, glob, sys, zipfile

""" ====== INGEST A SINGLE OBJECT ====== """
def createObjectFromFiles(fedora, config, objectData):
    """
    Create a fedora object containing all the data in objectData and more
    """

    bookFolder = os.path.join(config.inDir, objectData['label'])

    for ds in [ "DC", "MODS", "MARC", "METS" ]:
        # some error checking
        if not objectData['datastreams'][ds]:
            # broken object
            print("Object data is missing required datastream: %s" % ds)
            return False

    #objPid = fedora.getNextPID(config.fedoraNS)
    objPid = "%s:%s" % (config.fedoraNS, objectData['label'])

    if not config.dryrun:
        # create the object (page)
        try:
            bookObj = addCollectionToFedora(fedora, unicode("%s" % objectData['label']), objPid, objectData['parentPid'], objectData['contentModel'])
        except FedoraConnectionException, fcx:
            print("Connection error while trying to add fedora object (%s) - the connection to fedora may be broken", objPid)
            return False

        # ingest the datastreams we were given
        for dsid, file in objectData['datastreams'].iteritems():
            # hard coded blarg:
            if dsid in ["MODS"]: # maybe make all these 'X'
                controlGroup = "X"
            else:
                controlGroup = "M"
            fedoraLib.update_datastream(bookObj, dsid, file, label=unicode(os.path.basename(file)), mimeType=misc.getMimeType(os.path.splitext(file)[1]), controlGroup=controlGroup)

    # ingest my custom datastreams for this object

    # scan for pages
    pages = glob.glob("%s/*.tif" % os.path.join(config.inDir, objectData['label']))
    pages.sort()
    count = len(pages)

    # book thumbnail is the first image
    if not config.dryrun:
        tnFile = os.path.join(config.tempDir, "tmp.jpg")
        converter.tif_to_jpg(os.path.join(bookFolder, pages[0]), tnFile, imageMagicOpts='TN')
        #add a TN datastream to the book
        fedoraLib.update_datastream(bookObj, u"TN", tnFile, label=unicode(config.myCollectionName+"_TN.jpg"), mimeType=misc.getMimeType("jpg"))
        os.remove(tnFile) # delete it so we can recreate it again for the next thumbnail
        # now tnFile is closed and deleted

    print("Build book object with %d pages" % count)

    baseName = objectData['label']

    # ingest my pages
    zipName = objectData['ocr']
    if zipName and zipfile.is_zipfile(zipName):
        ocrzip = zipfile.ZipFile(zipName, "r")
        ocr_list = []
    else:
        ocrzip = None
        ocr_list = None

    fullPDF = os.path.join(config.tempDir, "%s.pdf" % baseName)
    fullOCR = os.path.join(config.tempDir, "%s-full.ocr" % baseName)

    for idx, page in enumerate(pages):
        print("\n==========\nIngesting object %d of %d: %s" % (idx+1, count, page))

        basePage = os.path.splitext(os.path.basename(page))[0]

        #pagePid = fedora.getNextPID(config.fedoraNS)
        pagePid = "%s-%d" % (objPid, idx+1) # objPid contains the namespace part of the pid

        extraNamespaces = { 'pageNS' : 'info:islandora/islandora-system:def/pageinfo#' }
        extraRelationships = { fedora_relationships.rels_predicate('pageNS', 'isPageNumber') : str(idx+1),
                               fedora_relationships.rels_predicate('pageNS', 'isPageOf') : str(objPid) }

        if not config.dryrun:
            # create the object (page)
            try:
                # pageCModel doesn't exist - its just here as a placeholder
                obj = addObjectToFedora(fedora, unicode("%s-%s" % (baseName, basePage)), pagePid, objPid, "archiveorg:pageCModel",
                        extraNamespaces=extraNamespaces, extraRelationships=extraRelationships)
            except FedoraConnectionException, fcx:
                print("Connection error while trying to add fedora object (%s) - the connection to fedora may be broken", page)
                continue

            # ingest the tif
            tifFile = os.path.join(bookFolder, page)
            fedoraLib.update_datastream(obj, u"TIFF", tifFile, label=unicode("%s.tif" % basePage), mimeType=misc.getMimeType("tiff"))

            # create a thumbnail
            tnFile = os.path.join(config.tempDir, "%s_TN.jpg" % basePage)
            converter.tif_to_jpg(tifFile, tnFile, imageMagicOpts='TN')
            fedoraLib.update_datastream(obj, u"TN", tnFile, label=unicode("%s_TN.jpg" % basePage), mimeType=misc.getMimeType("jpg"))
            os.remove(tnFile) # now tnFile is closed and deleted

            # create a JP2 datastream
            jp2File = os.path.join(config.tempDir, "%s.jp2" % basePage)
            converter.tif_to_jp2(tifFile, jp2File, 'default', 'default')
            fedoraLib.update_datastream(obj, u"JP2", jp2File, label=unicode("%s.jp2" % basePage), mimeType=misc.getMimeType("jp2"))
            os.remove(jp2File) # finished with that

            pdfFile = os.path.join(config.tempDir, "%s.pdf" % basePage)
            converter.tif_to_pdf(tifFile, pdfFile, 'default')
            #fedoraLib.update_datastream(obj, u'PDF', pdfFile, label=unicode("%s.pdf" % basePage), mimeType=misc.getMimeType("pdf"))
            # for the first page, move it to the full when finished with it
            if idx == 0:
                os.rename(pdfFile, fullPDF)
            # for every other page (>1), append it to fullPDF and delete the original
            else:
                manipulator.appendPDFwithPDF(fullPDF, pdfFile)
                os.remove(pdfFile)

            if config.jhoveCmd != None: # config.jhoveCmd will be empty if jhove extraction cannot be completed
                # extract mix metadata
                #cmd= jhove -h xml $INFILE | xsltproc jhove2mix.xslt - > `basename ${$INFILE%.*}.mix`
                mixFile = os.path.join(config.tempDir, "%s.mix.xml" % baseName)
                """ extract this into tif_to_mix() """
                outfile = open(mixFile, "w")
                jhoveCmd1 = ["jhove", "-h", "xml", tifFile]
                #jhoveCmd2 = ["xsltproc", "data/jhove2mix.xslt", "-"] # complete cmd for xsltproc
                #jhoveCmd2 = ["xalan", "-xsl", "data/jhove2mix.xslt"] # complete cmd for xalan
                jhoveCmd2 = config.jhoveCmd
                p1 = subprocess.Popen(jhoveCmd1, stdout=subprocess.PIPE)
                p2 = subprocess.Popen(jhoveCmd2, stdin=p1.stdout, stdout=outfile)
                r = p2.communicate()
                if os.path.getsize(mixFile) == 0:
                    # failed for some reason
                    print("jhove conversion failed")
                outfile.close()
                """ end extract """
                fedoraLib.update_datastream(obj, u"MIX", mixFile, label=os.path.basename(mixFile), mimeType=misc.getMimeType("xml"))
                os.remove(mixFile)

            # ingest the ocr if it exists
            if ocrzip:
                # try to find the files' ocr data
                ocrFileName = "%s.txt" % basePage
                if ocrFileName in ocrzip.namelist():
                    ocrFile = ocrzip.extract(ocrFileName, config.tempDir)
                    fedoraLib.update_datastream(obj, u"OCR", os.path.join(config.tempDir, ocrFile), label=unicode(ocrFileName), mimeType=misc.getMimeType("txt"))
                    f = open(os.path.join(config.tempDir, ocrFile),'r')
                    ocr_list.append(f.read())
                    f.close()
                    os.remove(os.path.join(config.tempDir, ocrFileName)) # get rid of that temp file
        sys.stdout.flush()
        sys.stderr.flush()

    # ingest the full PDF on the master book object
    # and delete it
    if not config.dryrun:
        print("Ingesting full PDF document")
        fedoraLib.update_datastream(bookObj, u"PDF", fullPDF, label=os.path.basename(fullPDF), mimeType=misc.getMimeType("pdf"))
        os.remove(fullPDF)

        if ocr_list:
            full_ocr = ''.join(ocr_list)
            full_ocr_file = open(fullOCR, "a")
            full_ocr_file.write(full_ocr)
            full_ocr_file.close()
            fedoraLib.update_datastream(bookObj, u"BOOKOCR", fullOCR, label=os.path.basename(fullOCR), mimeType=misc.getMimeType("txt"))
            os.remove(fullOCR)

    ocrzip.close()

    return True
