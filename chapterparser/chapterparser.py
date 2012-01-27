#!/usr/bin/python

import sys

try:
  from lxml import etree
  print("running with lxml.etree")
except ImportError:
  try:
    # Python 2.5
    import xml.etree.cElementTree as etree
    print("running with cElementTree on Python 2.5+")
  except ImportError:
    try:
      # Python 2.5
      import xml.etree.ElementTree as etree
      print("running with ElementTree on Python 2.5+")
    except ImportError:
      try:
        # normal cElementTree install
        import cElementTree as etree
        print("running with cElementTree")
      except ImportError:
        try:
          # normal ElementTree install
          import elementtree.ElementTree as etree
          print("running with ElementTree")
        except ImportError:
          print("Failed to import ElementTree from any known place")

def parseChapters(filename):
  doc = etree.parse(filename).getroot()

  xmlChapters = doc.findall(".//HEAD")
  xmlChapters.extend(doc.findall(".//head"))
  dictChapters = { 'level': 1, 'label': 1, 'pagenum': 1, 'title': '' }

  for chapter in xmlChapters:
    # get inner text
    print (str(chapter))
    print("Title = %s" % chapter.xpath(".//text()")[0])
    page = chapter.xpath("//PB")
    if (not page):
        page = doc.xpath("//pb")
        if (not page):
            continue
    #print("Page= %s" % page.xpath("@SEQ"))
    # get first child and SEQ attribute

def main(argv):
  parseChapters(argv[1])

  return 0

if __name__ == "__main__":
  sys.exit(main(sys.argv))
