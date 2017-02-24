#!/usr/bin/python

from datetime import datetime
from datetime import timedelta
import threading
import re
import traceback
import shutil
import subprocess
import sys, os, getopt
import socket, urllib2


#ftp://ftp.nnvl.noaa.gov/View/GOES/playlist.GOES.daily.sos
#https://sos.noaa.gov/Docs/Playlist.html

def downloadImages(outputDir, shortName, layers, resolution):
   
   try:
      wmsBasePath  = "https://gibs.earthdata.nasa.gov/wms/epsg4326/all/wms.cgi?"
      wmsBasePath += "SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&SRS=EPSG:4326&FORMAT=image/png&TRANSPARENT=FALSE&STYLES="
      wmsBasePath += "&WIDTH=" + str(resolution * 1024)
      wmsBasePath += "&HEIGHT=" + str(resolution * 512)
      wmsBasePath += "&BBOX=-180,-90,180,90" 
      wmsBasePath += "&LAYERS="
      for layer in layers:
         wmsBasePath += layer + ","
      wmsBasePath  = wmsBasePath[:-1]
      
      imageDir = os.path.join(outputDir, "Images", "Color", "Daily")
   
      if not os.path.exists(imageDir):
         os.makedirs(imageDir)

      for date in datetimeList:
         remoteWmsPath = wmsBasePath + "&TIME=" + datetime.strftime(date, "%Y-%m-%d")
         localWmsPath  = os.path.join(imageDir, shortName + ".daily." + datetime.strftime(date, "%Y%m%d") + ".color.png")
      
         if verbose: print("Downloading " + remoteWmsPath + " to " + localWmsPath)
      
         remoteWmsUrl = urllib2.urlopen(remoteWmsPath)
   
         with open(localWmsPath, 'w') as f:
            f.write(remoteWmsUrl.read())
      
         remoteWmsUrl.close()
         
   except:
      print(traceback.format_exc())


def writePlaylistFile(outputDir, shortName, longName):
   
   with open(os.path.join(outputDir, "playlist." + shortName + ".daily.sos"), 'w') as f:
      f.write("name = " + longName + "\n")
      f.write("layer = NASA Imagery\n")   
      f.write("layerdata = images\n")
      f.write("fps = 4\n")
      f.write("label = labels.txt\n")
      f.write("labelColor = white\n")
      #f.write("pip = " + shortName + ".SOS.colorbar.png")
      #f.write("pipvertical = -33")
      f.write("majorcategory = Site-Custom\n")
      f.write("subcategory = Uncategorized\n")
      f.write("source = NASA Worldview\n")
      f.write("creator = NASA Global Imagery Browse Services\n")
      
      #f.write("framewidth = width"
      

def writeLabelsFile(outputDir, shortName):

   with open(os.path.join(outputDir, "labels.txt"), 'w') as f:
      
      for date in datetimeList[::-1]:
         f.write(datetime.strftime(date, "%Y-%m-%d\n"))


def writeAboutFile(outputDir, shortName, layers):
   
   with open(os.path.join(outputDir, "About_" + shortName + ".txt"), 'w') as f:
      f.write("About " + shortName + "\n\n")
      f.write("Layers: \n")
      
      for layer in layers:
         f.write("\t" + layer + "\n")

      f.write("\nTime steps: \n")
      f.write("\tDaily\n")

      #Range: -5 to 5
      #Units: degrees C

      f.write("\nSource https://worldview.earthdata.nasa.gov\n\n") 
      f.write("Files available: colorized PNG\n\n")
      
      f.write("File naming syntax:\n")
      f.write("\t####.text.&&&&$$.color.png\n\n")
      f.write("Where #### is the 4-letter data code; \"text\" is either 'daily', 'monthly', or 'yearly'; " + \
              "&&&& is the year; $$ is the week or month number\n")

      f.write("e.g., TMTA.weekly.199508.color.png is the colored Middle-Troposphere temperature departure image for August 1995.\n") 



def usage():
   print ("process.py [OPTIONS]")
   print (   "-l/--layers       : Comma separated list of layers")
   print (   "-s/--start-date   : Start date, inclusive.  'YYYY-MM-DD'")
   print (   "-e/--end-date     : End date, inclusive.  'YYYY-MM-DD')")
   print (   "-r/--resolution   : Output image resolution as powers of 2 km. (e.g. '4')")
   print (   "-t/--threads      : The number of concurrent threads to spawn for imagery processing (Optional)")
   print (   "-v/--verbose      : Verbose Output")

      
def main(argv):

   # timeout in seconds
   timeout = 90
   socket.setdefaulttimeout(timeout)
   
   global processSemaphore
   global datetimeList
   global verbose

   startDate  = None
   endDate    = None
   layers     = []
   maxThreads = 1
   resolution = 4
   verbose    = False

   # The datetime.strptime() function needs to be initialized in the main thread because it is 
   # not thread-safe otherwise.  http://bit.ly/1TQOQtS
   datetime.strptime(datetime.strftime(datetime.utcnow(), "%Y-%m-%dT00:00:00Z"), "%Y-%m-%dT%H:%M:%SZ")

   try:
      opts, args = getopt.getopt(sys.argv[1:],"hs:e:l:t:r:v",["start_date=","end_date=","layers=","resolution=","threads=","verbose"])
   except getopt.GetoptError:
      usage()
      sys.exit(2)
    
   for opt, arg in opts:
      if opt == '-h':
         usage()
         sys.exit()
      elif opt in ("-s", "--start_date"):
         if re.match("[0-9]{4}-[0-9]{2}-[0-9]{2}", arg):
            startDate = datetime.strptime(arg, "%Y-%m-%d")
         else:
            print("Invalid Start Date Value.")
            exit(-1)
      elif opt in ("-e", "--end_date"):
         if re.match("[0-9]{4}-[0-9]{2}-[0-9]{2}", arg):
            endDate = datetime.strptime(arg, "%Y-%m-%d")
         else:
            print("Invalid End Date Value")
            exit(-1)
      elif opt in ("-l", "--layers"):
         layers = map(str.strip, arg.split(","))
      elif opt in ("-r", "--resolution"):
         resoution = int(arg)
         # TODO - Validate argument
      elif opt in ("-t", "--threads"):
         maxThreads = int(arg) 
      elif opt in ("-v", "--verbose"):
         verbose = True  

   if (startDate and not endDate) or (endDate and not startDate):
      print("If you provide a start or end date, you have to provide both.")
      exit(-1)
   elif startDate and endDate:
      if startDate > endDate:
         print("Start date is after the end date.")
         exit(-1)
      else:
         datetimeList = [endDate - timedelta(days=x) for x in range(0, (endDate - startDate).days + 1)]

   if len(layers) == 0:
      print("Layers must be provided.")
      exit(-1)
      
   processSemaphore = threading.BoundedSemaphore(maxThreads)
   
   shortName = "GIBS"
   longName  = "GIBS Test Imagery"


   outputDir = os.path.join("output", shortName)
   
   if not os.path.exists(outputDir):
      os.makedirs(outputDir)
      
   downloadImages(outputDir, shortName, layers, resoution)
   writePlaylistFile(outputDir, shortName, longName)
   writeLabelsFile(outputDir, shortName)
   writeAboutFile(outputDir, shortName, layers)


if __name__ == "__main__":
   main(sys.argv[1:])