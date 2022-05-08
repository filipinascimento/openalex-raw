
from pathlib import Path
import sys
import os
import ujson
import tempfile
import datetime
# import json_lines SLOWER
import gzip

from collections import OrderedDict
from collections import Counter
import collections

class OpenAlex:
    '''
    OpenAlex is a class for accessing the OpenAlex data snapshots
    '''
    #init
    def __init__(self,
        openAlexPath,
        processedPath = None,
        tempPath = None,
        tagLabel = None, # current yyyy-mm-dd-hh-mm-ss will be used instead
        verbose = False
        ):
        """
        OpenAlex is a class for accessing the OpenAlex data snapshots.
        
        Parameters
        ----------
        openAlexPath : str or pathlib.Path
            The path to the OpenAlex directory. (default: current working directory)
        processedPath : str or pathlib.Path
            The path to the processed directory used to create indices.
            (default: current working directory, optional if reading from raw data)
        tempPath : str or pathlib.Path
            The path to the temp directory. (default: regular system temp directory)
        tagLabel : str
            The tag label for the current snapshot. (default: current date and time)
        verbose : bool
            If True, print out more information. (default: False)
        """

        if(openAlexPath is None):
            openAlexPath = Path.cwd()
        
        if (processedPath is None):
            processedPath = Path.cwd()

        if (tempPath is not None):
            tempPath = Path(tempPath)

        if (tagLabel is None):
            tagLabel = self.generateTagLabel()
        
        self._openAlexPath = Path(openAlexPath)
        self._processedPath = Path(processedPath)
        self._tempPath = tempPath
        self._tagLabel = tagLabel
        self._verbose = verbose


    def getManifestPath(self,entityType):
        """
        Get the path to the manifest file for the given entity type.
        
        Parameters
        ----------
        entityType : str
            The data: ("authors" | "concepts" | "institutions" | "venues" | "works")
        
        
        Returns
        -------
        path
            The path to the manifest file for the given entity type.

        """
        return self._openAlexPath/"data"/entityType/"manifest"
    

    def getManifest(self,entityType):
        """
        Get the manifest for the given entity type.
        
        Parameters
        ----------
        entityType : str
            The data: ("authors" | "concepts" | "institutions" | "venues" | "works")
        

        Returns
        -------
        manifest dict
            The manifest for the given entity type.
        """
        manifestPath = self.getManifestPath(entityType)
        with open(manifestPath,"r") as f:
            manifest = ujson.load(f)
        return manifest
    

    def getRawEntityCount(self,entityType):
        """
        Get the number of raw entities of the given entity type.

        Parameters
        ----------
        entityType : str
            The data: ("authors" | "concepts" | "institutions" | "venues" | "works")
        

        Returns
        -------
        int
            The number of raw entities of the given entity type.
        """
        manifestData = self.getManifest(entityType)
        # count = 0
        # for entry in manifestData["entries"]:
        #     count+=int(entry["meta"]["record_count"])
        return manifestData["meta"]["record_count"]


    def getRawEntitiesPaths(self,entityType):
        """
        Get the paths to the raw data on entities of the given entity type.

        Parameters
        ----------
        entityType : str
            The data: ("authors" | "concepts" | "institutions" | "venues" | "works")
        

        Returns
        -------
        paths iterable
            A iterable collection of paths to the raw data on entities of the given entity type.
        """
        return sorted(self._openAlexPath.glob("data/%s/*/*.gz"%entityType))
        

    def rawEntities(self, entityType):
        """
        Iterate over the entities of the selected type directly from the raw snapshot.

        Parameters
        ----------
        entityType : str
            The entity type: ("authors" | "concepts" | "institutions" | "venues" | "works")
        

        Returns
        -------
        entities iterable
            An iterable collection of entities of the selected type.
        """
        from tqdm.auto import tqdm
        for jlpath in self.getRawEntitiesPaths(entityType):
            # SLOWER
            # with json_lines.open(jlpath) as f:
            #     print("starting to read %s"%jlpath)
            #     for item in tqdm(f,total=self.getRawEntityCount(entityType)):
            #          pbar.update(1)
            with gzip.open(jlpath,"rt") as f:
                for line in f:
                    item = ujson.loads(line)
                    yield item


    def generateTagLabel(self):
        """
        Generate a tag name for the current processed data based on the
        format yyyy-mm-dd-hh-mm-ss.
        """
        return datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")


    def getSchemaForEntityType(self, entityType, reportPath=None, mostCommon = 5,
        saveEach=100000, minPercentageFilter=0, file=sys.stdout):
        """
        Get the schema for the given entity type.

        Parameters
        ----------
        entityType : str
            The entity type: ("authors" | "concepts" | "institutions" | "venues" | "works")
        reportPath : str or pathlib.Path
            The path to save a report file. If not specified, the text report will
            not be sabed. (default: None)
        mostCommon : int
            The number of most common values to be included in the schema. (default: 5)
        saveEach : int
            The number of values to be included in the schema before saving to disk. (default: 1000)

        Returns
        -------
        schema dict
            The schema for the given entity type.
        """
        try:
            from tqdm.auto import tqdm
        except ImportError:
            tqdm = lambda x: x
        
        def saveReport():
            if(reportPath is not None):
                with open(reportPath,"w") as file:
                    _printSchema(schemaData,
                        file=file,
                        mostCommon = mostCommon,
                        minPercentageFilter=minPercentageFilter)

        # authors  concepts  institutions  venues  works
        schemaData = OrderedDict()
        entityCount = 0
        for entity in tqdm(self.rawEntities(entityType),total=self.getRawEntityCount(entityType)):
            _addToSchema(schemaData,entity)
            if(entityCount>0 and entityCount%saveEach==0):
                saveReport()
            entityCount+=1
        saveReport()
        return schemaData





def _flatten(d, parent_key=tuple()):
    items = []
    for k, v in d.items():
        if(k=="abstract_inverted_index"):
            continue
        new_key = parent_key + (k,) if parent_key else (k,)
        if isinstance(v, collections.MutableMapping):
            items.extend(_flatten(v, new_key).items())
        else:
            items.append((new_key, v))
    return dict(items)



def _addToSchema(schemaData, entryData,maxSamples=100):
  flatData = _flatten(entryData)
  if "__COUNT__" not in schemaData:
    schemaData["__COUNT__"] = 0
  schemaData["__COUNT__"] += 1
  for key, value in flatData.items():
    if(key not in schemaData):
      schemaData[key] = {
        "count": 0,
        "samples": Counter(),
        "listSamples": Counter(),
        "schemaEntry" : OrderedDict(),
        "types": Counter()
      }
    if not value:
        continue
    schemaData[key]["count"] += 1
    schemaData[key]["types"][type(value)] += 1
    if(not isinstance(value,list)):
      if(len(schemaData[key]["samples"])<maxSamples):
        schemaData[key]["samples"][value] += 1
      else:
        if(value in schemaData[key]["samples"]):
          schemaData[key]["samples"][value] += 1
    else:
      for entry in value:
        if(isinstance(entry,dict)):
          _addToSchema(schemaData[key]["schemaEntry"], entry)
        else:
          if(len(schemaData[key]["listSamples"])<maxSamples):
            schemaData[key]["listSamples"][entry] += 1
          else:
            if(entry in schemaData[key]["listSamples"]):
              schemaData[key]["listSamples"][entry] += 1


def _printSchema(schemaEntry,indent = 0,mostCommon = 5,minPercentageFilter=0,file=sys.stdout):
  for key, value in schemaEntry.items():
    if(key=="__COUNT__"):
      print(" "*indent + str(value)+" ENTRIES",file=file);
      continue
    entriesCount = schemaEntry["__COUNT__"]
    if(value["count"]/entriesCount*100<minPercentageFilter):
      continue
    print((" "*indent)+":".join(key),file=file)
    print((" "*(indent+2))+"Count: %d (%.2f%%)"%(value["count"],value["count"]/entriesCount*100),file=file)
    typesStringArray = []
    for sampleItem, sampleCount in value["types"].most_common(mostCommon):
      typesStringArray.append("%s (%.2f%%)"%(sampleItem.__name__,sampleCount/value["count"]*100))
    if(len(value["types"])>mostCommon):
      typesStringArray.append("...")
    print((" "*(indent+2))+"Types: "+", ".join(typesStringArray),file=file)
    if(value["samples"]):
      print(" "*(indent+2)+"Samples:",file=file)
      for sampleItem,sampleCount in value["samples"].most_common(mostCommon):
        print((" "*(indent+4))+"%d (%.2f%%): %s"%(sampleCount,sampleCount/value["count"]*100,sampleItem),file=file)
      if(len(value["samples"])>mostCommon):
        print((" "*(indent+4))+"...",file=file)
    if(value["listSamples"]):
      print(" "*(indent+2)+"List Samples:",file=file)
      for sampleItem,sampleCount in value["listSamples"].most_common(mostCommon):
        print((" "*(indent+4))+"%d (%.2f%%): %s"%(sampleCount,sampleCount/value["count"]*100,sampleItem),file=file)
      if(len(value["listSamples"])>mostCommon):
        print((" "*(indent+4))+"...",file=file)
    if(value["schemaEntry"]):
      print(" "*(indent+2)+"Schema Entry:",file=file)
      _printSchema(value["schemaEntry"],indent+4,mostCommon = 5,file=file)
  

