import warnings
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
    # init

    def __init__(self,
                 openAlexPath = None,
                 processedPath = None,
                 tempPath=None,
                 tagLabel=None,  # current yyyy-mm-dd-hh-mm-ss will be used instead
                 verbose=False
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

    def getManifestPath(self, entityType):
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

    def getManifest(self, entityType):
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
        with open(manifestPath, "r") as f:
            manifest = ujson.load(f)
        return manifest

    def getRawEntityCount(self, entityType):
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

    def getRawEntitiesPaths(self, entityType):
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
        return sorted(self._openAlexPath.glob("data/%s/*/*.gz" % entityType))

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
            with gzip.open(jlpath, "rt") as f:
                lineNumber = 0
                for line in f:
                    lineNumber+=1;
                    try:
                        item = ujson.loads(line)
                        yield item
                    except ValueError:
                        warnings.warn("Found problem loading file %s:%d, Content: %s"%(jlpath, lineNumber, str(line)), category=Warning)

    def generateTagLabel(self):
        """
        Generate a tag name for the current processed data based on the
        format yyyy-mm-dd-hh-mm-ss.
        """
        return datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    def getSchemaForEntityType(self, entityType, reportPath=None, mostCommon=5,
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
            def tqdm(x): return x

        def saveReport():
            if(reportPath is not None):
                with open(reportPath, "w") as file:
                    _printSchema(schemaData,
                                 file=file,
                                 mostCommon=mostCommon,
                                 minPercentageFilter=minPercentageFilter)

        # authors  concepts  institutions  venues  works
        schemaData = OrderedDict()
        entityCount = 0
        for entity in tqdm(self.rawEntities(entityType), total=self.getRawEntityCount(entityType)):
            _addToSchema(schemaData, entity)
            if(entityCount > 0 and entityCount % saveEach == 0):
                saveReport()
            entityCount += 1
        saveReport()
        return schemaData


def cleanScheme(schemaEntry):
    for key, value in schemaEntry.items():
        if(key == "$count"):
            continue
        if("$samples" in value):
            del value["$samples"]
        if("$listSamples" in value):
            del value["$listSamples"]
        if("$listSamples" in value):
            del value["$listSamples"]
        if("$schemaEntry" in value):
            if(value["$schemaEntry"]):
                cleanScheme(value["$schemaEntry"])
            else:
                del value["$schemaEntry"]

def basicTypeName(obj):
    if(isinstance(obj, str)):
        return "string"
    elif(isinstance(obj, (int, float, complex)) and not isinstance(obj, bool)):
        return "number"
    elif(isinstance(obj, bool)):
        return "bool"
    elif(isinstance(obj, list)):
        return "list"
    else:
        return type(obj).__name__

# from https://github.com/smierz/openalex-utils/blob/8e423319987a07fc7b5599b1b4f19069510d6e9a/notebooks/inverted_index_to_text.ipynb
def processInvertedAbstract(entry):
    invertedAbstract = entry or {}
    abstract_index = {}
    for k, vlist in invertedAbstract.items():
        for v in vlist:
            abstract_index[v] = k
    abstract = ' '.join(abstract_index[k] for k in sorted(abstract_index.keys()))
    return abstract.replace("\n", " ").replace("\r", " ").replace("\t", " ")

def _flatten(d, parent_key=""):
    items = []
    for k, v in d.items():
        if(k == "abstract_inverted_index" and v):
            v = "<ABSTRACT>" # avoid large abstracts in the schema when not null
        k = k.replace(":", "_") # no ":" allowed in keys
        new_key = parent_key+":"+str(k) if parent_key else str(k)
        if isinstance(v, collections.MutableMapping):
            items.extend(_flatten(v, new_key).items())
        else:
            items.append((new_key, v))
    return dict(items)



def _addToSchema(schemaData, entryData, flatDict=True, maxSamples=100):
    if(flatDict):
        entryData = _flatten(entryData)
    if "$count" not in schemaData:
        schemaData["$count"] = 0
    schemaData["$count"] += 1
    for key, value in entryData.items():
        if(key not in schemaData):
            schemaData[key] = {
                "$count": 0,
                "$samples": Counter(),
                "$listSamples": Counter(),
                "$schemaEntry": OrderedDict(),
                "$types": Counter()
            }
        if not value:
            continue
        schemaData[key]["$count"] += 1
        schemaData[key]["$types"][basicTypeName(value)] += 1
        if(not isinstance(value, list)):
            if(len(schemaData[key]["$samples"]) < maxSamples):
                schemaData[key]["$samples"][value] += 1
            else:
                if(value in schemaData[key]["$samples"]):
                    schemaData[key]["$samples"][value] += 1
        else:
            for entry in value:
                if(isinstance(entry, dict)):
                    _addToSchema(
                        schemaData[key]["$schemaEntry"], entry, flatDict=flatDict, maxSamples=maxSamples)
                else:
                    if(len(schemaData[key]["$listSamples"]) < maxSamples):
                        schemaData[key]["$listSamples"][entry] += 1
                    else:
                        if(entry in schemaData[key]["$listSamples"]):
                            schemaData[key]["$listSamples"][entry] += 1




def _percentageText(percentage):
    percentageText = "%.2f%%" % percentage
    if(percentage<0.01):
        percentageText = " < 0.01%"
    return percentageText

def _printSchema(schemaEntry, indent=0, mostCommon=5, minPercentageFilter=0, file=sys.stdout):
    for key, value in schemaEntry.items():
        if(key == "$count"):
            print(" "*indent + str(value)+" ENTRIES", file=file)
            continue
        entriesCount = schemaEntry["$count"]
        if(value["$count"]/entriesCount*100 < minPercentageFilter):
            continue
        # if key instance of tuple
        keyLabel = key
        if(isinstance(key, tuple)):
            keyLabel = ":".join(key)
        print((" "*indent)+keyLabel, file=file)
        print((" "*(indent+2))+"Count: %d (%s)" %
              (value["$count"], _percentageText(value["$count"]/entriesCount*100)), file=file)
        typesStringArray = []
        for sampleItem, sampleCount in value["$types"].most_common(mostCommon):
            typesStringArray.append("%s (%s)" % (
                sampleItem,
                _percentageText(sampleCount/value["$count"]*100)))
        if(len(value["$types"]) > mostCommon):
            typesStringArray.append("...")
        print((" "*(indent+2))+"Types: "+", ".join(typesStringArray), file=file)
        if(value["$samples"]):
            print(" "*(indent+2)+"Samples:", file=file)
            for sampleItem, sampleCount in value["$samples"].most_common(mostCommon):
                print((" "*(indent+4))+"%d (%s): %s" % (sampleCount,
                      _percentageText(sampleCount/value["$count"]*100), sampleItem), file=file)
            if(len(value["$samples"]) > mostCommon):
                print((" "*(indent+4))+"...", file=file)
        if(value["$listSamples"]):
            print(" "*(indent+2)+"List Samples:", file=file)
            for sampleItem, sampleCount in value["$listSamples"].most_common(mostCommon):
                print((" "*(indent+4))+"%d (%s): %s" % (sampleCount,
                      _percentageText(sampleCount/value["$count"]*100), sampleItem), file=file)
            if(len(value["$listSamples"]) > mostCommon):
                print((" "*(indent+4))+"...", file=file)
        if(value["$schemaEntry"]):
            print(" "*(indent+2)+"Schema Entry:", file=file)
            _printSchema(value["$schemaEntry"], indent +
                         4, mostCommon=5, file=file)



