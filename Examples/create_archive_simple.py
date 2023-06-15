import dbgz
import importlib
import pickle
from pathlib import Path
import ujson

from tqdm.auto import tqdm
import openalexraw

importlib.reload(openalexraw)

oa = openalexraw.OpenAlex(
    openAlexPath = "/gpfs/sciencegenome/OpenAlex/openalex-snapshot"
)

def extractEntry(data,path):
    for pathIndex,key in enumerate(path):
        if(isinstance(data,dict)):
            if(key in data):
                data = data[key]
            else:
                return None
        elif(isinstance(data,list)):
            return [extractEntry(entry,path[pathIndex:]) for entry in data]
        else:
            return None
        
    return data


entityType = "works"

def processOpenAlexID(entityID):
    if(entityID):
        if(isinstance(entityID,str)):
            return int(entityID.replace("https://openalex.org/","")[1:])
        elif(isinstance(entityID,int)):
            return entityID
        else:
            print("Error: ID is not a string or an integer: ",entityID)
            return 0
    else:
        return 0

schemasRich = {}
schemasRich["works"] = [
    ("id", "i", processOpenAlexID),
    ("doi", "s", None),
    ("ids:pmid", "s", None),
    ("title", "s", None),
    ("publication_year", "i", None),
    ("publication_date", "s", None),

    ("host_venue:id", "i", processOpenAlexID),
    ("host_venue:display_name", "s", None),
    ("host_venue:publisher", "s", None),
    ("host_venue:type", "s", None),
    ("host_venue:url", "s", None),
    ("type", "s", None),

    ("is_oa","s", str),
    ("open_access:oa_status", "s", None),
    ("open_access:oa_url", "s", None),

    ("authorships:author_position", "S", None),
    ("authorships:author:id", "I", processOpenAlexID),
    ("authorships:institutions:id", "a", lambda entry: [processOpenAlexID(value) for value in entry] ),

    ("biblio:volume", "s", None),
    ("biblio:issue", "s", None),
    ("biblio:first_page", "s", None),
    ("biblio:last_page", "s", None),

    ("is_retracted", "s", str),
    ("is_paratext", "s", str),

    ("concepts:id", "I", processOpenAlexID),
    ("concepts:score", "F", None),

    ("mesh:is_major_topic", "S", str),
    ("mesh:descriptor_ui", "S", None),
    ("mesh:descriptor_name", "S", None),
    ("mesh:qualifier_ui", "S", None),
    ("mesh:qualifier_name", "S", None),

    # ("alternate_host_venues:id", "I", processOpenAlexID),

    ("referenced_works", "I", processOpenAlexID),
# ("abstract_inverted_index:id", "s", None)
]

schemasRich["authors"] = [
    ("id", "i", processOpenAlexID),
    ("orcid", "s", None),
    ("display_name", "s", None),
    ("last_known_institution:id", "i", processOpenAlexID)
]

schemasRich["venues"] = [
    ("id", "i", processOpenAlexID),
    ("issn_l", "s", None),
    ("display_name", "s", None),
    ("issn", "S", None),
    ("publisher", "s", None),
    ("is_oa", "s", None),
    ("is_in_doaj", "s", None),
    ("homepage_url", "s", None),
]


schemasRich["institutions"] = [
    ("id", "i", processOpenAlexID),
    ("ror", "s", None),
    ("display_name", "s", None),
    ("country_code", "s", None),
    ("type", "s", None),
    ("homepage_url", "s", None),
    ("image_url", "s", None),
    ("image_thumbnail_url", "s", None),
    ("display_name_acronyms", "S", None),
    ("display_name_alternatives", "S", None),
    ("ids:wikipedia", "s", None),
    ("geo:city", "s", None),
    ("geo:geonames_city_id", "s", None),
    ("geo:region", "s", None),
    ("geo:country_code", "s", None),
    ("geo:country", "s", None),
    ("geo:latitude", "f", None),
    ("geo:longitude", "f", None),
    ("associated_institutions:id", "I", processOpenAlexID),
    ("associated_institutions:relationship", "S", None),
]

schemasRich["concepts"] = [
    ("id", "i", processOpenAlexID),
    ("wikidata", "s", None),
    ("display_name", "s", None),
    ("level", "i", None),
    ("description", "s", None),
    ("image_url", "s", None),
    ("image_thumbnail_url", "s", None),
    ("ancestors:id", "I", processOpenAlexID),
    ("related_concepts:id", "I", processOpenAlexID),
]


# schemaWorksAbstract = []

# for entityType in tqdm(["concepts", "institutions", "venues", "authors", "works" ]):
    # entitiesCount = oa.getRawEntityCount(entityType)


# for entityType in tqdm(["concepts", "institutions", "venues", "authors", "works" ]):
for entityType in tqdm(["authors"]):
# for entityType in tqdm(["works"]):
    archiveLocation = "./Processed/%s_simple.dbgz"%entityType
    schema = [(key,dataType) for key,dataType,postProcess in schemasRich[entityType]]
    entitiesCount = oa.getRawEntityCount(entityType)
    with dbgz.DBGZWriter(archiveLocation, schema) as fdbgz:
        for entity in tqdm(oa.rawEntities(entityType),total=entitiesCount,desc=entityType):
            processedEntity = {}
            for key,dataType,postProcess in schemasRich[entityType]:
                path = key.split(":")
                value = extractEntry(entity,path)
                if(postProcess is not None):
                    if(isinstance(value,list)):
                        value = [postProcess(entry) for entry in value]
                    else:
                        value = postProcess(value)
                processedEntity[key] = value
            try:
                fdbgz.write(**processedEntity)
            except AttributeError as e:
                print(e)
                print(processedEntity)
                break

for entityType in tqdm(["authors"]):
# for entityType in tqdm(["concepts", "institutions", "venues", "authors", "works"]):
# for entityType in tqdm(["concepts", "institutions", "venues", "authors", "works" ]):
    entitiesCount = oa.getRawEntityCount(entityType)
    print(entitiesCount)
    archiveLocation = "./Processed/%s_simple.dbgz"%entityType
    indexLocation = "./Processed/%s_simple_byID.idbgz"%entityType
    print("Saving the index dictionary")
    with dbgz.DBGZReader(archiveLocation) as fd:
        print(fd.entriesCount)
        fd.generateIndex(key="id",
                        indicesPath=indexLocation,
                        useDictionary=False,
                        showProgressbar=True
                        )



for entityType in tqdm(["works"]):
# for entityType in tqdm(["concepts", "institutions", "venues", "authors", "works" ]):
    entitiesCount = oa.getRawEntityCount(entityType)
    print(entitiesCount)
    archiveLocation = "./Processed/%s_simple.dbgz"%entityType
    indexLocation = "./Processed/%s_simple_byAuthorID.idbgz"%entityType
    print("Saving the index dictionary")
    with dbgz.DBGZReader(archiveLocation) as fd:
        print(fd.entriesCount)
        fd.generateIndex(key="authorships:author:id",
                        indicesPath=indexLocation,
                        useDictionary=False,
                        showProgressbar=True
                        )


for entityType in tqdm(["works"]):
# for entityType in tqdm(["concepts", "institutions", "venues", "authors", "works" ]):
    entitiesCount = oa.getRawEntityCount(entityType)
    print(entitiesCount)
    archiveLocation = "./Processed/%s_simple.dbgz"%entityType
    indexLocation = "./Processed/%s_simple_byConceptID.idbgz"%entityType
    print("Saving the index dictionary")
    with dbgz.DBGZReader(archiveLocation) as fd:
        print(fd.entriesCount)
        fd.generateIndex(key="concepts:id",
                        indicesPath=indexLocation,
                        useDictionary=False,
                        showProgressbar=True
                        )

for entityType in tqdm(["works"]):
# for entityType in tqdm(["concepts", "institutions", "venues", "authors", "works" ]):
    entitiesCount = oa.getRawEntityCount(entityType)
    print(entitiesCount)
    archiveLocation = "./Processed/%s_simple.dbgz"%entityType
    indexLocation = "./Processed/%s_simple_byJournalID.idbgz"%entityType
    print("Saving the index dictionary")
    with dbgz.DBGZReader(archiveLocation) as fd:
        print(fd.entriesCount)
        fd.generateIndex(key="host_venue:id",
                        indicesPath=indexLocation,
                        useDictionary=False,
                        showProgressbar=True
                        )


for entityType in tqdm(["works"]):
# for entityType in tqdm(["concepts", "institutions", "venues", "authors", "works" ]):
    entitiesCount = oa.getRawEntityCount(entityType)
    print(entitiesCount)
    archiveLocation = "./Processed/%s_simple.dbgz"%entityType
    indexLocation = "./Processed/%s_simple_byCitationID.idbgz"%entityType
    print("Saving the index dictionary")
    with dbgz.DBGZReader(archiveLocation) as fd:
        print(fd.entriesCount)
        fd.generateIndex(key="referenced_works",
                        indicesPath=indexLocation,
                        useDictionary=False,
                        showProgressbar=True
                        )


entityType="works"
archiveLocation = "./Processed/%s_simple.dbgz"%entityType
indexLocation = "./Processed/%s_simple_byID.idbgz"%entityType
indexDictionary = dbgz.readIndicesDictionary(indexLocation,showProgressbar=True)

# # with open(schemaPath,"r") as fSchema:
# #     schema = ujson.load(fSchema)
#     with dbgz.DBGZWriter(archiveLocation, scheme) as fdbgz:
#         for entity in tqdm(oa.rawEntities(entityType),total=entitiesCount,desc=entityType):
#             entityID = int(entity["id"].replace("https://openalex.org/","")[1:])
#             fdbgz.write(ID=entityID, data=entity)



with open("./Processed/keys.dat","wt",encoding="utf-8") as f:
    for key in tqdm(indexDictionary):
        f.write(key+"\n")


# print(oa.getRawEntitiesPaths("works"))
# for entityType in ["concepts", "institutions", "venues", "authors", "works"]:
#     entitySchema = oa.getSchemaForEntityType(entityType,reportPath="Schema/report_%s.txt"%entityType)
#     with open("Schema/schema_%s.p"%entityType,"wb") as f:
#         pickle.dump(entitySchema,f)


# Defining a scheme

# scheme = [
#     ("anInteger", "i"),
#     ("aFloat", "f"),
#     ("aString", "s"),
#     ("anIntArray", "I"),
#     ("aFloatArray", "F"),
#     ("anStringArray", "S"),
#     ("anyType","a"),
# ]

# # Writing some data to a dbgz file
# totalCount = 10000
# print("Saving a dbgz file")
# with dbgz.DBGZWriter("test.dbgz", scheme) as fd:
#     # New entries can be added as:
#     fd.write(anInteger=1, aString="1")
#     fd.write(anInteger=2, aString="2", aFloat=5)
#     fd.write(anInteger=3, aString="3", anIntArray=list(
#         range(10)), aFloatArray=[0.1, 0.2, 0.3, 0.5])
#     # Here is a loop to write a lot of data:
#     for index in tqdm(range(totalCount)):
#         fd.write(
#             anInteger=index,
#             aFloat=index*0.01,
#             anIntArray=list(range(index, index+10)),
#             aString=str(index),
#             aFloatArray=[index+0.1, index-0.2, index+0.3, index+0.4],
#             anStringArray=[str(index), str(index+1),
#                            str(index+2), str(index+3)],
#             anyType={"a": index, "b": index+1, "c": index+2}
#         )

# # Loading a dbgz file
# print("Loading a dbgz file")
# with dbgz.DBGZReader("test.dbgz") as fd:
#     print("\t Number of entries: ", fd.entriesCount)
#     pbar = tqdm(total=fd.entriesCount)
#     print("\t Scheme: ", fd.scheme)
#     while True:
#         entries = fd.read(10)
#         if(not entries):
#             break
#         for entry in entries:
#             assert entry["anInteger"] == int(entry["aString"])
#         pbar.update(len(entries))
# pbar.refresh()
# pbar.close()


# # Creating a dictionary and check
# print("Creating an index")
# with dbgz.DBGZReader("test.dbgz") as fd:
#     indexDictionary = fd.generateIndex("anInteger",
#                                        indicesPath=None,
#                                        filterFunction=lambda entry: entry["anInteger"] < 10,
#                                        useDictionary=True,
#                                        showProgressbar=True
#                                        )
#     for key, values in indexDictionary.items():
#         print(key, values)
#         for value in values:
#             assert int(key) == fd.readAt(value)[0]["anInteger"]


# # Saving dictionary to file and loading it again
# print("Saving the index dictionary")
# with dbgz.DBGZReader("test.dbgz") as fd:
#     fd.generateIndex("anInteger",
#                      indicesPath="test_byAnInteger.idbgz",
#                      filterFunction=lambda entry: entry["anInteger"] < 10,
#                      useDictionary=True,
#                      showProgressbar=True
#                      )

#     print("Loading the index dictionary")
#     indexDictionary = dbgz.readIndicesDictionary("test_byAnInteger.idbgz")
#     for key, values in indexDictionary.items():
#         print(key, values)
#         for value in values:
#             assert int(key) == fd.readAt(value)[0]["anInteger"]
