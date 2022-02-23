import dbgz
from tqdm.auto import tqdm
import importlib
import openalex
import pickle
importlib.reload(openalex)

oa = openalex.OpenAlex(
    openalexFolder = "/gpfs/sciencegenome/OpenAlex/openalex-snapshot"
)
print(oa.getRawEntitiesPaths("works"))


# for entityType in ["concepts", "institutions", "venues", "authors", "works"]:
#     entitySchema = oa.getSchemaForEntityType(entityType,reportPath="Schema/report_%s.txt"%entityType)
#     with open("Schema/schema_%s.p"%entityType,"wb") as f:
#         pickle.dump(entitySchema,f)


for entityType in ["works"]:
    entitySchema = oa.getSchemaForEntityType(entityType,reportPath="Schema/report_%s.txt"%entityType)
    with open("Schema/schema_%s.p"%entityType,"wb") as f:
        pickle.dump(entitySchema,f)

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
