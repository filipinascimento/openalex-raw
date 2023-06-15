
import openalexraw
import importlib
from tqdm.auto import tqdm

importlib.reload(openalexraw)

inputLocation = "/gpfs/sciencegenome/OpenAlex/TSV/"


entityType = "concepts"
with openalexraw.archive.readTSV(inputLocation,entityType, selection=["core","basic"]) as tsvFile:
    for row in tqdm(tsvFile.entries, desc=entityType):
        pass


entityType = "funders"
with openalexraw.archive.readTSV(inputLocation,entityType, selection=["core","basic"]) as tsvFile:
    for row in tqdm(tsvFile.entries, desc=entityType):
        pass

entityType = "institutions"
with openalexraw.archive.readTSV(inputLocation,entityType, selection=["core","basic","ids"]) as tsvFile:
    for row in tqdm(tsvFile.entries, desc=entityType):
        pass

entityType = "publishers"
with openalexraw.archive.readTSV(inputLocation,entityType, selection=["core","basic"]) as tsvFile:
    for row in tqdm(tsvFile.entries, desc=entityType):
        pass

entityType = "sources"
with openalexraw.archive.readTSV(inputLocation,entityType, selection=["core","basic","ids"]) as tsvFile:
    for row in tqdm(tsvFile.entries):
        pass

entityType = "authors"
with openalexraw.archive.readTSV(inputLocation,entityType, selection=["core","basic","ids"]) as tsvFile:
    for row in tqdm(tsvFile.entries, desc=entityType):
        pass

# entityType = "works"
# with openalexraw.archive.readTSV(inputLocation,entityType, selection=["core","basic","authorship","ids",
#                                           "funding","concepts","references","mesh"]) as tsvFile:
#     for row in tqdm(tsvFile.entries):
#         pass