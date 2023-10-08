
import openalexraw
import importlib

importlib.reload(openalexraw)

oa = openalexraw.OpenAlex(
    openAlexPath = "/gpfs/sciencegenome/OpenAlex/openalex-snapshot"
    #Path to OpenAlex data
)


outputLocation = "/gpfs/sciencegenome/OpenAlex/TSV/"
#Path to the output

entityType = "concepts"
openalexraw.archive.createTSV(oa,entityType, outputLocation, selection=["core","basic"])

entityType = "funders"
openalexraw.archive.createTSV(oa,entityType, outputLocation, selection=["core","basic"])

entityType = "institutions"
openalexraw.archive.createTSV(oa,entityType, outputLocation, selection=["core","basic","ids"])

entityType = "publishers"
openalexraw.archive.createTSV(oa,entityType, outputLocation, selection=["core","basic"])

entityType = "sources"
openalexraw.archive.createTSV(oa,entityType, outputLocation, selection=["core","basic","ids"])

entityType = "authors"
openalexraw.archive.createTSV(oa,entityType, outputLocation, selection=["core","basic","ids"])

entityType = "works"
openalexraw.archive.createTSV(oa,entityType, outputLocation,
                               selection=["core","basic","authorship","ids",
                                          "funding","concepts","references","mesh"])


entityType = "works" # A file for the abstracts
openalexraw.archive.createTSV(oa,entityType, outputLocation,
                               selection=["core","abstract"])


