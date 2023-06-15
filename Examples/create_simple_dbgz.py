
import openalexraw
import importlib

importlib.reload(openalexraw)


oa = openalexraw.OpenAlex(
    openAlexPath = "/gpfs/sciencegenome/OpenAlex/openalex-snapshot"
    #Path to OpenAlex data
)


outputLocation = "/gpfs/sciencegenome/OpenAlex/DBGZ/"
#Path to the output

entityType = "concepts"
openalexraw.archive.createDBGZ(oa,entityType, outputLocation, selection=["core","basic"])

entityType = "funders"
openalexraw.archive.createDBGZ(oa,entityType, outputLocation, selection=["core","basic"])

entityType = "institutions"
openalexraw.archive.createDBGZ(oa,entityType, outputLocation, selection=["core","basic"])

entityType = "publishers"
openalexraw.archive.createDBGZ(oa,entityType, outputLocation, selection=["core","basic"])

entityType = "sources"
openalexraw.archive.createDBGZ(oa,entityType, outputLocation, selection=["core","basic"])

entityType = "authors"
openalexraw.archive.createDBGZ(oa,entityType, outputLocation, selection=["core","basic"])

entityType = "works"
openalexraw.archive.createDBGZ(oa,entityType, outputLocation,
                               selection=["core","basic","authorship","ids",
                                          "funding","concepts","references","mesh"])

