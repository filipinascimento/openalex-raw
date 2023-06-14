
import openalexraw
import importlib

importlib.reload(openalexraw)

oa = openalexraw.OpenAlex(
    openAlexPath = "/gpfs/sciencegenome/OpenAlex/openalex-snapshot"
)


# entityTypes = ["concepts","funders","institutions","publishers","sources","authors","works"]
outputLocation = "/gpfs/sciencegenome/OpenAlex/DBGZ/"
# entityType = "concepts"
# openalexraw.archive.createDBGZ(oa,entityType, outputLocation, selection=["core","basic"])

# entityType = "funders"
# openalexraw.archive.createDBGZ(oa,entityType, outputLocation, selection=["core","basic"])

# entityType = "institutions"
# openalexraw.archive.createDBGZ(oa,entityType, outputLocation, selection=["core","basic"])

# entityType = "publishers"
# openalexraw.archive.createDBGZ(oa,entityType, outputLocation, selection=["core","basic"])

# entityType = "sources"
# openalexraw.archive.createDBGZ(oa,entityType, outputLocation, selection=["core","basic"])

# entityType = "authors"
# openalexraw.archive.createDBGZ(oa,entityType, outputLocation, selection=["core","basic"])

entityType = "works"
openalexraw.archive.createDBGZ(oa,entityType, outputLocation,
                               selection=["core","basic","authorship","ids",
                                          "funding","concepts","references","mesh"])

