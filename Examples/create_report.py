from pathlib import Path
import pickle
import openalexraw as oaraw

# Path to the OpenAlex snapshot
openAlexPath = Path("/gpfs/sciencegenome/OpenAlex/openalex-snapshot")

# Path to where to save the schema files
schemasPath = Path("Schema")

# Initializing the OpenAlex object with the OpenAlex snapshot path
oa = oaraw.OpenAlex(
    openAlexPath = openAlexPath
)

# Creating any necessary directories
schemasPath.mkdir(parents=True, exist_ok=True)

# Creating the schema files
for entityType in ["concepts", "institutions", "venues", "authors", "works"]:
    entitySchema = oa.getSchemaForEntityType(entityType,reportPath=schemasPath/("report_%s.txt"%entityType))
    with open(schemasPath/("schema_%s.p"%entityType),"wb") as f:
        pickle.dump(entitySchema, f)


# Saving the first line with RELEASE of the OpenAlex RELEASE_NOTES.txt to store the dataset version
with open(openAlexPath/"RELEASE_NOTES.txt","r") as fnotes:
    with open(schemasPath/"LAST_UPDATE.txt","w") as fupdate:
        for line in fnotes:
            if line.strip().startswith("RELEASE"):
                fupdate.write(line)
                break



