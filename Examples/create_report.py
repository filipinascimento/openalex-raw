#!/usr/bin/env python
import sys
from pathlib import Path
import ujson
import openalexraw as oaraw

if __name__ == "__main__":
    # Path to the OpenAlex snapshot
    if len(sys.argv) > 1:
        openAlexPath = Path(sys.argv[1])
    else:
        openAlexPath = Path("/gpfs/sciencegenome/OpenAlex/openalex-snapshot")

    # Path to where to save the schema files
    schemaPath = Path("Schema")

    # Path to where to save the reports files
    reportsPath = Path("Reports")

    # Initializing the OpenAlex object with the OpenAlex snapshot path
    oa = oaraw.OpenAlex(
        openAlexPath = openAlexPath
    )

    # Creating any necessary directories
    schemaPath.mkdir(parents=True, exist_ok=True)
    reportsPath.mkdir(parents=True, exist_ok=True)

    # Creating the schema files
    for entityType in ["concepts", "institutions","funders", "publishers", "sources", "authors", "works" ]:
        entitySchema = oa.getSchemaForEntityType(entityType,reportPath=reportsPath/("report_%s.txt"%entityType))
        with open(schemaPath/("schema_%s_samples.json"%entityType),"wt") as f:
            ujson.dump(entitySchema, f, ensure_ascii=False, indent=4)
        oaraw.cleanScheme(entitySchema)
        with open(schemaPath/("schema_%s.json"%entityType),"wt") as f:
            ujson.dump(entitySchema, f, ensure_ascii=False, indent=4)

    # Saving the first line with RELEASE of the OpenAlex RELEASE_NOTES.txt to store the dataset version
    with open(openAlexPath/"RELEASE_NOTES.txt","r") as fnotes:
        with open(schemaPath/"LAST_UPDATE.txt","w") as fupdateSchema:
            with open(reportsPath/"LAST_UPDATE.txt","w") as fupdateReport:
                for line in fnotes:
                    if line.strip().startswith("RELEASE"):
                        fupdateReport.write(line)
                        fupdateSchema.write(line)
                        break



