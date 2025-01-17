
from tqdm.auto import tqdm
import ujson
from pathlib import Path
import pandas as pd
import csv
import math
import warnings
import openalexraw.openalex as openalex
import numpy as np
# import openalex

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

dataTypeMap = { # Default
    "i":("int",None,-1),
    "s":("str",None,""),
    "f":("float",None,np.nan),
    "d":("double",None,np.nan),
    "a":("json",None,""),
    "I":("json","int", -1),
    "F":("json","float", np.nan),
    "D":("json","float", np.nan),
    "S":("json","str", ""),
    "F":("json","int",-1),
}

dataTypeMapStringToPandasType = {
    "int":"Int64",
    "str":"string",
    "float":"float64",
    "double":"float64",
    "json":"string",
}


def processOpenAlexID(entityID):
    if(entityID):
        if(isinstance(entityID,str)):
            return int(entityID.replace("https://openalex.org/","")[1:])
        elif(isinstance(entityID,int)):
            return entityID
        else:
            print("Error: ID is not a string or an integer: ",entityID)
            return -1
    else:
        return -1
    
schemasPerCategory = {}
schemasPerCategory["works"] = {
    "core":[
        ("id", "i", processOpenAlexID),
    ],
    "basic":[
        ("title", "s", None),
        ("type", "s", None),
        ("publication_year", "i", None),
        ("publication_date", "s", None),
        ("doi", "s", None),
        ("primary_location:source:id", "i", processOpenAlexID),
        ("primary_location:source:type", "s", None),
        ("is_retracted", "s", str),
        ("is_paratext", "s", str),
    ],
    "authorship":[
        ("authorships:author_position", "S", None),
        ("authorships:author:id", "I", processOpenAlexID),
        ("authorships:institutions:id", "a", lambda entry: [processOpenAlexID(value) for value in entry] ),
        ("corresponding_author_ids", "I", processOpenAlexID),
        ("corresponding_institution_ids", "I", processOpenAlexID),
    ],
    "ids":[
        ("ids:doi", "s", None),
        ("ids:mag", "i", None),
        ("ids:pmcid", "s", None),
        ("ids:pmid", "s", None),
    ],
    "biblio":[
        ("biblio:volume", "s", None),
        ("biblio:issue", "s", None),
        ("biblio:first_page", "s", None),
        ("biblio:last_page", "s", None),
    ],
    "open_access":[
        ("open_access:is_oa","s", str),
        ("open_access:oa_status", "s", None),
        ("open_access:any_repository_has_fulltext", "s", None),
        ("open_access:oa_status", "s", None),
    ],
    "open_access_location":[
        ('best_oa_location:is_oa',"s", str),
        ('best_oa_location:landing_page_url',"s", None),
        ('best_oa_location:license', "s", None),
        ('best_oa_location:pdf_url', "s", None),
        ('best_oa_location:source:display_name', "s", None),
        ('best_oa_location:source:host_organization', "s", None),
        ('best_oa_location:source:host_organization_lineage', "s", None),
        ('best_oa_location:source:host_organization_name', "s", None),
        ('best_oa_location:source:id', "i", processOpenAlexID),
        ('best_oa_location:source:issn', "s", None),
        ('best_oa_location:source:issn_l', "s", None),
        ('best_oa_location:source:publisher', "s", None),
        ('best_oa_location:source:publisher_id', "i", processOpenAlexID),
        ('best_oa_location:source:type', "s", None),
        ('best_oa_location:version', "s", None),
    ],
    "source":[
        ("primary_location:source:publisher_id", "i", processOpenAlexID),
    ],
    "funding":[
        ("grants:funder", "I", processOpenAlexID),
        ("grants:award_id", "S", str),
    ],
    "concepts":[
        ("concepts:id", "I", processOpenAlexID),
        ("concepts:score", "F", None),
    ],
    "mesh":[
        ("mesh:is_major_topic", "S", str),
        ("mesh:descriptor_ui", "S", None),
        ("mesh:descriptor_name", "S", None),
        ("mesh:qualifier_ui", "S", None),
        ("mesh:qualifier_name", "S", None),
    ],
    "references":[
        ("referenced_works", "I", processOpenAlexID),
    ],
    "abstract":[
        (("abstract","abstract_inverted_index"), "s", openalex.processInvertedAbstract),
    ],
    "citation_count":[
        ("cited_by_count", "i", None),
    ],
    "citation_count_by_year":[
        ("cited_by_count", "a", None),
    ],
    "apc":[
        ("apc_payment", "a", None),
    ],
    "locations":[
        ("locations", "a", None),
    ]
}

schemasPerCategory["authors"] = {
    "core":[
        ("id", "i", processOpenAlexID),
    ],
    "basic":[
        ("orcid", "s", None),
        ("display_name", "s", None),
        ("last_known_institution:id", "i", processOpenAlexID)
    ],
    "name_alternatives":[
        ("display_name_alternatives", "S", None),
    ],
    "ids":[
        ("ids:orcid", "s", None),
        ("ids:mag", "s", None),
        ("ids:twitter", "s", None),
        ("ids:wikipedia", "s", None),
        ("ids:scopus", "s", None),
    ],
    "citation_count":[
        ("cited_by_count", "i", None),
    ],
    "citation_count_by_year":[
        ("cited_by_count", "a", None),
    ],
    "works_count":[
        ("works_count", "i", None),
    ],
    "concepts":[
        ("x_concepts:id", "I", processOpenAlexID),
        ("x_concepts:score", "F", None),
    ],
    "metrics":[
        ("summary_stats:2yr_h_index", "i", None),
        ("summary_stats:2yr_i10_index", "i", None),
        ("summary_stats:2yr_mean_citedness", "i", None),
        ("summary_stats:h_index", "i", None),
        ("summary_stats:i10_index", "i", None),
    ]
}

# [('abbreviated_title', 0.031994724297111726),
#  ('alternate_titles', 0.24632446434805846),
#  ('apc_prices', 0.06609714362066661),
#  ('apc_usd', 0.02368031999034512),
#  ('cited_by_count', 0.7791790764933817),
#  ('country_code', 0.47910623386363344),
#  ('counts_by_year', 0.9129207308400178),
#  ('created_date', 1.0),
#  ('display_name', 1.0),
#  ('homepage_url', 0.3096070377186895),
#  ('host_organization', 0.29923666096281576),
#  ('host_organization_lineage', 0.2986720228268975),
#  ('host_organization_lineage_names', 0.2986720228268975),
#  ('host_organization_name', 0.29923666096281576),
#  ('id', 1.0),
#  ('ids:fatcat', 0.15712456951729906),
#  ('ids:issn', 0.5118724866060076),
#  ('ids:issn_l', 0.5118724866060076),
#  ('ids:mag', 0.21006262742072437),
#  ('ids:openalex', 1.0),
#  ('ids:wikidata', 0.373570625024245),
#  ('is_in_doaj', 0.06198519872245234),
#  ('is_oa', 0.17898166865654916),
#  ('issn', 0.5118724866060076),
#  ('issn_l', 0.5118724866060076),
#  ('publisher', 0.5937881184619429),
#  ('publisher_id', 0.2937109656174167),
#  ('societies', 0.016689151620425245),
#  ('summary_stats:2yr_cited_by_count', 0.5922666126453081),
#  ('summary_stats:2yr_h_index', 0.28759477084743135),
#  ('summary_stats:2yr_i10_index', 0.07492015327123751),
#  ('summary_stats:2yr_mean_citedness', 0.288823182059162),
#  ('summary_stats:2yr_works_count', 0.4546931773610279),
#  ('summary_stats:cited_by_count', 0.7791790764933817),
#  ('summary_stats:h_index', 0.7901270220295077),
#  ('summary_stats:i10_index', 0.3740016465020452),
#  ('summary_stats:oa_percent', 0.45804652445831373),
#  ('summary_stats:works_count', 0.9480920834285173),
#  ('type', 1.0),
#  ('updated_date', 1.0),
#  ('works_api_url', 1.0),
#  ('works_count', 0.9480920834285173),
#  ('x_concepts', 0.9258298241001349)]
schemasPerCategory["sources"] = {
    "core":[
        ("id", "i", processOpenAlexID),
    ],
    "basic":[
        ("display_name", "s", None),
        ("type", "s", None),
        ("issn", "S", None),
        ("issn_l", "s", None),
        ("publisher", "s", None),
        ("publisher_id", "i", processOpenAlexID),
    ],
    "open_access":[
        ("is_oa", "s", None),
        ("is_in_doaj", "s", None),
    ],
    "urls":[
        ("homepage_url", "s", None),
    ],
    "ids":[
        ("ids:issn", "S", None),
        ("ids:issn_l", "s", None),
        ("ids:mag", "i", processOpenAlexID),
        ("ids:wikidata", "s", None),
        ("ids:fatcat", "s", None),
    ],
    "metrics":[
        ("summary_stats:2yr_cited_by_count", "i", None),
        ("summary_stats:2yr_h_index", "i", None),
        ("summary_stats:2yr_i10_index", "i", None),
        ("summary_stats:2yr_mean_citedness", "i", None),
        ("summary_stats:2yr_works_count", "i", None),
        ("summary_stats:cited_by_count", "i", None),
        ("summary_stats:h_index", "i", None),
        ("summary_stats:i10_index", "i", None),
    ],
    "citation_count":[
        ("cited_by_count", "i", None),
    ],
    "works_count":[
        ("works_count", "i", None),
    ],
    "country":[
        ("country_code", "s", None),
    ],
    "name_alternatives":[
        ("alternate_names", "S", None),
    ],
    "societies":[
        ("societies", "a", None),
    ],
    "concepts":[
        ("x_concepts:id", "I", processOpenAlexID),
        ("x_concepts:score", "F", None),
    ]
}

# [('associated_institutions', 0.19774776970701508),
#  ('cited_by_count', 0.8456783503144347),
#  ('country_code', 0.9999122507678058),
#  ('counts_by_year', 0.8731536099059133),
#  ('created_date', 1.0),
#  ('display_name', 1.0),
#  ('display_name_acronyms', 0.39979525179154685),
#  ('display_name_alternatives', 0.20008774923219422),
#  ('geo:city', 0.9983132647589333),
#  ('geo:country', 0.9983132647589333),
#  ('geo:country_code', 0.9999122507678058),
#  ('geo:geonames_city_id', 0.9980110174035978),
#  ('geo:latitude', 0.993789304343587),
#  ('geo:longitude', 0.993789304343587),
#  ('geo:region', 0.36350606932189344),
#  ('homepage_url', 0.9848778823185297),
#  ('id', 1.0),
#  ('ids:grid', 0.9997855018768586),
#  ('ids:mag', 0.2045532101594111),
#  ('ids:openalex', 1.0),
#  ('ids:ror', 1.0),
#  ('ids:wikidata', 0.2005459952225418),
#  ('ids:wikipedia', 0.34800370496758154),
#  ('image_thumbnail_url', 0.24326037147174961),
#  ('image_url', 0.24326037147174961),
#  ('relationship', 1.9499829376492957e-05),
#  ('repositories', 0.017374347974455223),
#  ('roles', 1.0),
#  ('ror', 1.0),
#  ('summary_stats:2yr_cited_by_count', 0.8018622337054551),
#  ('summary_stats:2yr_h_index', 0.6154926144396237),
#  ('summary_stats:2yr_i10_index', 0.40727343635743185),
#  ('summary_stats:2yr_mean_citedness', 0.6051869546141472),
#  ('summary_stats:2yr_works_count', 0.6977038950909179),
#  ('summary_stats:cited_by_count', 0.8456783503144347),
#  ('summary_stats:h_index', 0.8365816799103007),
#  ('summary_stats:i10_index', 0.7559596353531907),
#  ('summary_stats:oa_percent', 0.7925998147516209),
#  ('summary_stats:works_count', 0.8901769609515917),
#  ('type', 0.999483254521523),
#  ('updated_date', 1.0),
#  ('works_api_url', 1.0),
#  ('works_count', 0.8901769609515917),
#  ('x_concepts', 0.8901379612928387)]
schemasPerCategory["institutions"] = {
    "core":[
        ("id", "i", processOpenAlexID),
    ],
    "basic":[
        ("display_name", "s", None),
        ("type", "s", None),
        ("country_code", "s", None),
    ],
    "alternate_names":[
        ("display_name_acronyms", "S", None),
        ("display_name_alternatives", "S", None),
    ],
    "ids":[
        ("ids:grid", "s", None),
        ("ids:mag", "s", None),
        ("ids:wikidata", "s", None),
        ("ids:wikipedia", "s", None),
        ("ids:ror", "s", None),
    ],
    "geo":[
        ("geo:city", "s", None),
        ("geo:geonames_city_id", "s", None),
        ("geo:region", "s", None),
        ("geo:country_code", "s", None),
        ("geo:country", "s", None),
        ("geo:latitude", "f", None),
        ("geo:longitude", "f", None),
    ],
    "urls":[
        ("homepage_url", "s", None),
        ("image_url", "s", None),
        ("image_thumbnail_url", "s", None),
    ],
    "associated_institutions":[
        ("associated_institutions:id", "I", processOpenAlexID),
        ("associated_institutions:relationship", "S", None),
    ],
    "concepts":[
        ("x_concepts:id", "I", processOpenAlexID),
        ("x_concepts:score", "F", None),
    ],
    "metrics":[
        ("summary_stats:2yr_cited_by_count", "i", None),
        ("summary_stats:2yr_h_index", "i", None),
        ("summary_stats:2yr_i10_index", "i", None),
        ("summary_stats:2yr_mean_citedness", "f", None),
        ("summary_stats:2yr_works_count", "i", None),
        ("summary_stats:cited_by_count", "i", None),
        ("summary_stats:h_index", "i", None),
        ("summary_stats:i10_index", "i", None),
        ("summary_stats:oa_percent", "f", None),
        ("summary_stats:works_count", "i", None),
    ],
    "counts_by_year":[
        ("counts_by_year", "a", None),
    ],
    "roles":[
        ("roles", "a", None),
    ]
    # TODO: Add missing entries
}

schemasPerCategory["concepts"] = {
    "core":[
        ("id", "i", processOpenAlexID),
    ],
    "basic":[
        ("display_name", "s", None),
        ("level", "i", None),
        ("description", "s", None),
        ("ancestors:id", "I", processOpenAlexID),
        ("related_concepts:id", "I", processOpenAlexID),
    ],
    # ("wikidata", "s", None),
    # ("image_url", "s", None),
    # ("image_thumbnail_url", "s", None),
    # TODO: add missing entries
}

# [('alternate_titles', 0.12709760699036837),
#  ('cited_by_count', 0.9680270082414855),
#  ('country_codes', 0.7995233839737861),
#  ('counts_by_year', 0.9796445238804488),
#  ('created_date', 1.0),
#  ('display_name', 1.0),
#  ('hierarchy_level', 0.029490616621983913),
#  ('homepage_url', 0.5567470956210903),
#  ('id', 1.0),
#  ('ids:openalex', 1.0),
#  ('ids:ror', 0.3356171184589415),
#  ('ids:wikidata', 0.8425181213384967),
#  ('image_thumbnail_url', 0.2609472743521001),
#  ('image_url', 0.2609472743521001),
#  ('lineage', 1.0),
#  ('parent_publisher:display_name', 0.029490616621983913),
#  ('parent_publisher:id', 0.029490616621983913),
#  ('roles', 1.0),
#  ('sources_api_url', 1.0),
#  ('sources_count', 0.9945387746996326),
#  ('summary_stats:2yr_cited_by_count', 0.885413563697746),
#  ('summary_stats:2yr_h_index', 0.5894151524178334),
#  ('summary_stats:2yr_i10_index', 0.16284380895640949),
#  ('summary_stats:2yr_mean_citedness', 0.5924932975871313),
#  ('summary_stats:2yr_works_count', 0.747492801112104),
#  ('summary_stats:cited_by_count', 0.9680270082414855),
#  ('summary_stats:h_index', 0.9723959884817793),
#  ('summary_stats:i10_index', 0.6978452983814915),
#  ('summary_stats:oa_percent', 0.6902988779664383),
#  ('summary_stats:sources_count', 0.9945387746996326),
#  ('summary_stats:works_count', 0.9921556945685632),
#  ('updated_date', 1.0),
#  ('works_count', 0.9921556945685632),
#  ('x_concepts', 0.9910634495084897)]
schemasPerCategory["publishers"] = {
    "core":[
        ("id", "i", processOpenAlexID),
    ],
    "basic":[
        ("display_name", "s", None),
        ("country_codes", "s", None),
        ("hierarchy_level", "i", None),
        ("lineage", "I", processOpenAlexID),
    ],
    # TODO: add missing entries
}

# [('alternate_titles', 0.8879674445848876),
#  ('cited_by_count', 0.8848228874433517),
#  ('country_code', 0.9941116626075162),
#  ('counts_by_year', 0.9124764928939174),
#  ('created_date', 1.0),
#  ('description', 0.5320467367512408),
#  ('display_name', 1.0),
#  ('grants_count', 0.7614452631254431),
#  ('homepage_url', 0.5949070505903752),
#  ('id', 1.0),
#  ('ids:openalex', 1.0),
#  ('ids:ror', 0.5425902518728613),
#  ('ids:wikidata', 0.5799241606807042),
#  ('image_thumbnail_url', 0.21728273268181397),
#  ('image_url', 0.21728273268181397),
#  ('roles', 1.0),
#  ('summary_stats:2yr_cited_by_count', 0.8596664303110645),
#  ('summary_stats:2yr_h_index', 0.7630483706877949),
#  ('summary_stats:2yr_i10_index', 0.4962851065141659),
#  ('summary_stats:2yr_mean_citedness', 0.7335758547337916),
#  ('summary_stats:2yr_works_count', 0.8012763202515646),
#  ('summary_stats:cited_by_count', 0.8848228874433517),
#  ('summary_stats:h_index', 0.8777630483706877),
#  ('summary_stats:i10_index', 0.7633566606036316),
#  ('summary_stats:oa_percent', 0.8493078891389463),
#  ('summary_stats:works_count', 0.9124456639023337),
#  ('updated_date', 1.0),
#  ('works_count', 0.9124456639023337),
#  ('x_concepts', 0.9090544748281284)]
schemasPerCategory["funders"] = {
    "core":[
        ("id", "i", processOpenAlexID),
    ],
    "ids":[
        ("ids:wikidata","s",None),
        ("ids:ror","s",None),
        ("ids:crossref","s",None),
        ("ids:doi","s",None),
    ],
    "basic":[
        ("display_name", "s", None),
        ("country_code", "s", None),
        ("roles","s",None),
    ],
    "extra":[
        ("alternate_titles", "S", None),
        ("description", "s", None),
        ("homepage_url","s",None),
    ],
    "counts":[
        ("grants_count", "i", None),
        ("works_count", "i", None),
        ("cited_by_count","i",None),

    ],
    "concepts":[
        ("x_concepts:id", "I", processOpenAlexID),
        ("x_concepts:score", "F", None),
    ],
    # TODO: add missing entries
}





def createDBGZ(oa,entityType, outputLocation, selection=["core","basic"],filterFunction=None):
    import dbgz
    # oa = openalex.OpenAlex(
    #     openAlexPath = "/gpfs/sciencegenome/OpenAlex/openalex-snapshot"
    # )
    # entityType = "concepts"
    # selection = ["core","basic"]
    # outputLocation = "./Processed/"
    filePrefix = "%s_%s"%(entityType,"+".join(selection))
    archiveName = "%s.dbgz"%filePrefix
    archiveLocation = Path(outputLocation)/(archiveName)
    schemaData = []
    for selectedKey in selection:
        # if type of selectedKey is not strong and not in schemasPerCategory[entityType],
        # check if it is a tuple of 1,2,3 elements
        if( isinstance(selectedKey,str) and selectedKey in schemasPerCategory[entityType]):
            for entry in schemasPerCategory[entityType][selectedKey]:
                schemaData.append(entry)
        else:
            if(isinstance(selectedKey,tuple)):
                if(len(selectedKey)==2):
                    schemaData.append((selectedKey[0],selectedKey[1],None))
                elif(len(selectedKey)==3):
                    schemaData.append(selectedKey)
            else:
                schemaData.append(selectedKey)
    schema = [(key if not isinstance(key,tuple) else key[0],dataType) for key,dataType,postProcess in schemaData]
    entitiesCount = oa.getRawEntityCount(entityType)
    with dbgz.DBGZWriter(archiveLocation, schema) as fdbgz:
        for entity in tqdm(oa.rawEntities(entityType),total=entitiesCount,desc=entityType):
            if(filterFunction is not None):
                if(not filterFunction(entity)):
                    continue
            processedEntity = {}
            for key,dataType,postProcess in schemaData:
                if isinstance(key,tuple):
                    key,internalKey = key
                else:
                    internalKey = key
                path = internalKey.split(":")
                value = extractEntry(entity,path)
                if(postProcess is not None):
                    if(isinstance(value,list)):
                        value = [postProcess(entry) for entry in value]
                    else:
                        if(dataType!=dataType.lower()): # force list
                            value = [postProcess(value)]
                        else:
                            value = postProcess(value)
                processedEntity[key] = value
            # try:
            # print(processedEntity)
            fdbgz.write(**processedEntity)
            # except AttributeError as e:
            #     print(e)
            #     print(processedEntity)
            #     print("ERROR!!!!")
            #     break

    # entitiesCount = oa.getRawEntityCount(entityType)
    # print(entitiesCount)
    # archiveLocation = "./Processed/%s_simple.dbgz"%entityType
    # indexLocation = "./Processed/%s_simple_byID.idbgz"%entityType
    # print("Saving the index dictionary")
    # with dbgz.DBGZReader(archiveLocation) as fd:
    #     print(fd.entriesCount)
    #     fd.generateIndex(key="id",
    #                     indicesPath=indexLocation,
    #                     useDictionary=False,
    #                     showProgressbar=True
    #                     )

    # Loading a dbgz file
    print("Testing the dbgz file")
    with dbgz.DBGZReader(archiveLocation) as fd:
        entriesCount = fd.entriesCount
        print("\t Number of entries: ", entriesCount)
        for entry in tqdm(fd.entries,total=entriesCount):
            pass




def createTSV(oa,entityType, outputLocation, selection=["core","basic"],filterFunction=None):
    # # Open your tsv file in write mode ('w').
    # oa = openalex.OpenAlex(
    #     openAlexPath = "/gpfs/sciencegenome/OpenAlex/openalex-snapshot"
    # )
    # entityType = "concepts"
    # selection = ["core","basic"]
    # outputLocation = "./Processed/"
    filePrefix = "%s_%s"%(entityType,"+".join(selection))
    archiveName = "%s.tsv"%filePrefix
    archiveJSONMetadataName = "%s.json"%filePrefix
    archiveLocation = Path(outputLocation)/(archiveName)
    schemaData = [entry for selectedKey in selection for entry in schemasPerCategory[entityType][selectedKey]]
    entitiesCount = oa.getRawEntityCount(entityType)
    writtenEntriesCount = 0
    with open(archiveLocation, 'w', newline='') as tsv_file:
        # Use the csv.writer function, but specify a tab ('\t') as the delimiter. Disable quoting and scaping entirely.
        writer = csv.writer(tsv_file, delimiter='\t', quoting=csv.QUOTE_NONE, doublequote=False, escapechar='\\')

        keysInOrder = [key if not isinstance(key,tuple) else key[0] for key,_,_ in schemaData]
        writer.writerow(keysInOrder)

        for entity in tqdm(oa.rawEntities(entityType),total=entitiesCount,desc=entityType):
            processedEntity = {}
            if(filterFunction is not None):
                if(not filterFunction(entity)):
                    continue
            for key,dataType,postProcess in schemaData:
                if isinstance(key,tuple):
                    key,internalKey = key
                else:
                    internalKey = key
                path = internalKey.split(":")
                value = extractEntry(entity,path)
                _,_,defaultNAValue = dataTypeMap[dataType]
                if(postProcess is not None):
                    if(isinstance(value,list)):
                        value = [postProcess(entry) if entry is not None else defaultNAValue for entry in value if value]
                    else:
                        if(dataType!=dataType.lower()): # force list
                            processedValue = postProcess(value)
                            value = []
                            if(processedValue is not None):
                                value.append(processedValue)
                        else:
                            value = postProcess(value)
                if(dataType!=dataType.lower() or dataType=="a"): # needs json
                    value = ujson.dumps(value)
                if(value is None):
                    value = defaultNAValue
                processedEntity[key] = str(value).replace("\n"," ").replace("\r"," ").replace("\t"," ")
            for key,dataType,postProcess in schemaData:
                if isinstance(key,tuple):
                    key,internalKey = key
                else:
                    internalKey = key
                _,_,defaultNAValue = dataTypeMap[dataType]
                if(key not in processedEntity or processedEntity[key] is None):
                    if(dataType!=dataType.lower()):
                        processedEntity[key] = str(defaultNAValue)
                    else:
                        processedEntity[key] = str([])
            writer.writerow([processedEntity[key] for key in keysInOrder])
            writtenEntriesCount += 1
        
    with open(Path(outputLocation)/(archiveJSONMetadataName), 'w', newline='') as json_file:
        jsonObject = {}
        jsonSchemaData = {}
        for key,dataType,postProcess in schemaData:
            if isinstance(key,tuple):
                key,internalKey = key
            else:
                internalKey = key
            jsonSchemaData[key] = {}
            dataTypeString,dataSubTypeString,defaultNAValue = dataTypeMap[dataType]
            jsonSchemaData[key]["type"] = dataTypeString
            jsonSchemaData[key]["NA value"] = defaultNAValue
            if(dataSubTypeString is not None):
                jsonSchemaData[key]["subtype"] = dataSubTypeString
        jsonObject["count"] = writtenEntriesCount
        jsonObject["schema"] = jsonSchemaData
        ujson.dump(jsonObject,json_file,indent=4)

            
        # import pandas as pd
        # chunksize = 5000
        # length = 65073
        # with tqdm(total=length, desc="chunks read: ") as bar:
        #     for i, chunk in enumerate(pd.read_csv(archiveLocation, chunksize=chunksize,sep="\t",encoding='utf-8',error_bad_lines=False, low_memory=False,
        #     quoting=csv.QUOTE_NONE)):
        #         bar.update(chunksize)
        # print("Done!!!")
        # print(archiveLocation)


class _chunkIterator:
    """
    Iterator that iterates over all the chunks.
    """
    def __init__(self, chunks, chunksCount):
        self._chunks = chunks
        self._chunkCount = chunksCount


    def __iter__(self):
        for chunk in self._chunks:
            yield chunk
    
    def __len__(self):
        return self._chunkCount
    
class _entryIterator:
    """
    Iterator that iterates over all the entries.
    """
    def __init__(self, chunks, entriesCount):
        self._chunks = chunks
        self._entryCount = entriesCount


    def __iter__(self):
        for chunk in self._chunks:
            for entry in chunk.to_dict(orient="records"):
                yield entry
    
    def __len__(self):
        return self._entryCount

class readTSV:
    def __init__(self,filesLocation, entityType, selection=["core","basic"], chunksize = 5000, convertJSON=False):
        # if filesLocation is a .tsv file, no need to use entityType or selection
        fileLocation = Path(filesLocation)
        if(fileLocation.suffix != ".tsv"):
            filePrefix = "%s_%s"%(entityType,"+".join(selection))
            archiveName = "%s.tsv"%filePrefix
            fileLocation = Path(filesLocation)/(archiveName)
        self.fileLocation = fileLocation
        self.chunksize = chunksize
        self.convertJSON = convertJSON
        #check if metadata json exists
        metadataFileLocation = fileLocation.with_suffix(".json")
        if(metadataFileLocation.exists()):
            with open(metadataFileLocation,"r") as metadataFile:
                self.metadata = ujson.load(metadataFile)
            
            self.archiveDTypes = {}
            for key in self.metadata["schema"]:
                dataTypeString = self.metadata["schema"][key]["type"]
                dataType = dataTypeMapStringToPandasType[dataTypeString]
                self.archiveDTypes[key] = dataType
            self.entriesCount = self.metadata["count"]
            self.chunksCount = math.ceil(self.entriesCount/self.chunksize)

        else:
            self.metadata = None
            self.archiveDTypes = None
            self.entriesCount = -1
            self.chunksCount = -1
            warnings.warn("No metadata file found for %s, will use default dtypes and counter not available."%fileLocation)

    def chunkData(self):
        chunks = pd.read_csv(self.fileHandler, chunksize=self.chunksize, sep="\t", encoding='utf-8',
                                            dtype=self.archiveDTypes,
                                            na_values="None", doublequote=False,escapechar="\\",
                                            on_bad_lines="warn", low_memory=False, quoting=csv.QUOTE_MINIMAL)
        if(self.archiveDTypes and self.convertJSON):
            for chunk in chunks:
                for key in self.archiveDTypes:
                    dataTypeString = self.metadata["schema"][key]["type"]
                    if(dataTypeString!=dataTypeString.lower() or dataTypeString=="a"):
                        chunk[key] = chunk[key].apply(lambda x: ujson.loads(x) if isinstance(x,str) else x)
                yield chunk
        else:
            for chunk in chunks:
                yield chunk
    
        #if metadata and types are json, apply json.l
    def __enter__(self):
        # ttysetattr etc goes here before opening and returning the file object
        self.fileHandler = open(self.fileLocation, 'r', newline='')
        return self

    def __exit__(self, type, value, traceback):
        self.fileHandler.close()
        # Exception handling here
        pass

    def __iter__(self):
        for entry in _entryIterator(self.chunkData(),self.entriesCount):
            yield entry
    
    def __len__(self):
        return self.entriesCount
    
    @property
    def entries(self):
        return _entryIterator(self.chunkData(),self.entriesCount)
    
    @property
    def chunks(self):
        return _chunkIterator(self.chunkData(),self.chunksCount)


