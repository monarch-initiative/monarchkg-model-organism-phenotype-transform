import uuid  # For generating UUIDs for associations
import gzip
import shutil
import duckdb

from biolink_model.datamodel.pydanticmodel_v2 import PhenotypicFeature, OrganismTaxon, Association
from koza.cli_utils import get_koza_app

koza_app = get_koza_app("monarchkg-phenotype_phenotype_to_phenotype")


#https://stackoverflow.com/a/44712152
with gzip.open('data/monarch-kg.duckdb.gz', 'rb') as f_in:
    with open('data/monarch-kg.duckdb', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

phenotype_namespace_to_taxon = {
    "DDPHENO":"NCBITaxon:44689", #https://obofoundry.org/ontology/ddpheno.html
    "FYPO":"NCBITaxon:4894", #https://obofoundry.org/ontology/fypo.html
    "HP":"NCBITaxon:9606", #https://obofoundry.org/ontology/hp.html
    "MP":"NCBITaxon:10090", #https://obofoundry.org/ontology/mp.html; also possible NCBITaxon:40674
    "NBO":None, #https://obofoundry.org/ontology/nbo.html
    "UPHENO":None, #https://obofoundry.org/ontology/upheno.html
    "WBPhenotype":"NCBITaxon:6239", #https://obofoundry.org/ontology/wbphenotype.html
    "XPO":"NCBITaxon:8353", #https://obofoundry.org/ontology/xpo.html
    "ZP":"NCBITaxon:7955", #https://obofoundry.org/ontology/zp.html
}

db = duckdb.connect(database='data/monarch-kg.duckdb')

phenotype_query = db.sql("SELECT n.id, n.name FROM nodes n WHERE n.category='biolink:PhenotypicFeature'")
while True:
    phenotype = phenotype_query.fetchone()
    #Duckdb's api returns None when it's out of rows.
    if(phenotype==None):break

    namespace = phenotype[0].split(":")[0]
    taxon = phenotype_namespace_to_taxon[namespace]
    #Ignore all phenotypic terms which don't map onto an ontology
    # term cleanly.
    if(taxon==None):continue
    print(taxon)
    taxon_entity = OrganismTaxon(
            id=taxon,
            category=["biolink:OrganismTaxon"]
    )
    phenotype_entity = PhenotypicFeature(
        id=phenotype[0],
        name=phenotype[1],
        category=["biolink:PhenotypicFeature"],
    )
    association = Association(
        id=str(uuid.uuid1()),
        subject=phenotype_entity.id,
        predicate="biolink:in_taxon",
        object=taxon_entity.id,
        subject_category="biolink:PhenotypicFeature",
        object_category="biolink:OrganismTaxon",
        category=["biolink:Association"],
        knowledge_level="not_provided",
        agent_type="not_provided",
    )
    koza_app.write(phenotype_entity, taxon_entity, association)

while (row := koza_app.get_row()) is not None:
    continue
    # Code to transform each row of data
    # For more information, see https://koza.monarchinitiative.org/Ingests/transform
    entity_a = Entity(
        id=f"XMPL:00000{row['example_column_1'].split('_')[-1]}",
        name=row["example_column_1"],
        category=["biolink:Entity"],
    )
    entity_b = Entity(
        id=f"XMPL:00000{row['example_column_2'].split('_')[-1]}",
        name=row["example_column_2"],
        category=["biolink:Entity"],
    )
    association = Association(
        id=str(uuid.uuid1()),
        subject=row["example_column_1"],
        predicate=row["example_column_3"],
        object=row["example_column_2"],
        subject_category="SUBJ",
        object_category="OBJ",
        category=["biolink:Association"],
        knowledge_level="not_provided",
        agent_type="not_provided",
    )
    koza_app.write(entity_a, entity_b, association)
