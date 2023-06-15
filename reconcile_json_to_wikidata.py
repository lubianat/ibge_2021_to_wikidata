import csv
import logging
import time
from datetime import datetime
import pandas as pd
import re
from SPARQLWrapper import SPARQLWrapper, JSON
from wikibaseintegrator import WikibaseIntegrator
import json
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator import wbi_login

from wikibaseintegrator import WikibaseIntegrator, wbi_fastrun, wbi_login
from wikibaseintegrator.datatypes import ExternalID, Item, Quantity, Time, URL
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_enums import ActionIfExists, WikibaseRank
from wikibaseintegrator.wbi_exceptions import MWApiError
from login import *

wbi_config["USER_AGENT"] = "Update Population of Brazilian Cities"


# Write JSON to a file
with open("population.json", "r") as file:
    population_dict = json.loads(file.read())


# SPARQL query to get Wikidata QIDs for Brazilian municipalities
sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

query = """
SELECT ?item ?itemLabel ?code
WHERE
{
  ?item wdt:P31 wd:Q3184121; # is a municipality of Brazil
        wdt:P1585 ?code. # has a IBGE code
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
"""

sparql.setQuery(query)
sparql.addCustomHttpHeader("User-Agent", "ibge_2021_to_wikidata")

sparql.setReturnFormat(JSON)
results = sparql.query().convert()

# Create a dict with Wikidata QID and IBGE code
wikidata_dict = {
    result["code"]["value"]: result["item"]["value"].replace(
        "http://www.wikidata.org/entity/", ""
    )
    for result in results["results"]["bindings"]
}

# Create a Wikidata QID to population estimate dict
qid_population_dict = {
    wikidata_dict[k]: v for k, v in population_dict.items() if k in wikidata_dict
}

# login object
login_instance = wbi_login.Login(user=WDUSER, password=WDPASS)

wbi = WikibaseIntegrator(login=login_instance, is_bot=False)

logging.basicConfig(level=logging.DEBUG)

qualifiers = [
    Time(prop_nr="P585", time="+2021-07-01T00:00:00Z"),  # point in time
    Item(prop_nr="P459", value="Q791801"),  # determination method: estimate
]

references = [
    [
        URL(
            value="https://www.ibge.gov.br/estatisticas/sociais/populacao/9103-estimativas-de-populacao.html?edicao=31451",
            prop_nr="P854",
        )
    ]
]

base_filter = [
    Item(prop_nr="P31", value="Q3184121"),  # instance of municipality of Brazil
    Item(prop_nr="P17", value="Q155"),  # country Brazil
    ExternalID(prop_nr="P1585"),  # IBGE municipality code
]

print("Creating fastrun container")
frc = wbi_fastrun.get_fastrun_container(base_filter=base_filter)

for qid, pop in qid_population_dict.items():
    claims = [
        Quantity(
            amount=pop,
            prop_nr="P1082",
            references=references,
            qualifiers=qualifiers,
            rank=WikibaseRank.PREFERRED,
        ),
    ]

    id_item = qid

    logging.info(f"Write to Wikidata to {qid}")
    try:
        logging.debug("write")
        update_item = wbi.item.get(id_item)

        for claim in update_item.claims.get("P1082"):
            if claim.rank == WikibaseRank.PREFERRED:
                claim.rank = WikibaseRank.NORMAL

        update_item.claims.add(
            claims=Quantity(
                amount=pop,
                prop_nr="P1082",
                references=references,
                qualifiers=qualifiers,
                rank=WikibaseRank.PREFERRED,
            ),
            action_if_exists=ActionIfExists.APPEND_OR_REPLACE,
        )

        update_item.write(
            summary="Update population for 2021",
        )
    except MWApiError as e:
        logging.debug(e)
