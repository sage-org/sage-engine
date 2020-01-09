# utils.py
# Author: Thomas MINIER - MIT License 2017-2020

from typing import Dict, List, Tuple

GENERAL_PREDICATES: List[Tuple[int, str]] = [
    # RDF type
    (1, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
    # RDFS namespace
    (2, "http://www.w3.org/2000/01/rdf-schema#range"),
    (3, "http://www.w3.org/2000/01/rdf-schema#domain"),
    (4, "http://www.w3.org/2000/01/rdf-schema#subClassOf"),
    (5, "http://www.w3.org/2000/01/rdf-schema#subPropertyOf"),
    (6, "http://www.w3.org/2000/01/rdf-schema#label"),
    (7, "http://www.w3.org/2000/01/rdf-schema#comment"),
    (8, "http://www.w3.org/2000/01/rdf-schema#seeAlso"),
    # Reification voc.
    (9, "http://www.w3.org/1999/02/22-rdf-syntax-ns#subject"),
    (10, "http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate"),
    (11, "http://www.w3.org/1999/02/22-rdf-syntax-ns#object"),
    # DC Terms namespace
    (12, "http://purl.org/dc/terms/contributor"),
    (13, "http://purl.org/dc/terms/created"),
    (14, "http://purl.org/dc/terms/creator"),
    (15, "http://purl.org/dc/terms/date"),
    (16, "http://purl.org/dc/terms/description"),
    (17, "http://purl.org/dc/terms/format"),
    (18, "http://purl.org/dc/terms/identifier"),
    (19, "http://purl.org/dc/terms/language"),
    (20, "http://purl.org/dc/terms/license"),
    (21, "http://purl.org/dc/terms/publisher"),
    (22, "http://purl.org/dc/terms/source"),
    (23, "http://purl.org/dc/terms/title"),
    (24, "http://purl.org/dc/elements/1.1/contributor"),
    (25, "http://purl.org/dc/elements/1.1/created"),
    (26, "http://purl.org/dc/elements/1.1/creator"),
    (27, "http://purl.org/dc/elements/1.1/date"),
    (28, "http://purl.org/dc/elements/1.1/description"),
    (29, "http://purl.org/dc/elements/1.1/format"),
    (30, "http://purl.org/dc/elements/1.1/identifier"),
    (31, "http://purl.org/dc/elements/1.1/language"),
    (32, "http://purl.org/dc/elements/1.1/license"),
    (33, "http://purl.org/dc/elements/1.1/publisher"),
    (34, "http://purl.org/dc/elements/1.1/source"),
    (35, "http://purl.org/dc/elements/1.1/title"),
    (35, "http://purl.org/dc/terms/Location"),
    # FOAF namespace
    (36, "http://xmlns.com/foaf/name"),
    (37, "http://xmlns.com/foaf/givenName"),
    (38, "http://xmlns.com/foaf/age"),
    (39, "http://xmlns.com/foaf/homepage"),
    # Schema.org namespace
    (40, "http://schema.org/eligibleRegion"),
    (41, "http://schema.org/email"),
    (42, "http://schema.org/eligibleQuantity"),
    (43, "https://schema.org/creator"),
    (44, "https://schema.org/reviewBody"),
    (45, "http://schema.org/review"),
    (46, "http://schema.org/birthDate"),
    (47, "http://schema.org/nationality"),
    (48, "http://schema.org/priceValidUntil"),
    (49, "http://schema.org/actor"),
    (50, "http://schema.org/description"),
    (51, "http://schema.org/contentRating"),
    (52, "http://schema.org/text"),
    (53, "http://schema.org/keywords"),
    (54, "http://schema.org/language"),
    (55, "http://schema.org/telephone"),
    (56, "http://schema.org/jobTitle"),
    (57, "http://schema.org/author"),
    (58, "http://schema.org/caption"),
    (59, "http://schema.org/contentSize"),
    (60, "http://schema.org/isbn"),
    (61, "http://schema.org/editor"),
    (62, "http://schema.org/publisher"),
    (63, "http://schema.org/director"),
    (64, "http://schema.org/employee"),
    (65, "http://schema.org/expires"),
    (66, "http://schema.org/datePublished"),
    (67, "http://schema.org/openingHours"),
    (68, "http://schema.org/contactPoint"),
    (69, "http://schema.org/bookEdition"),
    (70, "http://schema.org/producer"),
    (71, "http://schema.org/paymentAccepted"),
    (72, "http://schema.org/award"),
    (73, "http://schema.org/aggregateRating"),
    (74, "http://schema.org/numberOfPages"),
    (75, "http://schema.org/wordCount"),
    (76, "http://schema.org/printColumn"),
    (77, "http://schema.org/printEdition"),
    (78, "http://schema.org/printPage"),
    (79, "http://schema.org/printSection"),
    (80, "http://schema.org/trailer"),
    (81, "http://schema.org/faxNumber"),
    (82, "http://schema.org/legalName"),
    (83, "http://schema.org/price"),
    (84, "http://schema.org/serialNumber"),
    (85, "http://schema.org/eligibleDuration"),
    (86, "http://schema.org/maxValue"),
    (87, "http://schema.org/minValue")
]

PREDICATES_TO_IDS: Dict[str, str] = dict()
IDS_TO_PREDICATES: Dict[str, str] = dict()

# build compressed general predicates
for idx, predicate in GENERAL_PREDICATES:
    idc = f"_P{idx}"
    PREDICATES_TO_IDS[predicate] = idc
    IDS_TO_PREDICATES[idc] = predicate


def predicate_to_id(predicate: str) -> str:
    """Try to convert a predicate to an unique identifier.
    
    This method will only compress general predicates (rdf:type, rdfs:label, etc), as they are shared in almost every RDF dataset. Otherwise, it will simply returns the predicate value.

    Argument: A RDF term used as a predicate in a RDF triple.

    Returns: An unique ID that encode the input RDF term.
    """
    if predicate in PREDICATES_TO_IDS:
        return PREDICATES_TO_IDS[predicate]
    return predicate


def id_to_predicate(idc: str) -> str:
    """Try to convert an unique identifier into a predicate. 
    
    If it is already a decompressed predicate, do nothing.

    Argument: An unique ID that encode a RDF term as a predicate in a RDF triple.

    Returns: A RDF term that corresponds to the input ID.
    """
    if idc in IDS_TO_PREDICATES:
        return IDS_TO_PREDICATES[idc]
    return idc
