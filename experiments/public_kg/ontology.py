"""Source-independent target vocabulary; these definitions are not gold labels."""

import argparse
import hashlib
import json
from pathlib import Path

from rdflib import Graph, Literal, Namespace, RDF, RDFS, URIRef, OWL


BASE = "https://w3id.org/rdb2g/public-spatial/"
CLASSES = {
    "Place": "Named geographic feature or place, including populated places.",
    "AdministrativeUnit": "Territorial administrative unit; level is a separate property.",
    "Road": "Named street or road feature.",
    "AddressPoint": "Address location associated with a house number or address text.",
    "AddressUnit": "Unit or room within an addressed building.",
    "PointOfInterest": "Named point or area of interest, including businesses and amenities.",
    "Facility": "Public or service facility.",
    "HealthFacility": "Healthcare institution or facility.",
    "FeatureType": "Classification concept for a geographic feature, not a physical place.",
}
PROPERTIES = {
    "identifier": ("Stable source identifier or business code.", "Literal"),
    "name": ("Primary display name; do not infer its language without evidence.", "Literal"),
    "asciiName": ("ASCII or transliterated representation of a name.", "Literal"),
    "alternateName": ("Alternative names or aliases; may contain several languages.", "Literal"),
    "description": ("Description or explanatory notes.", "Literal"),
    "addressText": ("Full or partial textual address.", "Literal"),
    "houseNumber": ("House number within a street address.", "Literal"),
    "unitNumber": ("Unit, apartment or room number.", "Literal"),
    "longitude": ("Longitude in decimal degrees.", "Literal"),
    "latitude": ("Latitude in decimal degrees.", "Literal"),
    "geometry": ("Geometry representation, where present.", "Literal"),
    "length": ("Geometric length; units must come from source documentation.", "Literal"),
    "area": ("Geometric area; units must come from source documentation.", "Literal"),
    "categoryCode": ("Classification code of a place or feature.", "Literal"),
    "categoryName": ("Classification name or category label.", "Literal"),
    "administrativeCode": ("Administrative code, possibly local to its parent territory.", "Literal"),
    "administrativeLevel": ("Administrative hierarchy level.", "Literal"),
    "countryCode": ("Country code identifying a country.", "Literal"),
    "alternateCountryCode": ("Alternative country codes associated with a feature, possibly comma-separated; not necessarily its primary country.", "Literal"),
    "featureClassCode": ("Broad one-character feature class code, distinct from a detailed feature type code.", "Literal"),
    "parentIdentifier": ("Source identifier referring to a parent entity; not yet a resolved edge.", "Literal"),
    "parentName": ("Textual name of a parent entity.", "Literal"),
    "population": ("Population count.", "Literal"),
    "elevation": ("Elevation above sea level.", "Literal"),
    "terrainElevation": ("Elevation derived from a digital elevation model.", "Literal"),
    "timezone": ("Time zone identifier.", "Literal"),
    "telephone": ("Contact telephone number.", "Literal"),
    "status": ("Operational or lifecycle status.", "Literal"),
    "createdAt": ("Creation or initial collection date/time.", "Literal"),
    "modifiedAt": ("Last modification date/time.", "Literal"),
    "validFrom": ("Activation or validity start date/time.", "Literal"),
    "validUntil": ("Retirement or validity end date/time.", "Literal"),
    "locatedIn": ("Geographic entity is assigned to an administrative unit.", "AdministrativeUnit"),
    "parentAdministrativeUnit": ("Administrative unit has this immediate administrative parent.", "AdministrativeUnit"),
    "hasFeatureType": ("Geographic feature has this classification concept.", "FeatureType"),
    "sameSourceEntity": ("Two source records represent the same upstream real-world feature.", "Place"),
    "locatedOnRoad": ("Address location is explicitly associated with a road.", "Road"),
    "partOfAddress": ("Address unit is part of an enclosing addressed entity.", "Place"),
}


def write_ontology(output_dir):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    terms = []
    graph = Graph()
    ns = Namespace(BASE)
    graph.bind("spatial", ns)
    for name, comment in CLASSES.items():
        terms.append({"uri": BASE + name, "type": "Class", "label": name,
                      "comment": comment, "domain": "", "range": ""})
        graph.add((ns[name], RDF.type, RDFS.Class))
        graph.add((ns[name], RDFS.label, Literal(name, lang="en")))
        graph.add((ns[name], RDFS.comment, Literal(comment, lang="en")))
        if name not in {"Place", "FeatureType"}:
            graph.add((ns[name], RDFS.subClassOf, ns.Place))
    for name, (comment, range_name) in PROPERTIES.items():
        property_kind = "DatatypeProperty" if range_name == "Literal" else "ObjectProperty"
        terms.append({"uri": BASE + name, "type": "Property", "property_kind": property_kind, "label": name,
                      "comment": comment, "domain": "", "range": range_name})
        graph.add((ns[name], RDF.type, RDF.Property))
        graph.add((ns[name], RDF.type, OWL[property_kind]))
        graph.add((ns[name], RDFS.label, Literal(name, lang="en")))
        graph.add((ns[name], RDFS.comment, Literal(comment, lang="en")))
        graph.add((ns[name], RDFS.range, RDFS.Literal if range_name == "Literal" else ns[range_name]))
    payload = {"status": "machine_reviewed_reference", "version": "public-spatial-v2-machine-reviewed",
               "human_reviewed": False,
               "review_scope": "Source-neutral concept definitions and explicit OWL property kinds; no source-column bindings.",
               "note": "Machine-reviewed working vocabulary; not independently human-annotated gold.",
               "terms": terms}
    content = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    path = output_dir / "target_terms.json"
    ttl_path = output_dir / "spatial_v2.ttl"
    if path.exists() or ttl_path.exists():
        raise FileExistsError("Refusing to replace an existing ontology version")
    path.write_text(content, encoding="utf-8")
    graph.serialize(ttl_path, format="turtle")
    return {"path": str(path), "terms": len(terms), "status": payload["status"],
            "sha256": hashlib.sha256(content.encode()).hexdigest()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="data/public/benchmark_v2/ontology")
    print(json.dumps(write_ontology(parser.parse_args().out), indent=2))
