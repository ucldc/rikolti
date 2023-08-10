import re
from xml.etree import ElementTree as ET
from collections import defaultdict
from ..mapper import Vernacular, Record
from ..utils import exists, getprop, iterify


class OacRecord(Record):

    def UCLDC_map(self) -> dict:
        def get_text(field):
            return [n["text"] for n in self.source_metadata.get(field, [])]

        return {
            "id": self.source_metadata.get("id"),
            "isShownAt": self.source_metadata.get("isShownAt"),
            "isShownBy": self.get_best_image(),
            "contributor": get_text("contributor"),
            "creator": get_text("creator"),
            "extent": get_text("extent"),
            "language": get_text("language"),
            "publisher": get_text("publisher"),
            "provenance": get_text("provenance"),
            "description": self.collate_fields(
                ("abstract", "description", "tableOfContents")),
            "identifier": get_text("identifier"),
            "rights": self.collate_fields(("accessRights", "rights")),
            "date": self.map_date,
            "format": self.map_format,
            "title": self.map_title,
            "type": self.map_type,
            "subject": self.map_subject,
            "copyrightDate": self.map_copyright_date,
            "alternativeTitle": self.map_alternative_title,
            "genre": self.map_genre,
            "stateLocatedIn": [{"name": "California"}],
            "spatial": self.map_spatial,
            "temporal": self.map_temporal,
        }

    def map_genre(self):
        def include(n):
            """
            Include nodes that have a `q` attribute with value "genreform"
            """
            return ("q" in n.get("attribs", {}) and
                    "genreform" == n.get("attribs").get("q"))

        return [{"name": n["text"]} for n in self.source_metadata.get("type", [])
                if include(n)]

    def map_copyright_date(self):
        def include(n):
            """
            Include nodes that have a `q` attribute with value "dcterms:dateCopyrighted"
            """
            return ("q" in n.get("attribs", {}) and
                    "dcterms:dateCopyrighted" == n.get("attribs").get("q"))

        return [{"name": n["text"]} for n in self.source_metadata.get("data", [])
                if include(n)]

    def map_alternative_title(self):
        def include(n):
            """
            Include nodes that have a `title` attribute with value "alternative"
            """
            return ("q" in n.get("attribs", {}) and
                    "alternative" == n.get("attribs").get("q"))

        return [{"name": n["text"]} for n in self.source_metadata.get("title", [])
                if include(n)]

    def map_subject(self):
        def include(n):
            """
            Disregard nodes that have a `q` attribute with value "series"
            """
            return ("q" not in n.get("attribs", {}) or
                    "series" != n.get("attribs").get("q"))

        return [{"name": n["text"]} for n in self.source_metadata.get("title", [])
                if include(n)]

    def map_title(self):
        def include(n):
            """
            Disregard nodes that have a `q` attribute with value "alternative"
            """
            return ("q" not in n.get("attribs", {}) or
                    "alternative" != n.get("attribs").get("q"))

        return [n["text"] for n in self.source_metadata.get("title", []) if include(n)]

    def map_type(self):
        def include(n):
            """
            Disregard nodes that have a `q` attribute with value "genreform"
            """
            return ("q" not in n.get("attribs", {}) or
                    "genreform" != n.get("attribs").get("q"))

        return [n["text"] for n in self.source_metadata.get("type", []) if include(n)]

    def map_format(self):
        def include(n):
            """
            Disregard nodes that have a `q` attribute with value "x"
            """
            return "q" not in n.get("attribs", {}) or "x" != n.get("attribs").get("q")

        return [n["text"] for n in self.source_metadata.get("format", []) if include(n)]

    def map_date(self):
        def include(n):
            """
            Disregard nodes that have a `q` attribute with value
            "dcterms:dateCopyrighted", or have a `text` value that ends in "-00-00"
            """
            return (("q" not in n.get("attribs", {}).items() or
                     "dcterms:dateCopyrighted" is not n.get("attribs").get("q"))
                     and not n.get("text").endswith("-00-00"))

        return [n["text"] for n in self.source_metadata.get("date", []) if include(n)]

    def collate_fields(self, original_fields):
        """
        Override to handle elements which are dictionaries of format
        {"attrib": {}, "text": "string value of element"}

        for a list of fields in the providers original data, append the
        values into a single sourceResource field
        """
        values = []
        for field in original_fields:
            if not exists(self.source_metadata, field):
                continue
            values.extend(
                [n["text"] for n in self.source_metadata.get(field, [])])
        return values

    def get_best_image(self):
        """
        From the list of images, choose the largest one
        """
        if "thumbnail" not in self.source_metadata:
            return None

        dim = 0
        best_image = None

        # "thumbnail" might be represented different in xmltodict
        # vs. the custom fetching mark was doing
        thumbnail = self.source_metadata.get("thumbnail")
        if thumbnail and "src" in thumbnail:
            dim = max(int(thumbnail.get("X")), int(thumbnail.get("Y")))
            best_image = thumbnail.get("src")

        # "reference-image" might be represented differently in xmltodict
        # vs. the custom fetching mark was doing
        reference_image = self.source_metadata.get("reference-image", [])
        if isinstance(reference_image, dict):
            reference_image = [reference_image]
        for obj in reference_image:
            if max(int(obj.get("X")), int(obj.get("Y"))) > dim:
                dim = max(int(obj.get("X")), int(obj.get("Y")))
                best_image = obj.get("src")

        if best_image and not best_image.startswith("http"):
            best_image = f"https://content.cdlib.org/{best_image}"

        return best_image

    def map_spatial(self):
        coverage = []
        if "coverage" not in self.source_metadata:
            return coverage

        coverage_data = iterify(self.source_metadata.get("coverage"))

        anum_re = re.compile("A\d\d\d\d")
        for c in coverage_data:
            # Remove arks from data
            if (not isinstance(c, str) and
                    not c.get("text").startswith("ark:")):
                if ("q" in c.get("attrib", {}) and
                        "temporal" not in c.get(
                            "attrib", {}).get("q")):
                    coverage.append(c.get("text"))
                # collection 25496 has coverage values like
                # A0800 & A1000 - drop these
                if ("q" not in c.get("attrib", {}) and
                        c.get("attrib", {}) is not None and
                        not anum_re.match(c.get("text"))):
                    coverage.append(c.get("text"))
        return coverage

    def map_temporal(self):
        temporal = []
        if "coverage" not in self.source_metadata:  # guard weird input
            return temporal

        time_data = iterify(self.source_metadata.get("coverage"))
        for t in time_data:
            if ("q" in t.get("attrib", {})
                    and "temporal" in t.get("attrib", {}).get("q")):
                temporal.append(t.get("text"))
        return temporal


class OacVernacular(Vernacular):
    record_cls = OacRecord

    def parse(self, api_response):
        cross_query_result = ET.fromstring(api_response)
        doc_hits = cross_query_result.findall("./docHit")

        records = []
        for document in doc_hits:
            obj = defaultdict(list)
            meta = document.find("meta")
            for tag in meta:
                # A few tags have special processing here to ensure the meaningful
                # data is extracted from XML properly.
                if tag.tag == "google_analytics_tracking_code":
                    continue
                if tag.tag == "reference-image":
                    obj[tag.tag].append(self.parse_reference_image(tag))
                elif tag.tag == "thumbnail":
                    obj[tag.tag] = self.parse_thumbnail(tag, document)
                elif tag.tag == "relation" and len(list(tag)) > 0:
                    relation = self.parse_relation(tag)
                    if not relation:
                        continue
                    obj[tag.tag].append(relation)
                else:
                    if not tag.text:
                        continue
                    obj[tag.tag].append({
                        "attrib": tag.attrib,
                        "text": tag.text
                    })
            records.append(obj)

        return self.get_records(records)

    def _get_doc_ark(self, doc_hit):
        """
        Return the object's ark from the xml etree docHit
        """
        ids = doc_hit.find("meta").findall("identifier")
        ark = None
        for i in ids:
            if i.attrib.get("q", None) == "local":
                continue
            try:
                split = i.text.split("ark:")
            except AttributeError:
                continue
            if len(split) > 1:
                ark = "".join(("ark:", split[1]))
        return ark

    def parse_reference_image(self, tag):
        try:
            x = int(tag.attrib["X"])
        except ValueError:
            x = 0
        try:
            y = int(tag.attrib["Y"])
        except ValueError:
            y = 0
        src = f"http://content.cdlib.org/{tag.attrib['src']}"
        src = src.replace("//", "/").replace("/", "//", 1)
        data = {
            "X": x,
            "Y": y,
            "src": src,
        }
        return data

    def parse_thumbnail(self, tag, document):
        ark = self._get_doc_ark(document)
        try:
            x = int(tag.attrib["X"])
        except ValueError:
            x = 0
        try:
            y = int(tag.attrib["Y"])
        except ValueError:
            y = 0
        src = f"http://content.cdlib.org/{ark}/thumbnail"
        src = src.replace("//", "/").replace("/", "//", 1)
        data = {
            "X": x,
            "Y": y,
            "src": src,
        }
        return data

    def parse_relation(self, tag):
        """
        Here's what the relation tag looks like, prettified:

            <relation>
               <snippet rank="1" score="100">
                   http://oac.cdlib.org/findaid/
                   <hit>
                       <term>ark</term>:/<term>12345/kjj49413z</term>
                    </hit>
               </snippet>
            </relation>
        """
        data = ""
        for innertext in tag.itertext():
            data = f"{data}{innertext.strip()}"
        if not data:
            return None
        return {
            "attrib": tag.attrib,
            "text": data
        }
