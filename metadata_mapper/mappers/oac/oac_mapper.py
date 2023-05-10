import re
from xml.etree import ElementTree as ET
from collections import defaultdict
from ..mapper import Vernacular, Record
from ..utils import exists, getprop, iterify


class OacRecord(Record):

    def UCLDC_map(self) -> dict:
        return {
            "id": self.source_metadata.get("id", ""),
            "isShownAt": self.pre_mapped_data.get("isShownAt", None),
            "isShownBy": self.get_best_image(),
            "contributor": self.get_vals("contributor"),
            "creator": self.get_vals("creator"),
            "extent": self.get_vals("extent"),
            "language": self.get_vals("language"),
            "publisher": self.get_vals("publisher"),
            "provenance": self.get_vals("provenance"),
            "description": self.collate_fields(
                ("abstract", "description", "tableOfContents")),
            "identifier": self.get_vals("identifier"),
            "rights": self.collate_fields(("accessRights", "rights")),
            "date": self.get_vals(
                "date", suppress_attribs={"q": "dcterms:dateCopyrighted"}),
            "format": self.get_vals("format", suppress_attribs={"q": "x"}),
            "title": self.get_vals(
                "title", suppress_attribs={"q": "alternative"}),
            "type": self.get_vals(
                "type", suppress_attribs={"q": "genreform"}),
            "subject": self.map_subject(),
            "copyrightDate": self.map_specific("date", "dcterms:dateCopyrighted"),
            "alternativeTitle": self.map_specific("title", "alternative"),
            "genre": self.map_specific("type", "genreform"),
            "stateLocatedIn": [{"name": "California"}],
            "spatial": self.map_spatial(),
            "temporal": self.map_temporal(),
        }

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
                self.get_vals(field))
        return values

    def get_vals(self, provider_prop, suppress_attribs={}):
        """
        Return a list of string values take from the OAC type
        original record (each value is {"text": <val>, "attrib": <val>} object)
        """
        values = []
        if not exists(self.source_metadata, provider_prop):
            return values

        for x in self.source_metadata[provider_prop]:
            try:
                value = x["text"]
            except KeyError:
                # not an elementtree type data value
                values.append(x)
                continue
            if not x["attrib"]:
                values.append(value)
            else:
                suppress = False
                for attrib, attval in x["attrib"].items():
                    if attval in suppress_attribs.get(attrib, []):
                        suppress = True
                        break
                if suppress:
                    continue
                if value.endswith("-00-00"):
                    continue

                values.append(value)
        return values

    def map_specific(self, src_prop, specify):
        provider_data = self.source_metadata.get(src_prop, None)
        if not provider_data:
            return []

        return [
            d.get("text") for d in provider_data
            if d.get("attrib") if d.get("attrib", {}).get("q") == specify
        ]

    def get_best_image(self):
        """
        From the list of images, choose the largest one
        """
        best_image = None
        if "thumbnail" not in self.source_metadata:
            return best_image

        dim = 0
        # "thumbnail" might be represented different in xmltodict
        # vs. the custom fetching mark was doing
        thumbnail = self.source_metadata.get("thumbnail", None)
        if thumbnail:
            if "src" in thumbnail:
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
            best_image = f"http://content.cdlib.org/{best_image}"
        return best_image

    def map_item_count(self):
        """
        Use reference-image-count value to determine compound objects.
        NOTE: value is not always accurate so only determines complex (-1)
        or not complex (no item_count value)
        """
        item_count = None
        image_count = 0
        if "reference-image-count" not in self.source_metadata:  # guard weird input
            return item_count

        ref_image_count = self.source_metadata.get("reference-image-count")
        if ref_image_count:
            image_count = ref_image_count[0]["text"]
        if image_count > "1":
            item_count = "-1"
        return item_count

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

    def map_subject(self):
        subject_values = self.get_vals(
            "subject", suppress_attribs={"q": "series"})
        subject_objs = [{"name": s} for s in subject_values]
        return subject_objs

    def to_dict(self):
        self.pre_mapped_data.update(self.mapped_data)
        return self.pre_mapped_data


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
