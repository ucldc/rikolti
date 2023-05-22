from ..csu_dspace_mapper import CsuDspaceRecord, CsuDspaceVernacular


class CsuciRecord(CsuDspaceRecord):
    def UCLDC_map(self):
        return {
            "type": self.map_type
        }

    def map_type(self):
        values = [v for v in self.source_metadata.get("formatName", [])
                  if "text/plain" not in v]
        if not values:
            return

        return values[-1]


class CsuciVernacular(CsuDspaceVernacular):
    record_cls = CsuciRecord
