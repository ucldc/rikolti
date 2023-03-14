from .oai_mapper import OaiRecord, OaiVernacular


class YosemiteRecord(OaiRecord):
    def UCLDC_map(self):
        return {
            'temporal': None,
            'date': self.collate_fields(["available", "created", "dateAccepted", "dateCopyrighted", "dateSubmitted",
                                         "issued", "modified", "valid", "temporal"])
        }

    def map_is_shown_at(self):
        if 'identifier' not in self.source_metadata:
            return

        values = [i for i in filter(None, self.source_metadata.get('identifier'))
                  if 'npgallery.nps.gov' in i
                  and 'AssetDetail' in i]

        if values:
            return values[-1]

    def map_is_shown_by(self):
        if 'identifier' not in self.source_metadata:
            return

        values = [i for i in filter(None, self.source_metadata.get('identifier'))
                  if 'npgallery.nps.gov' in i
                  and i.endswith('/')]

        if values:
            return values[-1]


class YosemiteVernacular(OaiVernacular):
    record_cls = YosemiteRecord
