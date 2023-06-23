from .contentdm_mapper import ContentdmRecord, ContentdmVernacular


class ChicoRecord(ContentdmRecord):
    def UCLDC_map(self):
        """Removing `created`, see:
        https://github.com/calisphere-legacy-harvester/dpla-ingestion/blob/ucldc/lib/mappers/chico_oai_mapper.py
        """
        return {
            'date': self.collate_fields(
                [
                    'available',
                    'date',
                    'dateAccepted',
                    'dateCopyrighted',
                    'dateSubmitted',
                    'issued',
                    'modified',
                    'valid'
                ]
            )
        }


class ChicoVernacular(ContentdmVernacular):
    record_cls = ChicoRecord
