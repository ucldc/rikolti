from ..contentdm_oai_dc_mapper import ContentdmOaiDcRecord, ContentdmOaiDcVernacular
from ..oai_mapper import collate_values


class ChicoOaiDcRecord(ContentdmOaiDcRecord):
    def UCLDC_map(self):
        """Removing `created`, see: https://github.com/calisphere-legacy-harvester/dpla-ingestion/blob/ucldc/lib/mappers/chico_oai_mapper.py

        """
        return {
            'date': collate_values(
                self.source_metadata_values(
                    'available',
                    'date',
                    'dateAccepted',
                    'dateCopyrighted',
                    'dateSubmitted',
                    'issued',
                    'modified',
                    'valid'
                )
            )
        }


class ChicoOaiDcVernacular(ContentdmOaiDcVernacular):
    record_cls = ChicoOaiDcRecord
