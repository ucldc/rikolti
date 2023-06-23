from .contentdm_mapper import ContentdmRecord, ContentdmVernacular


class CsuSacRecord(ContentdmRecord):
    def UCLDC_map(self):
        return {
            'date': self.collate_fields([
                'created',
                'date'
            ]),
        }


class CsuSacVernacular(ContentdmVernacular):
    record_cls = CsuSacRecord
