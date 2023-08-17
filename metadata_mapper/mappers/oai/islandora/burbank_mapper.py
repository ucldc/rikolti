import re

from ..islandora_mapper import IslandoraRecord, IslandoraVernacular


class BurbankRecord(IslandoraRecord):
    date_regex = re.compile(r"(\d+[-/]\d+[-/]\d+)")

    def UCLDC_map(self):
        return {
            "date": self.map_date()
        }

    def map_date(self):
        """
        Exclude date values matching YYYY-MM-DD. Note that the regex copied from the
        legacy system succeeds in this, but casts a wider net than necessary.
        """
        dates = self.collate_fields([
            'available',
            'created',
            'date',
            'dateAccepted',
            'dateCopyrighted',
            'dateSubmitted',
            'issued',
            'modified',
            'valid'
        ])

        return [d for d in dates if self.date_regex.search(d) is None]


class BurbankVernacular(IslandoraVernacular):
    record_cls = BurbankRecord
