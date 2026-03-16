from ..islandora_mapper import IslandoraRecord, IslandoraVernacular

class ArchipelagoRecord(IslandoraRecord):

    def UCLDC_map(self):
        return {
            'isShownAt': self.source_metadata.get('identifier.url')[0],
            'isShownBy': self.source_metadata.get('identifier.thumbnail')[0]
        }

class ArchipelagoVernacular(IslandoraVernacular):
    record_cls = ArchipelagoRecord
