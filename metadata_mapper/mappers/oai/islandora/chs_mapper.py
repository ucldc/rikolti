from ..islandora_mapper import IslandoraRecord, IslandoraVernacular


class ChsRecord(IslandoraRecord):
    """Suppress identifier values featuring 'islandora'
    """
    def map_identifier(self):
        if 'identifier' not in self.source_metadata.get('identifier'):
            return

        identifiers = self.source_metadata.get('identifier')

        if isinstance(identifiers, (str, bytes)):
            identifiers = [identifiers]

        return [i for i in identifiers if 'islandora' not in i]


class ChsVernacular(IslandoraVernacular):
    record_cls = ChsRecord
