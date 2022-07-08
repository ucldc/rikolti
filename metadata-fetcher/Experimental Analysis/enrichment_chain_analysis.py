from library_collection.models import Collection


def split_enrichment(c):
    return {
        "enrichment_string": c.enrichments_item,
        "enrichment_list": c.enrichments_item.split(',\r\n'),
        "harvest_type": c.harvest_type,
        "col_id": c.id,
        "collection": c,
    } 


def get_groups(array_of_dicts):
    return [a['group'] for a in array_of_dicts]


def abstract_group_by(items, group_by):
    unique = []
    while(len(items) > 0):
        remainder = []
        matched = []
        group = group_by(items[0])
        for item in items:
            if group_by(item) == group:
                matched.append(item)
            else:
                remainder.append(item)
        if group in unique:
            print('ERROR')
        unique.append({
            "group": group,
            "matched": matched
        })
        items = remainder
    return unique


rich_collections = Collection.objects.exclude(
    enrichments_item__exact='').exclude(harvest_type__exact='X').exclude(
        ready_for_publication__exact=False)
rich_collections = [split_enrichment(c) for c in rich_collections]

unique_enrichments = abstract_group_by(
    rich_collections, 
    lambda c: c["enrichment_string"])

first_items = abstract_group_by(
    rich_collections, 
    lambda c: c['enrichment_list'][0])

second_items = abstract_group_by(
    rich_collections, 
    lambda c: c['enrichment_list'][1])

group_by_harvest_type = abstract_group_by(
    rich_collections,
    lambda c: c['harvest_type'])

for htg in group_by_harvest_type:
    harvest_type = htg['group']
    collections = htg['matched']

    first_items = abstract_group_by(
        collections,
        lambda c: c['enrichment_list'][0])
    htg['first_items'] = first_items

for ght in group_by_harvest_type:
    print(f"{ght['group']} - {get_groups(ght['first_items'])}")











oac_collections = Collection.objects.exclude(
    enrichments_item__exact='').filter(harvest_type__exact='OAC')


oac_collections = first_items['/select-oac-id']
# oac_collections = [item for sublist in oac_collections for item in sublist]
oac_collections_harvest_types = [c.harvest_type for c in oac_collections]
from collections import Counter
Counter(oac_collections_harvest_types)
Counter({'OAC': 173, 'OAI': 3})
for collection in oac_collections:
    if collection.harvest_type != 'OAC':
        print(f"{collection} - {collection.id} - {collection.harvest_type}")

# California menu collection - 3507 - OAI
# Certificates of residence for Chinese laborers, 1894-1897 - 4064 - OAI
# California wine label and ephemera collection - 25602 - OAI
