>>> import json
>>> f = open('table_list.json')
>>> f = open('table_list_2.json')
>>> data = json.load(f)
>>> data = data['TableList']
>>> data = [{'name': i['Name'], 'schema': i['StorageDescriptor']['Columns']} for i in data]
>>> data = [d for d in data if d['name'][-9:] == 'oac_datel']
>>> data = [d for d in data if d['name'][-7:] == 'oac_old']
>>> len(data)
581
582

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

>>> field_names = [{'name': i['name'], 'field_names': [field['Name'] for field in i['schema']]} for i in data]
>>> unique_schema_column_sets = abstract_group_by(field_names, lambda c: set(c['field_names']))
>>> field_names_only = [i['group'] for i in unique_schema_column_sets]
>>> uber_list = set.union(*field_names_only)
{'facet-collection-title', 'iso-start-date', 'facet-oac-tab', 'hit', 'facet-decade', 'relation-from', 'rights', 'coverage', 'facet-jardabrowse', 'sort-title', 'description', 'facet-collection-order', 'institution-url', 'title', 'facet-ethbrowse', 'facet-contributor', 'reference-image-count', 'azbrowse', 'facet-subject', 'ethbrowse', 'source', 'google_analytics_tracking_code', 'jardabrowse', 'sort-year', 'snippet', 'oac4-tab', 'year', 'facet-onlineitems', 'term', 'facet-description', 'facet-rights', 'keywords', 'datestamp', 'relation', 'facet-creator', 'facet-title', 'facet-identifier', 'thumbnail', 'language', 'date', 'reference-image', 'creator', 'facet-type', 'publisher', 'facet-institution', 'institution-doublelist', 'sort-publisher', 'format', 'orphan', 'dochit', 'contributor', 'meta', 'iso-end-date', 'facet-format', 'facet-relation', 'type', 'identifier', 'facet-coverage', 'sort-creator', 'subject', 'facet-publisher', 'facet-language', 'facet-type-tab', 'facet-azbrowse'}
{'google_analytics_tracking_code', 'facet-oac-tab', 'facet-rights', 'thumbnail', 'contributor', 'publisher', 'facet-description', 'datestamp', 'jardabrowse', 'facet-onlineitems', 'keywords', 'facet-publisher', 'iso-start-date', 'year', 'coverage', 'facet-decade', 'subject', 'institution-doublelist', 'reference-image', 'azbrowse', 'sort-title', 'iso-end-date', 'title', 'relation-from', 'facet-jardabrowse', 'orphan', 'facet-language', 'facet-collection-order', 'facet-subject', 'facet-title', 'facet-type-tab', 'ethbrowse', 'facet-relation', 'relation', 'facet-ethbrowse', 'calisphere-id', 'identifier', 'institution-url', 'facet-identifier', 'reference-image-count', 'source', 'language', 'facet-creator', 'sort-year', 'facet-format', 'date', 'sort-publisher', 'facet-coverage', 'oac4-tab', 'facet-type', 'facet-azbrowse', 'type', 'rights', 'format', 'facet-contributor', 'description', 'creator', 'sort-creator', 'facet-institution', 'facet-collection-title'}
>>> core_list = set.intersection(*field_names_only)
>>> core_list
{'relation', 'title', 'term', 'type', 'facet-oac-tab', 'dochit', 'identifier', 'hit', 'meta', 'institution-doublelist', 'snippet', 'oac4-tab', 'sort-title', 'facet-type-tab', 'datestamp', 'facet-collection-order'}
{'title', 'relation', 'institution-doublelist', 'facet-oac-tab', 'calisphere-id', 'identifier', 'type', 'facet-collection-order', 'sort-title', 'oac4-tab', 'datestamp', 'facet-type-tab'}
>>> for field in uber_list:
...     print(field)
...     uniqueness = abstract_group_by(data, lambda c: [f['Type'] for f in c['schema'] if f['Name'] == field])
...     print([i['group'] for i in uniqueness])
... 
facet-collection-title          | [['array<struct<text():string>>'], []]
iso-start-date                  | [['array<struct<text():string>>'], []]
facet-oac-tab                   | [['array<struct<text():string>>']]
hit                             | [['array<struct<mix():string>>']]
facet-decade                    | [['array<struct<text():string>>'], []]
relation-from                   | [['array<struct<q:string,text():string>>'], []]
rights                          | [['array<struct<text():string>>'], [], ['array<struct<text():string,q:string>>']]
coverage                        | [['array<struct<q:string,text():string>>'], [], ['array<struct<text():string>>'], ['array<struct<text():string,q:string>>']]
facet-jardabrowse               | [[], ['array<struct<text():string>>']]
sort-title                      | [['array<struct<text():string>>']]
description                     | [['array<struct<text():string>>'], ['array<struct<text():string,q:string>>'], [], ['array<struct<q:string,text():string>>']]
facet-collection-order          | [['array<struct<text():string>>']]
institution-url                 | [['array<struct<text():string>>'], []]
title                           | [['array<struct<text():string>>'], ['array<struct<text():string,q:string>>']]
facet-ethbrowse                 | [['array<struct<text():string>>'], []]
facet-contributor               | [[], ['array<struct<text():string>>']]
reference-image-count           | [['array<struct<text():string>>'], []]
azbrowse                        | [['array<struct<text():string>>'], []]
facet-subject                   | [['array<struct<text():string>>'], []]
ethbrowse                       | [['array<struct<text():string>>'], []]
source                          | [[], ['array<struct<text():string>>']]
google_analytics_tracking_code  | [['array<struct<text():string>>'], []]
jardabrowse                     | [[], ['array<struct<text():string>>']]
sort-year                       | [['array<struct<text():string>>'], []]
snippet                         | [['array<struct<rank:string,score:string,mix():string>>']]
oac4-tab                        | [['array<struct<text():string>>']]
year                            | [['array<struct<text():string>>'], []]
facet-onlineitems               | [['array<struct<text():string>>'], []]
term                            | [['array<struct<text():string>>']]
facet-description               | [[], ['array<struct<text():string>>']]
facet-rights                    | [[], ['array<struct<text():string>>']]
keywords                        | [[], ['array<struct<text():string>>']]
datestamp                       | [['array<struct<text():string>>']]
relation                        | [['array<struct<mix():string,text():string>>'], ['array<struct<text():string,q:string,mix():string>>'], ['array<struct<q:string,mix():string,text():string>>'], ['array<struct<mix():string,q:string,text():string>>'], ['array<struct<mix():string,text():string,q:string>>'], ['array<struct<q:string,text():string,mix():string>>'], ['array<struct<text():string,mix():string>>'], ['array<struct<text():string>>'], ['array<struct<text():string,mix():string,q:string>>']]
facet-creator                   | [[], ['array<struct<text():string>>']]
facet-title                     | [[], ['array<struct<text():string>>']]
facet-identifier                | [[], ['array<struct<text():string>>']]
thumbnail                       | [['array<struct<X:string,Y:string,text():string>>'], []]
language                        | [['array<struct<text():string>>'], []]
date                            | [['array<struct<q:string,text():string>>'], ['array<struct<text():string>>'], [], ['array<struct<text():string,q:string>>']]
reference-image                 | [['array<struct<X:string,Y:string,src:string,text():string>>'], []]
creator                         | [[], ['array<struct<text():string>>']]
facet-type                      | [[], ['array<struct<text():string>>']]
publisher                       | [['array<struct<text():string>>'], ['array<struct<mix():string>>'], ['array<struct<mix():string,text():string>>'], []]
facet-institution               | [['array<struct<text():string>>'], []]
institution-doublelist          | [['array<struct<text():string>>']]
sort-publisher                  | [[], ['array<struct<text():string>>']]
format                          | [['array<struct<q:string,text():string>>'], ['array<struct<text():string,q:string>>'], ['array<struct<text():string>>'], []]
orphan                          | [[], ['array<struct<text():string>>']]
dochit                          | [['array<struct<rank:string,path:string,score:string,totalHits:string,mix():string>>']]
contributor                     | [[], ['array<struct<text():string>>']]
meta                            | [['array<struct<mix():string>>']]
iso-end-date                    | [['array<struct<text():string>>'], []]
facet-format                    | [[], ['array<struct<text():string>>']]
facet-relation                  | [[], ['array<struct<text():string>>']]
type                            | [['array<struct<text():string>>'], ['array<struct<text():string,q:string>>'], ['array<struct<q:string,text():string>>'], ['array<struct<mix():string>>'], ['array<struct<q:string,mix():string,text():string>>']]
identifier                      | [['array<struct<text():string,q:string>>'], ['array<struct<text():string>>'], ['array<struct<text():string,q:string,mix():string>>']]
facet-coverage                  | [['array<struct<text():string>>'], []]
sort-creator                    | [[], ['array<struct<text():string>>']]
subject                         | [['array<struct<text():string,q:string>>'], ['array<struct<q:string,text():string>>'], [], ['array<struct<text():string,q:string,mix():string>>']]
facet-publisher                 | [[], ['array<struct<text():string>>']]
facet-language                  | [[], ['array<struct<text():string>>']]
facet-type-tab                  | [['array<struct<text():string>>']]
facet-azbrowse                  | [['array<struct<text():string>>'], []]

google_analytics_tracking_code  | [['struct<#text:string>'], []]
facet-oac-tab                   | [['struct<#text:string>']]
facet-rights                    | [[], ['struct<#text:string>'], ['array<struct<#text:string>>']]
thumbnail                       | [['struct<@X:string,@Y:string>'], []]
contributor                     | [[], ['struct<#text:string>'], ['array<struct<#text:string>>'], ['string']]
publisher                       | [['array<struct<#text:string>>'], ['struct<#text:string>'], ['string'], ['struct<snippet:struct<@rank:string,@score:string,#text:string,hit:struct<term:struct<#text:string>>>>'], ['struct<snippet:struct<@rank:string,@score:string,#text:string,hit:struct<term:array<struct<#text:string>>>>>'], ['struct<snippet:struct<@rank:string,@score:string,hit:struct<term:array<struct<#text:string>>>>>'], [], ['struct<#text:string,snippet:struct<@rank:string,@score:string,#text:string,hit:struct<term:array<struct<#text:string>>>>>']]
facet-description               | [[], ['array<struct<#text:string>>'], ['struct<#text:string>']]
datestamp                       | [['struct<#text:string>']]
jardabrowse                     | [[], ['array<struct<#text:string>>'], ['struct<#text:string>']]
facet-onlineitems               | [['struct<#text:string>'], []]
keywords                        | [[], ['struct<#text:string>']]
facet-publisher                 | [[], ['array<struct<#text:string>>'], ['struct<#text:string>']]
iso-start-date                  | [['struct<#text:string>'], [], ['array<struct<#text:string>>']]
year                            | [['struct<#text:string>'], [], ['array<struct<#text:string>>']]
coverage                        | [['struct<@q:string,#text:string>'], [], ['struct<#text:string>'], ['struct<#text:string,@q:string>'], ['array<struct<#text:string>>'], ['array<struct<@q:string,#text:string>>']]
facet-decade                    | [['struct<#text:string>'], [], ['array<struct<#text:string>>']]
subject                         | [['array<struct<#text:string,@q:string>>'], ['array<struct<@q:string,#text:string>>'], ['struct<@q:string,#text:string>'], ['struct<#text:string,@q:string>'], [], ['array<struct<#text:string,@q:string,snippet:struct<@rank:string,@score:string,hit:struct<term:array<struct<#text:string>>>>>>']]
institution-doublelist          | [['struct<#text:string>'], ['array<struct<#text:string>>']]
reference-image                 | [['struct<@X:string,@Y:string,@src:string>'], ['array<struct<@X:string,@Y:string,@src:string>>'], []]
azbrowse                        | [['struct<#text:string>'], [], ['array<struct<#text:string>>']]
sort-title                      | [['struct<#text:string>']]
iso-end-date                    | [['struct<#text:string>'], [], ['array<struct<#text:string>>']]
title                           | [['struct<#text:string>'], ['array<struct<#text:string,@q:string>>']]
relation-from                   | [['struct<@q:string,#text:string>'], []]
facet-jardabrowse               | [[], ['array<struct<#text:string>>'], ['struct<#text:string>']]
orphan                          | [[], ['struct<#text:string>']]
facet-language                  | [[], ['struct<#text:string>']]
facet-collection-order          | [['struct<#text:string>']]
facet-subject                   | [['array<struct<#text:string>>'], ['struct<#text:string>'], []]
facet-title                     | [[], ['struct<#text:string>']]
facet-type-tab                  | [['struct<#text:string>'], ['array<struct<#text:string>>']]
ethbrowse                       | [['struct<#text:string>'], [], ['array<struct<#text:string>>']]
facet-relation                  | [[], ['array<struct<#text:string>>']]
relation                        | [['array<struct<snippet:struct<@rank:string,@score:string,#text:string,hit:struct<term:array<struct<#text:string>>>>,#text:string>>'], ['array<struct<#text:string,@q:string,snippet:struct<@rank:string,@score:string,#text:string,hit:struct<term:array<struct<#text:string>>>>>>'], ['array<struct<@q:string,snippet:struct<@rank:string,@score:string,#text:string,hit:struct<term:array<struct<#text:string>>>>,#text:string>>'], ['array<struct<snippet:struct<@rank:string,@score:string,#text:string,hit:struct<term:array<struct<#text:string>>>>,@q:string,#text:string>>'], ['array<struct<snippet:struct<@rank:string,@score:string,#text:string,hit:struct<term:array<struct<#text:string>>>>,#text:string,@q:string>>'], ['array<struct<snippet:struct<@rank:string,@score:string,#text:string,hit:struct<term:struct<#text:string>>>,@q:string,#text:string>>'], ['array<struct<@q:string,snippet:struct<@rank:string,@score:string,hit:struct<term:array<struct<#text:string>>>,#text:string>,#text:string>>'], ['array<struct<@q:string,#text:string,snippet:struct<@rank:string,@score:string,#text:string,hit:struct<term:array<struct<#text:string>>>>>>'], ['array<struct<#text:string,@q:string,snippet:struct<@rank:string,@score:string,hit:struct<term:array<struct<#text:string>>>>>>'], ['array<struct<#text:string,snippet:struct<@rank:string,@score:string,#text:string,hit:struct<term:array<struct<#text:string>>>>>>'], ['array<struct<#text:string,snippet:struct<@rank:string,@score:string,#text:string,hit:struct<term:array<struct<#text:string>>>>,@q:string>>'], ['array<struct<snippet:struct<@rank:string,@score:string,hit:struct<term:array<struct<#text:string>>>>,#text:string,@q:string>>'], ['array<struct<snippet:struct<@rank:string,@score:string,hit:struct<term:struct<#text:string>>,#text:string>,#text:string>>'], ['array<struct<snippet:struct<@rank:string,@score:string,hit:struct<term:array<struct<#text:string>>>>,#text:string>>'], ['array<struct<#text:string,snippet:struct<@rank:string,@score:string,#text:string,hit:struct<term:struct<#text:string>>>>>'], ['array<struct<#text:string>>'], ['array<struct<#text:string,snippet:struct<@rank:string,@score:string,#text:string,hit:struct<term:struct<#text:string>>>,@q:string>>'], ['array<struct<@q:string,snippet:struct<@rank:string,@score:string,hit:struct<term:array<struct<#text:string>>>>,#text:string>>']]
facet-ethbrowse                 | [['struct<#text:string>'], [], ['array<struct<#text:string>>']]
calisphere-id                   | [['string']]
identifier                      | [['array<struct<#text:string,@q:string>>'], ['struct<#text:string>'], ['array<struct<#text:string>>'], ['array<struct<#text:string,@q:string,snippet:struct<@rank:string,@score:string,hit:struct<term:array<struct<#text:string>>>>>>']]
institution-url                 | [['struct<#text:string>'], []]
facet-identifier                | [[], ['array<struct<#text:string>>'], ['struct<#text:string>']]
reference-image-count           | [['struct<#text:string>'], []]
source                          | [[], ['struct<#text:string>'], ['array<struct<#text:string>>'], ['string']]
language                        | [['struct<#text:string>'], [], ['array<struct<#text:string>>']]
facet-creator                   | [[], ['struct<#text:string>']]
sort-year                       | [['struct<#text:string>'], []]
facet-format                    | [[], ['struct<#text:string>']]
date                            | [['struct<@q:string,#text:string>'], ['struct<#text:string>'], [], ['array<struct<@q:string,#text:string>>'], ['struct<#text:string,@q:string>'], ['string'], ['array<struct<#text:string>>']]
sort-publisher                  | [[], ['struct<#text:string>']]
facet-coverage                  | [['struct<#text:string>'], [], ['array<struct<#text:string>>']]
oac4-tab                        | [['struct<#text:string>'], ['array<struct<#text:string>>']]
facet-type                      | [[], ['array<struct<#text:string>>'], ['struct<#text:string>']]
facet-azbrowse                  | [['struct<#text:string>'], [], ['array<struct<#text:string>>']]
type                            | [['struct<#text:string>'], ['array<struct<#text:string,@q:string>>'], ['struct<@q:string,#text:string>'], ['array<struct<@q:string,#text:string>>'], ['struct<snippet:struct<@rank:string,@score:string,hit:struct<term:struct<#text:string>>>>'], ['struct<@q:string,snippet:struct<@rank:string,@score:string,hit:struct<term:struct<#text:string>>>>']]
rights                          | [['struct<#text:string>'], ['array<struct<#text:string>>'], [], ['array<struct<#text:string,@q:string>>']]
format                          | [['struct<@q:string,#text:string>'], ['array<struct<#text:string,@q:string>>'], ['struct<#text:string>'], [], ['struct<#text:string,@q:string>'], ['array<struct<@q:string,#text:string>>']]
facet-contributor               | [[], ['array<struct<#text:string>>'], ['struct<#text:string>']]
description                     | [['array<struct<#text:string>>'], ['struct<#text:string>'], [], ['array<struct<#text:string,@q:string>>'], ['struct<@q:string,#text:string>'], ['struct<#text:string,@q:string>'], ['string']]
creator                         | [[], ['struct<#text:string>'], ['string']]
sort-creator                    | [[], ['struct<#text:string>']]
facet-institution               | [['struct<#text:string>'], []]
facet-collection-title          | [['struct<#text:string>'], []]