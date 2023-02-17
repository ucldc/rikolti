import requests
import argparse
import json
import sys
import lambda_shepherd
import logging


def map_endpoint(url):
    # TODO: this sort of translation from registry's mapper_type to
    # rikolti's mapper_type should really be done in the registry.
    # once we have a firmer rikolti vocabulary of mappers, we should
    # migrate the registry's data.
    lookup = {
        'ucldc_nuxeo': 'nuxeo.nuxeo',
        'oac_dc': 'oac.oac',
        'islandora_oai_dc': 'oai.islandora',
        'cca_vault_oai_dc': 'oai.cca_vault',
        'chapman_oai_dc': 'oai.chapman',
        'tv_academy_oai_dc': 'oai.tv_academy',
        'yosemite_oai_dc': 'oai.yosemite',
        'up_oai_dc': 'oai.up',
        'ucsc_oai_dpla': 'oai.samvera',
        'contentdm_oai_dc': 'oai.contentdm',
        'arck_oai': 'oai.contentdm.arck',
        'black_gold_oai': 'oai.contentdm.blackgold',
        'chico_oai_dc': 'oai.contentdm.chico',
        'chula_vista_pl_contentdm_oai_dc': 'oai.contentdm.cvpl',
        'contentdm_oai_dc_get_sound_thumbs': 'oai.contentdm.pepperdine',
        'csudh_contentdm_oai_dc': 'oai.contentdm.csudh',
        'lapl_oai': 'oai.contentdm.lapl',
        'csu_dspace_mets': 'oai.contentdm.csu_dspace'
    }

    collection_page = url
    results = []

    while collection_page:
        try:
            response = requests.get(url=collection_page)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            msg = (
                f"[{collection_page}]: "
                f"{err}; A valid collection id is required for mapping"
            )
            print(msg)
            collection_page = None
            break
        total_collections = response.json().get('meta', {}).get('total_count', 1)
        print(
            f">>> Mapping {total_collections} collections "
            f"described at {collection_page}"
        )

        collection_page = response.json().get('meta', {}).get('next')
        if collection_page:
            collection_page = f"https://registry.cdlib.org{collection_page}"
        logging.debug(f"Next page: {collection_page}")
        collections = response.json().get('objects', [response.json()])
        for collection in collections:
            log_msg = f"[{collection['collection_id']}]: " + "{}"
            print(log_msg.format(
                f"Mapping collection {collection['collection_id']} - "
                f"{collection['solr_count']} items in solr as of "
                f"{collection['solr_last_updated']}"
            ))
            logging.debug(log_msg.format(f"lambda payload: {collection}"))
            try:
                collection['mapper_type'] = lookup[collection['mapper_type']]
                return_val = lambda_shepherd.map_collection(
                    json.dumps(collection), None)
            except KeyError:
                print(f"[{collection['collection_id']}]: {collection['mapper_type']} not yet implemented")
                continue
            except FileNotFoundError:
                print(f"[{collection['collection_id']}]: not fetched yet")
                continue
            results.append(return_val)

            print(log_msg.format(f"{json.dumps(return_val)}"))

    # print(json.dumps(results))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run mapper for registry endpoint")
    parser.add_argument('endpoint', help='registry api endpoint')
    args = parser.parse_args(sys.argv[1:])
    map_endpoint(args.endpoint)
    sys.exit(0)
