from pymongo import MongoClient
from typing import List
import pandas as pd
import yaml
import itertools


def load_config_file() -> dict:
    with open('config.yml', 'r') as file:
        config = yaml.safe_load(file)
    return config


def import_data_from_mongo() -> List[dict]:
    config = load_config_file()
    _mongodb_credentials = config.get('mongodb', {})
    client = MongoClient(
        host=_mongodb_credentials.get('host', None),
        port=_mongodb_credentials.get('port', None),
        username=_mongodb_credentials.get('user', None),
        password=_mongodb_credentials.get('password', None),

    )
    db = client.get_database(_mongodb_credentials.get('database', None))
    col = db.get_collection(_mongodb_credentials.get('collection', None))

    # Get MAX import_meta.timestamp_imported
    result = col.aggregate([{'$group' : { '_id': None, 'max': { '$max' : "$import_meta.timestamp_imported" }}}]).next()
    return list(col.find({'import_meta.timestamp_imported': result.get('max', None)}))


def generate_edges(data: List[dict]) -> None:
    tags = []
    for package in data:
        package_tags = []
        for keyword in package.get('keywords', {}).get('de', None):
            package_tags.append(keyword)
        tags.append(package_tags)
    comb = []
    for tag_list in tags:
        comb.extend(list(itertools.combinations(tag_list, 2)))

    # Sort tuple lements inside list of tuples
    comb = [tuple(sorted(i)) for i in comb]
    (
        pd.DataFrame(comb, columns=['Source', 'Target'])
        .groupby(['Source', 'Target'])
        .size()
        .reset_index()
        .rename(columns={0: 'Weight'})
        .assign(Type='Undirected')
        .sort_values('Weight', ascending=False)
        .to_csv('edges.csv', index=False)
    )


def generate_nodes(data: List[dict]) -> None:
    node_data = []
    for package in data:
        for keyword in package.get('keywords', {}).get('de', []):
            org = package.get('organization', {}).get('display_name', {}).get('de', None)
            node_data.append((keyword, org))

    (
        pd.DataFrame(node_data, columns=['Id', 'Org'])
        .groupby('Id')
        .agg(dict(Id='size', Org=pd.Series.mode))
        .rename(columns=dict(Id='Size', Org='dominant_org'))
        .reset_index()
        .assign(Label=lambda x: x['Id'])
        .sort_values('Size', ascending=False)
        .to_csv('nodes.csv', index=False)
    )


def main():
    data = import_data_from_mongo()
    generate_edges(data)
    generate_nodes(data)


if __name__ == '__main__':
    main()

