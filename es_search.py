#!/usr/local/bin/python

import json

import click
import elasticsearch


@click.command()
@click.argument('query', required=True)
@click.option('---raw-result/---no-raw-result', default=False)
def search(query, raw_result):
    es = elasticsearch.Elasticsearch()
    matches = es.search('testfsobj', q=query)
    hits = matches['hits']['hits']
    if not hits:
        click.echo('No matches found')
    else:
        if raw_result:
            click.echo(json.dumps(matches, indent=4))
        for hit in hits:
            click.echo('Subject:{}\nPath: {}\n\n'.format(
                hit['_source']['filesize'],
                hit['_source']['path']
            ))

if __name__ == '__main__':
    search()
