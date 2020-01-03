# This is a more complex example to get data from URL and perform some transformations

import bonobo
import requests
from bonobo.config import use_context_processor
from bonobo.config import use


FABLABS_API_URL = 'https://public-us.opendatasoft.com/api/records/1.0/search/?dataset=fablabs&rows=1000'


@use('http')
def extract_fablabs(http):
    yield from http.get(FABLABS_API_URL).json().get('records')


def with_opened_file(self, context):
    with open('output.txt', 'w+') as f:
        yield f


@use_context_processor(with_opened_file)
def write_repr_to_file(f, *row):
    f.write(repr(row) + "\n")


def get_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(
        extract_fablabs,
        bonobo.Limit(5),
        write_repr_to_file,
    )
    return graph


def get_services():
    http = requests.Session()
    http.headers = {'User-Agent': 'Monkeys!'}
    return {
        'http': http
    }


if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(get_graph(**options), services=get_services(**options))
