import bonobo


def extract():
    yield 'hello'
    yield 'world'


def transform(*args):
    yield tuple(
        map(str.title, args)
    )


def load(*args):
    print(*args)


def get_graph(**options):
    graph = bonobo.Graph()
    # graph.add_chain(range(10), product, load)
    graph.add_chain(extract,transform,load)
    return graph


# This function is to show how iterators work in Bonobo
def product(x):
    for i in range(10):
        yield x, i, x * i


def get_services(**options):
    return {}


if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )