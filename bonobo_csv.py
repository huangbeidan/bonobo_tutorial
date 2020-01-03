# Read data from csv and perform some basic transformations

import bonobo
import pandas as pd
from bonobo.config import use_context_processor


def get_price():
    data = pd.read_csv('train.csv')
    price = data['SalePrice'].tolist()
    yield from price


def transform(*args):
    # clean the data format here
    data = str(args).replace(",", "").replace("(", "").replace(")", "")
    # print(data)
    return "Transform: " + data + ", data2:" + data


def load(*args):
    # with open('pricing.txt', 'a+', encoding='utf8') as f:
    #     f.write((str(args) + '\n'))
    print(str(args))


def with_opened_file(self, context):
    with open('output_csv.txt', 'w+') as f:
        yield f


# decorator is used here: every time we open the file, and append row to the existing rows, instead of overwriting it
# Or directly use load (not writing to file)
@use_context_processor(with_opened_file)
def write_repr_to_file(f, *row):
    f.write(repr(row) + "\n")


# if we don't use decorator, only one record will be written (will over-write the old records)
def write_to_file_onetime(*row):
    with open('output_csv_trial.txt', 'w+') as f:
        f.write(repr(row) + "\n")


if __name__ == '__main__':
    graph = bonobo.Graph()
    graph.add_chain(
        get_price,
        transform,
        bonobo.Limit(20),
        write_repr_to_file,
    )
    bonobo.run(graph)
