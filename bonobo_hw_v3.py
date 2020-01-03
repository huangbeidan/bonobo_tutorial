import csv

import bonobo
import pandas as pd
from bonobo.config import use_context_processor


def extract():
    yield from data.itertuples()


# map the terms(e.g. LandSlope) to full-term in dictionary
def transform(*args):
    # get the index for the columns that you want to look up
    args = list(args)
    # filter out the rows (yearbuilt < 1980)
    idx0 = category_dict['YearBuilt'] + 1
    if int(args[idx0]) < 1980:
        return None
    # plus 1 because the first element of args is the system idx
    idx1 = category_dict['LandSlope'] + 1
    idx2 = category_dict['LotShape'] + 1
    idx3 = category_dict['Neighborhood'] + 1

    # replace the tuple values
    args[idx1] = term_dictionary[args[idx1]]
    args[idx2] = term_dictionary[args[idx2]]
    args[idx3] = term_dictionary[args[idx3]]
    return args[1:]


# function for writing to the files
def with_opened_file(self, context):
    with open('output/output_lotinfo.csv', 'w+') as f1, \
            open('output/output_homeinfo.csv', 'w+') as f2, open('output/output_salesinfo.csv', 'w+') as f3:

        wr1 = csv.writer(f1, delimiter=',')
        wr2 = csv.writer(f2, delimiter=',')
        wr3 = csv.writer(f3, delimiter=',')
        yield wr1, wr2, wr3


# decorator is used here: every time we open the file, and append row to the existing rows, instead of overwriting it
@use_context_processor(with_opened_file)
def write_repr_to_file(wr1, wr2, wr3,  *row):
    global first_line_written
    # uncomment to see why I need to replace "[" and "]"
    # f.write(repr(row))
    if row is None:
        return

    if not first_line_written:
        wr1.writerow([category_list[a] for a in lotinfo_idx])
        wr2.writerow([category_list[a] for a in homeinfo_idx])
        wr3.writerow([category_list[a] for a in salesinfo_idx])


    tuples_lotinfo = [row[0][a] for a in lotinfo_idx]
    wr1.writerow(tuples_lotinfo)

    tuples_homeinfo = [row[0][a] for a in homeinfo_idx]
    wr2.writerow(tuples_homeinfo)

    tuples_salesinfo = [row[0][a] for a in salesinfo_idx]
    wr3.writerow(tuples_salesinfo)

    first_line_written = True


def getdict():
    d = defaultdict(lambda: "NA")
    input_file = open("data_description.txt")
    for line in input_file:
        information = line.split('\t')
        # get rid of the information-less tuples
        if len(information) != 2 or information[0].strip().startswith('\n') or len(information[0])==0 or not information[0].startswith(' '):
            continue
        # build the dictionary from the tuples with data cleaning
        term = information[0].strip()
        desri = information[1].strip()
        d[term] = desri
    return d


# write dictionary to csv file
# output: two column data dictionary csv file
def write_dict_to_csv():
    with open('output/term_dict.csv', 'w') as f:
        f.write('Abbreviation,Description\n')
        for key in term_dictionary.keys():
            f.write("%s,%s\n" % (key, term_dictionary[key]))


if __name__ == '__main__':
    from collections import defaultdict

    # default setting
    first_line_written = False

    # prepare for the data
    data = pd.read_csv('train.csv', encoding='ISO-8859-1')

    # construct category dictionary (map the category to index)
    category_dict = defaultdict()
    category_list = data.columns.tolist()
    for i in range(len(category_list)):
        category_dict[category_list[i]] = i
    print(category_dict)

    # construct the terms dictionary (get the dict from data descriptions file)
    term_dictionary = getdict()
    print(term_dictionary)

    # write the terms dictionary into two-column csv
    write_dict_to_csv()

    # divide the table into 3 sub-tables
    # TODO: The index needs to be changed according to the requirements
    lotinfo_idx = [0,1,2,3,4,5,6,7,8,9,10]
    homeinfo_idx = [11,12,13,14,15,16,17,18,19,20]
    salesinfo_idx = [21,22,23,24,25]

    # build Bonobo pipeline
    graph = bonobo.Graph()
    graph.add_chain(
        extract,
        # the transform step will replace the abbr. with its full description
        transform,
        bonobo.Limit(100),
        write_repr_to_file,
    )
    bonobo.run(graph)

