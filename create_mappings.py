import sys

from MappingCreator import MappingCreator

data_file = "sample_tx.txt"
if len(sys.argv) == 2:
    data_file = sys.argv[1]

MappingCreator().create_mapping(data_file)
