import sys

from bs4 import BeautifulSoup

data_file = "sample_output.xml"
if len(sys.argv) == 2:
    data_file = sys.argv[1]

print "Opening data file"
with open(data_file, "r") as data_file_handle:
    y = BeautifulSoup(data_file_handle, "lxml")
    print "Parser constructed"
output_file = open('xml_parser_output.txt', "wb")

mapped = {}
result = {}
print "Starting..."
txs = y.find_all("tx")
print "Found all TX's of size %i" % len(txs)
for tx_i, tx in enumerate(txs):
    # print tx
    if tx_i % 100000 == 0:
        print("Creating mapping for TX=%i" % tx_i)
    outputs = tx.outputs.find_all("address")
    addresses = []
    for address in outputs:
         addresses = addresses + address.contents
    mapped[tx["hash"]] = addresses

print ("Done creating mapping.")

for tx_i, tx in enumerate(txs):
    if tx_i % 100000 == 0:
        print("At TX=%i" % tx_i)
    indexes = tx.inputs.find_all("index")
    inputs = []
    for i, input in enumerate(tx.inputs.find_all("in_tx_hash")):
        tx_inputs = mapped.get(input.contents[0], None)
        if tx_inputs is None:
            print ("Skipping input: %s" % input.contents[0])
            continue
        inputs.append(tx_inputs[int(indexes[i].contents[0])])

    stringa = str(tx["hash"]) + "," + ",".join(inputs) + "\n"
    output_file.write(stringa)

output_file.close()
