import sys
from lxml import etree

output_file = open('/data/s1968130/xml_parser_output.txt', "wb")
data_file = "sample_output.xml"
if len(sys.argv) == 2:
    data_file = sys.argv[1]


def fast_iter(context, func, *args, **kwargs):
    for event, elem in context:
        func(elem, event, *args, **kwargs)
        elem.clear()
        for ancestor in elem.xpath('ancestor-or-self::*'):
            while ancestor.getprevious() is not None:
                del ancestor.getparent()[0]
    del context


class ParseThingy:
    def __init__(self):
        self.reset()
        self.outputs = {}
        self.inputs = {}
        self.is_in_input = False
        self.index = 0

    def reset(self):
        self.current_tx = ""
        self.current_inputs = []
        self.current_outputs = []
        self.current_input_indexes = []

    def process_element(self, elt, evt):
        self.parseTx(elt, evt)
        self.parseInput(elt, evt)
        self.parseOutput(elt, evt)

    def parseTx(self, elt, evt):
        if elt.tag == "tx" and evt == "start":
            if self.index % 10000 == 0:
                print "Parsing TX %i" % self.index
            self.index += 1
            self.current_tx = elt.attrib['hash']
        if elt.tag == "tx" and evt == "end":
            self.outputs[self.current_tx] = self.current_outputs
            self.inputs[self.current_tx] = {k: v for k, v in zip(self.current_inputs, self.current_input_indexes)}
            self.reset()

    def parseInput(self, elt, evt):
        if elt.tag == "inputs" and evt == "start":
            self.is_in_input = True
        if self.is_in_input and elt.tag == "index" and evt == "start":
            self.current_input_indexes.append(elt.text)
        if elt.tag == "in_tx_hash" and evt == "start":
            self.current_inputs.append(elt.text)
        if elt.tag == "outputs" and evt == "start":
            self.is_in_input = False

    def parseOutput(self, elt, evt):
        if elt.tag == "address" and evt == "start":
            self.current_outputs.append(elt.text)


    def parse(self):
        with open(data_file) as f:
            context = etree.iterparse(f, events=('start', 'end',), html=True)
            fast_iter(context, self.process_element)

        print "Done parsing!"
        self.index = 0
        for tx, input_txs in self.inputs.iteritems():
            if self.index % 10000 == 0:
                print "Writing TX: %i" % self.index
            self.index += 1
            inputs = []
            for input, i in input_txs.iteritems():
                tx_inputs = self.outputs.get(input, None)
                if tx_inputs is None:
                    print ("Skipping input: %s" % input)
                    continue
                inputs.append(tx_inputs[int(i)])

            stringa = tx + "," + ",".join(inputs) + "\n"
            output_file.write(stringa)

        output_file.close()

ParseThingy().parse()

#
# txs = []
# print "Starting..."
# txs[0:220000000] = root.findall('.//block/tx')
# print "Found all TX's of size %i" % len(txs)
# for tx in txs:
#     outputs = tx.findall('.//outputs/output/address')
#     addresses = []
#     for address in outputs:
#         addresses.append(address.text)
#     mapped[tx.attrib["hash"]] = addresses
# print ("Done creating mapping.")
#
#
# for tx_i, tx in enumerate(txs):
#     if tx_i % 100000 == 0:
#         print("At TX=%i" % tx_i)
#     indexes = tx.findall(".//inputs/input/index")
#     inputs = []
#     for i, input in enumerate(tx.findall(".//inputs/input/in_tx_hash")):
#         tx_inputs = mapped.get(input.text, None)
#         if tx_inputs is None:
#             print ("Skipping input: %s" % input.text)
#             continue
#         inputs.append(tx_inputs[int(indexes[i].text)])
#
#     stringa = str(tx.attrib["hash"]) + "," + ",".join(inputs) + "\n"
#     output_file.write(stringa)
#
# output_file.close()
