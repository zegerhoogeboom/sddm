import sys

import datetime
import numpy as np
from lxml import etree
from mpi4py import MPI
import cPickle as pickle

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

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
        self.tx_count = 0
        self.reset()
        self.outputs = {}
        self.inputs = {}
        self.is_in_input = False
        self.index = 0
        self.items_per_process = 6875000
        self.current_items = []
        self.nodeIndexes = [1 for i in range(size - 1)]
        self.initial = True
        self.done = False

    def reset(self):
        self.current_tx = ""
        self.current_inputs = []
        self.current_outputs = []
        self.current_input_indexes = []

    def distribute_elements(self, elt, evt):
        print "appending..."
        if elt.tag == "tx" and evt == "end":
            self.tx_count += 1
        self.current_items.append(({'text': elt.text, 'tag': elt.tag, 'attrib': dict(elt.attrib)}, evt))
        if self.tx_count >= self.items_per_process:
            for i, node in enumerate(self.nodeIndexes):
                if node == 1:
                    comm.send(pickle.dumps(self.current_items), dest=i + 1)
                    print "Done sending"
                    self.current_items = []
                    self.tx_count = 0
                    self.nodeIndexes[i] = -1
                    break
                elif 1 not in self.nodeIndexes and not self.done:
                    print "Waiting.."
                    status = MPI.Status()
                    comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                    source = status.Get_source()
                    print "Node %i is free!" % source
                    self.nodeIndexes[source-1] = 1
                    break
                else:
                    print "Stuck!"

    def worker(self):
        print "Node %i Working..." % rank
        items = comm.recv(source=0)
        items = pickle.loads(items)
        for item in items:
            self.process_element(item[0], item[1])
        self.writeOutput()
        comm.send(1, dest=0)
        print "Worker %i done" % rank

    def process_element(self, elt, evt):
        self.parseTx(elt, evt)
        self.parseInput(elt, evt)
        self.parseOutput(elt, evt)

    def parseTx(self, elt, evt):
        if elt["tag"] == "tx" and evt == "start":
            if self.index % 10000 == 0:
                print "Parsing TX %i" % self.index
            self.index += 1
            self.current_tx = elt["attrib"]['hash']
        if elt["tag"] == "tx" and evt == "end":
            self.outputs[self.current_tx] = self.current_outputs
            self.inputs[self.current_tx] = {k: v for k, v in zip(self.current_inputs, self.current_input_indexes)}
            self.reset()

    def parseInput(self, elt, evt):
        if elt["tag"] == "inputs" and evt == "start":
            self.is_in_input = True
        if self.is_in_input and elt["tag"] == "index" and evt == "start":
            self.current_input_indexes.append(elt["text"])
            print "TEXT: %s" % elt["text"]
        if elt["tag"] == "in_tx_hash" and evt == "start":
            self.current_inputs.append(elt["text"])
        if elt["tag"] == "outputs" and evt == "start":
            self.is_in_input = False

    def parseOutput(self, elt, evt):
        if elt["tag"] == "address" and evt == "start":
            self.current_outputs.append(elt["text"])

    def writeOutput(self):
        print "Worker %i writing output" % rank
        now = datetime.datetime.now().strftime('%Y:%m:%d:%H:%M:%S')
        output_file = open('/data/s1968130/sddm_xml_parse_output/xml_parser_output_%s_%s.txt' % (rank, now), "wb")
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

        print "Worker %i wrote output" % rank
        output_file.close()

    def parse(self):
        if rank == 0:
            with open(data_file) as f:
                context = etree.iterparse(f, events=('start', 'end',), html=True)
                fast_iter(context, self.distribute_elements)
                self.done = True
            print "Done parsing!"
        else:
            print "Receiving! %i" % rank
            self.worker()


ParseThingy().parse()
