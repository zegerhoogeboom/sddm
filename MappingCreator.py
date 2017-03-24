import pandas as pd
import numpy as np
import multiprocessing as mp


class MappingCreator:
    def __init__(self):
        self.counter = 0
        self.txCounter = 0
        manager = mp.Manager()
        self.mapping = manager.dict()
        self.txMapping = manager.dict()

        self.p_counter = mp.Value('i', 0)
        self.lock = mp.Lock()

        self.p_counter_tx = mp.Value('i', 0)
        self.lock_tx = mp.Lock()

        self.numprocs = mp.cpu_count()

        self.inq = mp.Queue()
        self.inputs_out_queue = mp.Queue()
        self.tx_out_queue = mp.Queue()

        self.full_data = pd.DataFrame(columns=['tx','inputs'])

    def createMapping(self, data_file="sample_tx.txt"):
        self.infile = open(data_file, 'r')

        self.pin = mp.Process(target=self.__readFile, args=())
        self.pout = mp.Process(target=self.__writeMappings, args=())
        self.ps = [ mp.Process(target=self.__populateMapping, args=(self.mapping, self.txMapping))
                    for i in range(self.numprocs)]

        self.pin.start()
        self.pout.start()
        for p in self.ps:
            p.start()

        self.pin.join()
        i = 0
        for p in self.ps:
            p.join()
            print "Done", i
            i += 1

        self.pout.join()
        self.infile.close()

    def readInputMapping(self):
        return self.__readMapping("inputs")

    def readTxMapping(self):
        return self.__readMapping("tx")

    def __readFile(self):
        for line in self.infile:
            data = np.array(line.split(','))
            self.inq.put({'tx':data[0], 'inputs': data[1:]})

        for i in range(self.numprocs):
            self.inq.put("STOP")

    def __compressInputValue(self, mapping, input):
        with self.lock:
            if input not in mapping:
                mapping[input] = self.p_counter.value
                self.p_counter.value += 1
                return mapping[input]
        return None

    def __compressTxValue(self, mapping, tx):
        with self.lock_tx:
            if tx not in mapping:
                mapping[tx] = self.p_counter_tx.value
                self.p_counter_tx.value += 1
                return mapping[tx]
        return None

    def __populateMapping(self, mapping, txMapping):
        for row in iter(self.inq.get, "STOP"):
            for input in row['inputs']:
                iv = self.__compressInputValue(mapping, input)
                if iv is not None:
                    self.inputs_out_queue.put((iv, input))
            tv = self.__compressTxValue(txMapping, row['tx'])
            if tv is not None:
                self.tx_out_queue.put((tv, row['tx']))
        self.inputs_out_queue.put("STOP")
        self.tx_out_queue.put("STOP")

    def __writeMappings(self):
        inputs = open('mappings/inputs.txt', 'a')
        tx = open('mappings/tx.txt', 'a')
        for works in range(self.numprocs):
            for i, val in iter(self.inputs_out_queue.get, "STOP"):
                if "\n" not in val:
                    val += "\n"
                inputs.write("%s,%s" % (i, val))
            for i, val in iter(self.tx_out_queue.get, "STOP"):
                tx.write("%s,%s\n" % (i, val))
        inputs.close()
        tx.close()

    def __readMapping(self, file):
        mapping = {}
        with open('mappings/'+file+'.txt', 'rb') as f:
            for line in f:
                (key, val) = line.split(",")
                mapping[int(key)] = val
        return mapping

MappingCreator().createMapping()