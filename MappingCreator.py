import numpy as np
import multiprocessing as mp


# Basic structure taken from:
# http://stackoverflow.com/questions/2359253/solving-embarassingly-parallel-problems-using-python-multiprocessing
class MappingCreator:
    def __init__(self):
        manager = mp.Manager()

        # Dict to share between processes.
        # More info: https://docs.python.org/3/library/multiprocessing.html#sharing-state-between-processes
        self.mapping = manager.dict()
        self.txMapping = manager.dict()

        # Shared counter
        # More info: http://eli.thegreenplace.net/2012/01/04/shared-counter-with-pythons-multiprocessing
        self.p_counter = mp.Value('i', 0)
        self.lock = mp.Lock()

        self.p_counter_tx = mp.Value('i', 0)
        self.lock_tx = mp.Lock()

        # Max amount of processors. This setting: Use ALL of the CPU's!
        self.numprocs = mp.cpu_count()

        self.input_queue = mp.Queue()
        self.inputs_out_queue = mp.Queue()
        self.tx_out_queue = mp.Queue()

    def create_mapping(self, data_file="sample_tx.txt"):
        self.infile = open(data_file, 'r')

        p_read_inputs = mp.Process(target=self.__read_file, args=())
        p_write_inputs = mp.Process(target=self.__write_input_mappings, args=())
        p_write_tx = mp.Process(target=self.__write_tx_mappings, args=())
        p_populate_mapping = [mp.Process(target=self.__populate_mapping, args=(self.mapping, self.txMapping))
                              for i in range(self.numprocs)]

        p_read_inputs.start()
        p_write_inputs.start()
        p_write_tx.start()
        for p in p_populate_mapping:
            p.start()

        p_read_inputs.join()
        i = 0
        for p in p_populate_mapping:
            p.join()
            print "Done", i
            i += 1

        p_write_inputs.join()
        p_write_tx.join()
        self.infile.close()

    def read_input_mapping(self):
        return self.__read_mapping("inputs")

    def read_tx_mapping(self):
        return self.__read_mapping("tx")

    def __read_file(self):
        for line in self.infile:
            data = np.array(line.split(','))
            self.input_queue.put({'tx': data[0], 'inputs': data[1:]})

        for i in range(self.numprocs):
            self.input_queue.put("STOP")

    def __compress_input_value(self, mapping, input):
        with self.lock:
            if input not in mapping:
                mapping[input] = self.p_counter.value
                self.inputs_out_queue.put((mapping[input], input))
                self.p_counter.value += 1

    def __compress_tx_value(self, mapping, tx):
        with self.lock_tx:
            if tx not in mapping:
                mapping[tx] = self.p_counter_tx.value
                self.p_counter_tx.value += 1
                self.tx_out_queue.put((mapping[tx], tx))

    def __populate_mapping(self, mapping, tx_mapping):
        for row in iter(self.input_queue.get, "STOP"):
            for input in row['inputs']:
                self.__compress_input_value(mapping, input)
            self.__compress_tx_value(tx_mapping, row['tx'])

        self.inputs_out_queue.put("STOP")
        self.tx_out_queue.put("STOP")

    def __write_input_mappings(self):
        inputs = open('mappings/inputs.txt', 'a')
        for works in range(self.numprocs):
            for i, val in iter(self.inputs_out_queue.get, "STOP"):
                if "\n" not in val:
                    val += "\n"
                inputs.write("%s,%s" % (i, val))
        inputs.close()

    def __write_tx_mappings(self):
        tx = open('mappings/tx.txt', 'a')
        for works in range(self.numprocs):
            for i, val in iter(self.tx_out_queue.get, "STOP"):
                tx.write("%s,%s\n" % (i, val))
        tx.close()

    @staticmethod
    def __read_mapping(file):
        mapping = {}
        with open('mappings/' + file + '.txt', 'rb') as f:
            for line in f:
                (key, val) = line.split(",")
                mapping[int(key)] = val
        return mapping


mappings = MappingCreator()
mappings.create_mapping()
print mappings.read_input_mapping()
print mappings.read_tx_mapping()

