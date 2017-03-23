import pandas as pd
import numpy as np
import pickle


class MappingCreator:
    def __init__(self):
        self.mapping = {}
        self.counter = 0
        self.txMapping = {}
        self.txCounter = 0
        pass

    def createMapping(self, data_file="sample_tx.txt"):
        self.__readFile(data_file)
        self.__populateMapping()
        self.__writeMappings()

    def readInputMapping(self):
        return self.__readMapping("inputs")

    def readTxMapping(self):
        return self.__readMapping("tx")

    def __readFile(self, data_file):
        self.full_data = pd.DataFrame(columns=['tx','inputs'])
        with open(data_file, 'r') as f:
            for line in f:
                data = np.array(line.split(','))
                self.full_data = self.full_data.append({'tx':data[0], 'inputs': data[1:]}, ignore_index=True)

    def __compressInputValue(self, input):
        if input not in self.mapping:
            self.mapping[input] = self.counter
            self.counter += 1

    def __compressTxValue(self, tx):
        if tx not in self.txMapping:
            self.txMapping[tx] = self.txCounter
            self.txCounter += 1

    def __populateMapping(self):
        for ix, value in self.full_data['inputs'].iteritems():
            for input in value:
                self.__compressInputValue(input)

        for ix, value in self.full_data['tx'].iteritems():
            self.__compressTxValue(value)

    def __writeMappings(self):
        with open('mappings/inputs.pkl', 'wb') as f:
            pickle.dump(self.mapping, f, pickle.HIGHEST_PROTOCOL)
        with open('mappings/tx.pkl', 'wb') as f:
            pickle.dump(self.txMapping, f, pickle.HIGHEST_PROTOCOL)

    def __readMapping(self, file):
        with open('mappings/'+file+'.pkl', 'rb') as f:
            return pickle.load(f)
