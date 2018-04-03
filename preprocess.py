#!/usr/bin/python3
import csv
import math
import collections
import operator
from pdb import set_trace
from pprint import pprint

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

class Preprocess():

    COLOR = {
        'class': {
            'dökkbrúnn': 0,
            'svarbrúnn': 1,
            'rafbrúnn': 2,
            'brúnn': 2,
            'rafgullinn': 3,
            'rafrauður': 3,
            'ljósrafgullinn': 3,
            'fölgulur': 3,
            'múrsteinsrauður': 3,
            'gullinn': 4,
            'kirsuberjarauður': 4,
            'gulur': 4,
            'rauður': 4,
            'ljósbrúnn': 5,
            'ljóskirsuberjarauður': 5,
            'ljósgullin': 5,
            'ljósgullinn': 5,
        },
        'maximum': 5
    }

    CLARITY = {
        'class': {
            None: 0,
            'óskír': 1,
            'skýjaður': 1,
        },
        'maximum': 1
    }

    SWEETNESS = {
        'class': {
            None: 0,
            'sætur': 3,
            'smásætur': 2,
            'sætuvottur': 1,
            'ósætur': 0
        },
        'maximum': 3
    }

    BITTERNESS = {
        'class': {
            None: 0,
            'hverfandibeiskja': 0,
            'hverfandlbeiskja': 0,
            'litlabeiskju': 1,
            'lítilbeiskja': 1,
            'meðalbeiskja': 2,
            'meðalbeiskur': 2,
            'miðlungsbeiskja': 2,
            'beiskur': 3,
            'humlabeiskja': 3,
            'mikilbeiskja': 4,
            'öflugbeiskjan': 4,
        },
        'maximum': 4
    }
     
    def bitterness(self, desc):
        for i, e in enumerate(desc):
            if 'beiskju' == e:
                return desc[i - 1] + e

            if 'beiskjan' == e:
                return desc[i - 1] + e

            if 'beiskja' == e:
                return desc[i - 1] + e

            if 'beiskir' == e:
                continue

            if 'beisk' == e:
                continue

            if 'beisk' in e:
               return e
        
        return None

    def parse(self, desc, name):
        def _findfeature(att, desc):
            for key in att['class'].keys():
                if key in desc:
                    return key
            print('Missing feature ', att, name)
            return None
 
        return {
            'color': _findfeature(self.COLOR, desc),
            'clarity': _findfeature(self.CLARITY, desc),
            'sweetness': _findfeature(self.SWEETNESS, desc),
            'bitterness': self.bitterness(desc),
        }

    def _get_base_stats(self, collection, alc_average):
        freq = {
            'color': collections.defaultdict(int),
            'bitterness': collections.defaultdict(int)
        }


        stdev = 0
        for i, c in enumerate(collection):
            freq['color'][c['color']] += 1
            freq['bitterness'][c['bitterness']] += 1
            stdev += math.pow((c['alcohol'] - alc_average), 2)

        stdev = math.sqrt(stdev / i)
        mode_color = max(freq['color'].items(), key=operator.itemgetter(1))[0]
        mode_bitterness = max(freq['bitterness'].items(), key=operator.itemgetter(1))[0]


    def run(self):
        # for normalizing alcohol
        minimum, maximum, average = 100, 0, 0


        with open('raw.csv', 'r') as fd:
            csv_reader = csv.reader(fd, delimiter=',')

            collection = []
            for i, row in enumerate(csv_reader):
                desc = row[3].lower().replace('.', '').replace(',', '')

                alc = float(row[-1])
                if alc < minimum:
                    minimum = alc
                if alc > maximum:           
                    maximum = alc
                average += alc

                # Remove gifts or items without description
                if 'engin' in desc:
                    continue

                if 'gjafa' in desc or 'gjafa' in row[0]:
                    continue 

                if 'öskju' in desc or 'öskju' in row[0]:
                    continue

                if 'flöskur m/glasi' in desc or 'kútur' in row[0]:
                    continue

                features = self.parse(desc.split(), row[0])
                features['name'] = row[0]
                features['alcohol'] = alc
                collection.append(features)

        average = average / (i + 1)
        
        base = self._get_base_stats(collection, average)

        with open('beers.avsc', 'r') as fd:
            schema = avro.schema.Parse(fd.read())

        with open('beers.avro', 'wb') as fd:
            writer = DataFileWriter(fd, DatumWriter(), schema)
        
            denominator_alc = maximum - minimum
            
            for c in collection:
                c['bitterness'] = self.BITTERNESS['class'][c['bitterness']] / self.BITTERNESS['maximum']
                c['color'] = self.COLOR['class'][c['color']] / self.COLOR['maximum']
                c['clarity'] = self.CLARITY['class'][c['clarity']] / self.CLARITY['maximum']
                c['sweetness'] = self.SWEETNESS['class'][c['sweetness']] / self.CLARITY['maximum']
                c['alcohol'] = (c['alcohol'] - minimum) / denominator_alc
                writer.append(c)
            writer.close()

if __name__ == '__main__':
    Preprocess().run()
