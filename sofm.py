import random
import collections
import math

from pprint import pprint

from avro.datafile import DataFileReader
from avro.io import DatumReader

from pdb import set_trace


class sofm:

	neurons = []

	def __init__(self, X, Y, data, label, epochs, learning_rate):
		self.data = data
		self.label = label
		self.epochs = epochs

		self.learning_rate = learning_rate
		self._sigma = max(X, Y)/2;
		self._lambda = epochs/math.log(self._sigma);


		for i in range(X):
			self.neurons.append([])
			for j in range(Y):
				self.neurons[i].append([random.SystemRandom().random() for _ in range(len(data[0]))])

	def train(self):
		for e in range(1, self.epochs + 1):
			v = random.choice(self.data)
			d_x, w_n = self._find_bmu(v)
			self._update_neigh(v, w_n, e)

		result = collections.defaultdict(list)

		for i, d in enumerate(self.data):
			d_x, w_n = self._find_bmu(d)
			result[w_n].append(d + [self.label[i]])
		return result 

	def _find_bmu(self, v):
		d_x, w_n = 100, ()
		for i, row in enumerate(self.neurons):
			for j, col in enumerate(row):
				tmp = 0

				for k in range(len(v)):
					tmp += math.pow(v[k] - col[k], 2)

				tmp = math.sqrt(tmp)
				if d_x > tmp:
					d_x = tmp
					w_n = (i, j)

		return d_x, w_n

 
	def _update_neigh(self, v, w_n, rlen):
		rad = self._sigma * math.exp(-1 * rlen/self._lambda)

		neighbourhood = self._find_neighbourhood(w_n, rad)

		lr = self.learning_rate * math.exp(-1 * rlen / self.epochs)

		for node in neighbourhood:
			influence = math.exp(-1 * math.pow(node[-1], 2) / (2 * rad * rad))
			w_n = []
			w = self.neurons[node[0]][node[1]] 

			for i in range(len(v)):
				w_n.append(w[i] + lr * influence * (v[i] - w[i]))
			self.neurons[node[0]][node[1]] = w_n

	def _find_neighbourhood(self, w_n, rad):
		nodes = set()
		nodes.add((w_n[0], w_n[1], 0))
		
		for i, row in enumerate(self.neurons):
			for j, col in enumerate(row):
				if (i, j) == w_n:
					continue

				dist = math.sqrt(math.pow(i - w_n[0], 2) + math.pow(j - w_n[1], 2))

				if rad > dist:
					nodes.add((i, j, dist))

		return nodes


if __name__ == '__main__':
	import matplotlib.pyplot as plt
	



	training_data, label = [], []
	epochs = 500
	learning_rate = 0.1

	with open('beers.avro', 'rb') as fd:
		reader = DataFileReader(fd, DatumReader())
		for row in reader:
			label.append(row['name'])
			training_data.append([
				row['alcohol'],
				row['color'],
				row['clarity'],
				row['bitterness'],
				row['sweetness'],
			])
		reader.close()

	s = sofm(5, 5, training_data, label, epochs, learning_rate)
	result = s.train()

	fig = plt.figure()
	plt.clf()
	
	#ax.set_aspect(1)
	res = ax.imshow(np.array(norm_conf), cmap=plt.cm.jet, interpolation='nearest')
	plt.show()

#	set_trace()
