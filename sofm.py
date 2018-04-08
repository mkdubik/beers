import random
import collections
import math

from pprint import pprint

from avro.datafile import DataFileReader
from avro.io import DatumReader

from pdb import set_trace
from matplotlib import pyplot as plt

class sofm:

    neurons = []

    def __init__(self, X, Y, data, label, epochs, learning_rate, sigma=None):
        # Som implementation
        # References:
        #  https://github.com/JustGlowing/minisom
        #  http://www.ai-junkie.com/ann/som/som1.html
        #  http://users.ics.aalto.fi/jhollmen/dippa/node9.html
        #  http://www.cs.bham.ac.uk/~jxb/NN/l16.pdf
        #  http://davis.wpi.edu/~matt/courses/soms/

        self.data = data
        self.label = label
        self.epochs = epochs

        self.learning_rate = learning_rate
        self._sigma = max(X, Y)/2 if sigma is None else sigma
        self._lambda = epochs/math.log(self._sigma)


        for i in range(X):
            self.neurons.append([])
            for j in range(Y):
                self.neurons[i].append([random.SystemRandom().random() for _ in range(len(data[0]))])

    def train(self):
        for e in range(1, self.epochs + 1):
            v = random.choice(self.data)
            d_x, w_n = self._find_bmu(v)
            self._update_neigh(v, w_n, e)

        result = [[[] for k in range(len(self.neurons))] for i in range(len(self.neurons))]

        for i, d in enumerate(self.data):
            d_x, w_n = self._find_bmu(d)
            result[w_n[0]][w_n[1]].append(self.label[i])

        u_mat, total = self._get_umat()
        return result, u_mat

    def _get_umat(self):
        # Generate a unified distance matrix from neurons
        X, Y, W = len(self.neurons), len(self.neurons[0]), 5

        mat = [[0 for _ in range(X)] for _ in range(Y)] 
        max_dist = 0
        stotal = 0
        for i in range(X):
            for j in range(Y):
                center = self.neurons[i][j]

                numinave = 0
                total = 0.0

                for k in range(-W, W):
                    for o in range(-W, W):
                        if (k + i) >= 0 and (k + i) < X and \
                            (j + o) >= 0  and (o + j) < Y:
                            D = math.sqrt(sum([math.pow(self.neurons[k + i][j + o][p] - center[p], 2) for p in range(len(center))]))
                            total += D
                            numinave += 1

                total /= (numinave - 1)
                if total > max_dist:
                    max_dist = total
                
                mat[i][j] = total
                stotal += total

        return mat, stotal

    def _find_bmu(self, v):
        # Find best matching unit for vector v and its distance from it

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

    def quantization_error(self):
        error = 0
        for x in self.data:
            d_x, w_n = self._find_bmu(x)
            error += d_x
        return error/len(self.data)

if __name__ == '__main__':
    import matplotlib.pyplot as plt
    import numpy as np

    training_data, label = [], []
    epochs = 5000
    learning_rate = 1.0

    X, Y = 25, 25

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

    so = sofm(X, Y, training_data, label, epochs, learning_rate)
    result, u_mat = so.train()
    qe = so.quantization_error()

    print(qe)

    fig = plt.figure()
    plt.clf()
    ax = fig.add_subplot(111)
    ax.set_aspect(1)
    res = ax.imshow(np.array(u_mat), cmap=plt.get_cmap("RdBu_r"), 
                    interpolation='nearest')


    for x in range(X):
        for y in range(Y):
            #set_trace()
            ax.annotate(str(len(result[x][y])), xy=(x, y), 
                        horizontalalignment='center',
                        verticalalignment='center')

    cb = fig.colorbar(res)
    plt.xticks(list(range(X)))
    plt.yticks(list(range(Y)))

    annot = ax.annotate("", xy=(0,0), xytext=(20,20),textcoords="offset points",
                        bbox=dict(boxstyle="round", fc="w"))
    annot.set_visible(False)

    def update_annot(x, y):
        annot.xy = (x, y)
        annot.set_text('\n'.join(result[x][y]))
        annot.get_bbox_patch().set_alpha(1.0)

    def hover(event):
        vis = annot.get_visible()
        if event.inaxes == ax:
            cont, ind = fig.contains(event)
            x, y = int(round(event.xdata)), int(round(event.ydata))
            if cont:
                update_annot(x, y)
                annot.set_visible(True)
                fig.canvas.draw_idle()
            else:
                if vis:
                    annot.set_visible(False)
                    fig.canvas.draw_idle()

    fig.canvas.mpl_connect("motion_notify_event", hover)
    plt.show()

