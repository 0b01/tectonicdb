import numpy as np
from matplotlib import pyplot as plt

dataset = np.load("test.npy")
print dataset.shape
print dataset.dtype

plt.imshow(dataset[0].astype(float).transpose(), cmap='hot', interpolation='nearest')
plt.show()
