import numpy as np

dataset = np.load("bnc_btc_usdt.npz")
print(dataset.__dict__)
print(dataset['is_trade'][0])
print(dataset['ts'][0])
print()
print("shape", dataset['ts'].shape)
print("dtype", dataset['ts'].dtype)