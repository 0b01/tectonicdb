# Example program: Plotter

```python
import sys
if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO

from tectonic import TectonicDB
import pandas as pd
import numpy as np
from math import floor, ceil
import copy
import time

from matplotlib.ticker import FormatStrFormatter
import matplotlib.ticker as ticker


class OrderBookPlot(object):
    def __init__(self, plt, market, start, finish):
        self.db = TectonicDB(host="35.196.130.153", port=9001)
        self.db.cmd("USE {}".format(market).encode())[1]
        # self.start = 1515019167 
        # self.finish = 1515022767 
        self.start = start 
        self.finish = finish 

        self.plt = plt

        self.plt.grid(False)
        self.plt.axis('on')
        self.plt.style.use('dark_background')

        fig, ax = self.plt.subplots(figsize=(20, 10), dpi=100)
        # 1 btc = 1e8 satoshi
        ax.yaxis.set_major_formatter(FormatStrFormatter('%.8f'))
        # set x axis as date
        N = 10
        ind = np.arange(N)  # the evenly spaced plot indices
        def format_date(x, pos=None):
            x = int(x/1000.)
            return time.strftime('%m-%d %H:%M:%S', time.localtime(x))
        xfmt = ticker.FuncFormatter(format_date)
        ax.xaxis.set_major_formatter(xfmt)
        self.plt.xlim(self.start * 1000, self.finish * 1000)
        self.plt.title(market)

        self.tick_bins_cnt = 2000
        self.step_bins_cnt = 2000


        data = self.db.cmd("GET ALL FROM {} TO {} AS CSV".format(self.start, self.finish).encode())[1]
        # data = self.db.cmd("GET ALL FROM 1514764800 TO 1514768400 AS CSV\n")[1]
        self.df = self.__csv_to_df(data)
        print(len(self.df))

    def plot_trades(self):
        self.__separate()
        self.__plot_trades()
        

    def plot_pl(self):
        self.prices = np.array(self.df["price"])
        self.rejected = self.__reject_outliers(self.prices, m=4)
        self.updates = self.to_updates(self.df)
        self.__plot_price_levels()

    def plot_ba(self):
        self.ob = self.__get_ob() # expensive
        self.best_ba_df = self.__best_ba() # expensive
        self.__plot_best_ba()

    # def plot_trades(self):


    def to_updates(self, events):
        
        sizes, boundaries = np.histogram(self.rejected, self.tick_bins_cnt)
        def into_tick_bin(price):
            for (s, b) in zip(boundaries, boundaries[1:]):
                if b > price > s:
                    return s
            return False

        min_ts = float(self.df['ts'].min())
        min_ts = int(floor(min_ts))

        max_ts = float(self.df['ts'].max())
        max_ts = int(ceil(max_ts))

        step = (max_ts - min_ts) / float(self.step_bins_cnt)
        step = int(ceil(step))
        step_thresholds = range(min_ts, max_ts, step)

        def into_step_bin(time):
            for (s, b) in zip(step_thresholds, step_thresholds[1:]):
                if s < time and time < b:
                    return b
            return False
            
        updates = {}
        for (_i, row) in self.df.iterrows():
            ts, seq, is_trade, is_bid, price, size = row
            price = into_tick_bin(price)
            time = into_step_bin(ts)
            if not float(price) or not (time):
                continue
            if price not in updates:
                updates[price] = {}
            if time not in updates[price]:
                updates[price][time] = 0
            updates[price][time] += size;
        for time_dict in list(updates.values()):
            for size in list(time_dict.values()):
                if size != 0:
                    time_dict[self.finish * 1000] = size
            
        return updates

    def __reject_outliers(self, data, m = 2.):
        d = np.abs(data - np.median(data))
        mdev = np.median(d)
        s = d/mdev if mdev else 0.
        return data[s<m]

    def __separate(self):
            
        cancelled = []
        created = []
        current_level = {}

        for row in self.df.iterrows():
            _, (ts, seq, is_trade, is_bid, price, size) = row
            if not is_trade:
                prev = current_level[price] if price in current_level else 0
                if (size == 0 or size <= prev):
                    cancelled.append((ts, seq, prev - size, price, is_bid, is_trade))
                elif (size > prev):
                    created.append((ts, seq, size - prev, price, is_bid, is_trade))
                else: # size == prev
                    raise Exception("Impossible")

            current_level[price] = size

        self.cancelled = pd.DataFrame.from_records(cancelled)
        self.created =   pd.DataFrame.from_records(created)
        self.trades = self.df[self.df['is_trade']]

        # sanity check
        assert len(cancelled) + len(created) + len(self.trades) == len(self.df)

    def __csv_to_df(self, raw_data):
        raw_data = str(raw_data, 'utf-8')
        csv = StringIO("ts,seq,is_trade,is_bid,price,size\n" + raw_data)
        df = pd.read_csv(csv, dtype={'ts': np.float, 'seq': np.int16, 'is_trade': np.bool, 'is_bid': np.bool, 'price': np.float, 'size': np.float32})
        df.set_index("ts")
        df = df[:-1]
        df.ts *= 1000
        df.ts = df.ts.astype(int)
        return df

    def __get_ob(self):
        most_recent_orderbook = {"bids": {}, "asks": {}}
        orderbook = {}
        for seq, e in self.df.iterrows():
            if e.is_trade:
                continue
            if e.ts not in orderbook:
                for side, sidedicts in most_recent_orderbook.items():
                    for price, size in sidedicts.items():
                        if size == 0:
                            del sidedicts[price]
                most_recent_orderbook["bids" if e.is_bid else "asks"][e.price] = e["size"]
                orderbook[e.ts] = copy.deepcopy(most_recent_orderbook)        
        return orderbook

    def __best_ba(self):
        best_bids_asks = []

        for ts, ob in self.ob.items():
            try:
                best_bid = max(ob["bids"].keys())
            except: # sometimes L in max(L) is []
                continue
            try:
                best_ask = min(ob["asks"].keys())
            except:
                continue
            best_bids_asks.append((ts, best_bid, best_ask))

        best_bids_asks = pd.DataFrame.from_records(best_bids_asks, columns=["ts", "best_bid", "best_ask"], index="ts").sort_index()
        return best_bids_asks

    def __plot_best_ba(self):
        bhys = []    # bid - horizontal - ys
        bhxmins = [] # bid - horizontal - xmins
        bhxmaxs = [] # ...
        bvxs = []
        bvymins = []
        bvymaxs = []
        ahys = []
        ahxmins = []
        ahxmaxs = []
        avxs = []
        avymins = []
        avymaxs = []

        bba_tuple = self.best_ba_df.to_records()
        for (ts1, b1, a1), (ts2, b2, a2) in zip(bba_tuple, bba_tuple[1:]): # bigram
            bhys.append(b1)
            bhxmins.append(ts1)
            bhxmaxs.append(ts2)
            bvxs.append(ts2)
            bvymins.append(b1)
            bvymaxs.append(b2)
            ahys.append(a1)
            ahxmins.append(ts1)
            ahxmaxs.append(ts2)
            avxs.append(ts2)
            avymins.append(a1)
            avymaxs.append(a2)

        self.plt.hlines(bhys, bhxmins, bhxmaxs, color="green", lw=3, alpha=1)
        self.plt.vlines(bvxs, bvymins, bvymaxs, color="green", lw=3, alpha=1)
        self.plt.hlines(ahys, ahxmins, ahxmaxs, color="red", lw=3, alpha=1)
        self.plt.vlines(avxs, avymins, avymaxs, color="red", lw=3, alpha=1)

    def __plot_price_levels(self, zorder=0, max_threshold=5000, min_threshold=500):    
        ys = []
        xmins = []
        xmaxs = []
        colors = []

        for price, vdict in self.updates.items():
            vtuples = vdict.items()
            vtuples = sorted(vtuples, key=lambda tup: tup[0])
            for (t1, s1), (t2, s2) in zip(vtuples, vtuples[1:]): # bigram
                xmins.append(t1)
                xmaxs.append(t2)
                ys.append(price)
                if s1 < min_threshold:
                    colors.append((0, 0, 0))
                elif s1 > max_threshold:
                    colors.append((0, 1, 1))
                else:
                    colors.append((0, s1/max_threshold, s1/max_threshold))
        self.plt.hlines(ys, xmins, xmaxs, color=colors, lw=3, alpha=1, zorder=zorder)

    def __plot_trades(self, zorder=10):
        max_size = self.trades["size"].max()
        trades_colors = list(map(lambda is_bid: "#ff0000" if is_bid else "#00ff00", self.trades.is_bid))
        self.plt.scatter(self.trades["ts"], self.trades["price"], s=self.trades["size"]/max_size*100000, color=trades_colors, zorder=zorder)

if __name__ == "__main__":
    ob = OrderBookPlot(plt, market, current_time - 60 * int(minutes), current_time)
    ob.plot_pl()
    # ob.plot_ba()
    ob.plot_trades()
    print('done plotting')
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
```

This generates plots like the ones on my [blog post](http://rickyhan.com/jekyll/update/2017/09/24/visualizing-order-book.html).