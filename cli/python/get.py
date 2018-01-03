import sys
if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO

from tectonic import TectonicDB
import pandas as pd

def get():
    db = TectonicDB()
    print db.cmd("USE bnc_zrx_btc")[1]
    data = db.cmd("GET ALL FROM 1514764800 TO 1514851200 AS CSV\n")[1]
    # data = db.cmd("GET ALL FROM 1514764800 TO 1514764860 AS CSV\n")[1]
    csv = StringIO("ts,seq,is_trade,is_bid,price,size\n"+data)
    df = pd.read_csv(csv)
    print df


if __name__ == '__main__':
    get()
