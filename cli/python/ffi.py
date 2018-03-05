import ctypes
from ctypes import *


def __csv_to_df(raw_data):
    csv = StringIO("ts,seq,is_trade,is_bid,price,size\n" + raw_data)
    df = pd.read_csv(csv, dtype={
        'ts': np.float,
        'seq': np.int16,
        'is_trade': np.bool,
        'is_bid': np.bool,
        'price': np.float,
        'size': np.float32}
    )
    df.set_index("ts")
    df = df[:-1]
    df.ts *= 1000
    df.ts = df.ts.astype(int)
    return df


lib = CDLL("target/debug/liblibtectonic.so")

class Update(Structure):
    """
    ts: u64,
    seq: u32,
    is_trade: bool,
    is_bid: bool,
    price: f32,
    size: f32,
    """
    _fields_ = [
        ("ts", c_uint64),
        ("seq", c_uint32),
        ("is_trade", c_bool),
        ("is_bid", c_bool),
        ("price", c_float),
        ("size", c_float),
    ]
    def __repr__(self):
        return 'Update<{},{},{},{},{},{}>'.format(
            self.ts, self.seq, self.is_trade, self.is_bid,
            self.price, self.size)
    def to_dict(self):
        return {
            "ts": self.ts,
            "seq": self.seq,
            "is_trade": self.is_trade,
            "is_bid": self.is_bid,
            "price": self.price,
            "size": self.size
        }

class Slice(Structure):
    _fields_ = [("ptr", POINTER(Update)), ("len", c_uint64)]

def read_dtf_to_csv(fname):
    ptr = lib.read_dtf_to_csv(fname.encode("utf-8"))
    try:
        return ctypes.cast(ptr, c_char_p).value.decode('utf-8')
    finally:
        lib.str_free(ptr)

def read_dtf_to_csv_with_limit(fname, num):
    ptr = lib.read_dtf_to_csv_with_limit(fname.encode("utf-8"), num)
    try:
        return ctypes.cast(ptr, c_char_p).value.decode('utf-8')
    finally:
        lib.str_free(ptr)

def read_dtf_from_file(fname):
    ups = lib.read_dtf_to_arr(fname.encode("utf-8"))
    return [ups.ptr[i] for i in range(ups.len)]

def parse_stream(stream):
    ups = lib.parse_stream(stream, len(stream))
    return [ups.ptr[i] for i in range(ups.len)]

## Type Definitions:

lib.str_free.argtypes = (c_void_p, )

lib.read_dtf_to_csv.argtype = (c_char_p,)
lib.read_dtf_to_csv.restype = c_void_p

lib.read_dtf_to_csv_with_limit.argtype = (c_char_p, c_uint32)
lib.read_dtf_to_csv_with_limit.restype = c_void_p

lib.read_dtf_to_arr.argtype = (c_char_p,)
lib.read_dtf_to_arr.restype = Slice

lib.read_dtf_to_arr_with_limit.argtype = (c_char_p, c_uint32)
lib.read_dtf_to_arr_with_limit.restype = Slice

lib.parse_stream.argtype = (c_char_p, c_uint32)
lib.parse_stream.restype = Slice

async def test_parse_stream():
    from tectonic import TectonicDB
    db = TectonicDB()
    await db.insert(0,0,True,True,0,0,"default")
    await db.insert(1,1,False,False,1,1,"default")
    print(await db.get(2))

def main():
    fname = "/home/g/Desktop/tick-data/10102017/bf_neobtc.dtf"
    data = read_dtf_to_csv_with_limit(fname, 100000)
    df = __csv_to_df(data)
    print(df)

if __name__ == '__main__':
    # from time import time
    # start = time()
    # main()
    # print(time() - start)

    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_parse_stream())
    loop.close()