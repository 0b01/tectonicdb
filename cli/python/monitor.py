from tectonic import TectonicDB
import time

def monitor():
    db = TectonicDB()
    init = int(db.countall()[1])
    while 1:
        time.sleep(1)
        new_count = int(db.countall()[1])
        print(new_count - init)
        init = new_count

if __name__ == '__main__':
    monitor()
