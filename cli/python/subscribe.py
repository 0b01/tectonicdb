from tectonic import TectonicDB
import time

def subscribe(name):
    db = TectonicDB()
    print db.subscribe(name)

    try:
        while 1:
            _, item = db.poll()
            if item == "NONE\n":
                time.sleep(0.01)
            else:
                print item
    except KeyboardInterrupt:
        db.unsubscribe()
        print "unsubbed"

if __name__ == "__main__":
    subscribe("bnc_bnb_btc")
