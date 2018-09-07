package tectonic

import (
	"bytes"
	"fmt"
	"net"
	"strconv"

	"github.com/pquerna/ffjson/ffjson"
)

// Delta : Indicates a change in the orderbook state. This delta struct also works for
// options and other derivative products. The field `IsBid` is overloaded to equal a call
// option if it is true.
type Delta struct {
	Timestamp float64 `json:"ts"`
	Price     float64 `json:"price"`
	Size      float64 `json:"size"`
	Seq       uint32  `json:"seq"`
	IsTrade   bool    `json:"is_trade"`
	IsBid     bool    `json:"is_bid"`
}

// Tectonic : Main type for single-instance connection to the Tectonic database
type Tectonic struct {
	// Connection settings
	Host       string
	Port       uint16 // type ensures port selected is valid
	Connection net.Conn

	// TODO: Create authentication mechanisms in TectonicDB project, then these will be functional
	Username string
	Password string

	CurrentDB       string
	CurrentSymbol   string
	CurrentExchange string
}

// TectonicDB function prototypes
// ****************************
// Help()                                               ( string, error )       done
// Ping()                                               ( string, error )       done
// Info()                                               ( string, error )       done
// Perf()                                               ( string, error )       done
// BulkAdd(ticks *[]Delta)                              error                   done
// BulkAddInto(dbName string, ticks *[]Delta)	        error                   done
// Use(dbName string)                                   error                   done
// Create(dbName string)                                error                   done
// Get(amount int)                                      ( *[]Delta, error )     done
// GetFrom(amount int, dbName string, asTick bool)      ( *[]Delta, error )     done
// Insert(tick *Delta)                                   error                   done
// InsertInto(dbName string, tick *Delta)                error                   done
// Count()                                              uint64                  done
// CountAll()                                           uint64                  done
// Clear()                                              error                   done
// ClearAll()                                           error                   done
// Flush()                                              error                   done
// FlushAll()                                           error                   done
// Subscribe(dbName, message chan string)               error                   incomplete
// Unsubscribe()                                        error                   incomplete
// Exists(dbName string)                                bool                    done
//
// Locally defined methods:
// ****************************
// Connect()        error               done
// SendMessage()    ( string, error )   done
// ****************************

// Pool : TODO
type Pool struct{}

// DefaultTectonic : Default settings for Tectonic structure
var DefaultTectonic = Tectonic{
	Host: "127.0.0.1",
	Port: 9001,
}

// Connect : Connects Tectonic instance to the database. Run to initialize
func (t *Tectonic) Connect() error {
	var (
		connectAddress = fmt.Sprintf("%s:%d", t.Host, t.Port)
		connectErr     error
	)

	t.Connection, connectErr = net.Dial("tcp", connectAddress)

	return connectErr
}

// SendMessage : Sends message to TectonicDB
func (t *Tectonic) SendMessage(message string) (string, error) {
	var readBuf = make([]byte, (1 << 15))

	_, _ = t.Connection.Write([]byte(message + "\n"))
	_, readErr := t.Connection.Read(readBuf)

	return string(readBuf), readErr
}

// Help : Return help string from Tectonic server
func (t *Tectonic) Help() (string, error) {
	return t.SendMessage("HELP")
}

// Ping : Sends a ping message to the TectonicDB server
func (t *Tectonic) Ping() (string, error) {
	return t.SendMessage("PING")
}

// Info : From official documentation: "Returns info about table schemas"
func (t *Tectonic) Info() (string, error) {
	return t.SendMessage("INFO")
}

// Perf : From official documentation: "Returns the answercount of items over time"
func (t *Tectonic) Perf() (string, error) {
	return t.SendMessage("PERF")
}

// BulkAdd : Batch-inserts deltas stored in an array into the TectonicDB server. If you want
// to select what data-store you want to insert the batch into, consider using the function `BulkAddInto`.
func (t *Tectonic) BulkAdd(ticks *[]Delta) error {
	_, _ = t.SendMessage("BULKADD")

	for _, tick := range *ticks {
		var (
			isTrade = "f"
			isBid   = "f"
		)
		if tick.IsTrade {
			isTrade = "t"
		}
		if tick.IsBid {
			isBid = "t"
		}

		_, _ = t.SendMessage(fmt.Sprintf("%.3f, %d, %s, %s, %f, %f;", tick.Timestamp, tick.Seq, isTrade, isBid, tick.Price, tick.Size))
	}

	_, recvErr := t.SendMessage("DDAKLUB")

	return recvErr
}

// BulkAddInto : Batch-inserts deltas stored in an array to the specified store
func (t *Tectonic) BulkAddInto(dbName string, ticks *[]Delta) error {
	_, _ = t.SendMessage("BULKADD INTO " + dbName)

	for _, tick := range *ticks {
		var (
			isTrade = "f"
			isBid   = "f"
		)
		if tick.IsTrade {
			isTrade = "t"
		}
		if tick.IsBid {
			isBid = "t"
		}

		_, _ = t.SendMessage(fmt.Sprintf("%.3f, %d, %s, %s, %f, %f;", tick.Timestamp, tick.Seq, isTrade, isBid, tick.Price, tick.Size))
	}

	_, recvErr := t.SendMessage("DDAKLUB")

	return recvErr
}

// Use : "Switch the current store"
func (t *Tectonic) Use(dbName string) error {
	_, readErr := t.SendMessage("USE " + dbName)

	if readErr == nil {
		t.CurrentDB = dbName
	}

	return readErr
}

// Create : "Create store"
func (t *Tectonic) Create(dbName string) error {
	_, readErr := t.SendMessage("CREATE " + dbName)
	return readErr
}

// Get : "Returns `amount` items from current store"
func (t *Tectonic) Get(amount uint64) (*[]Delta, error) {
	// We use a buffer here to make it easier to maintain
	var (
		msgBuf  = bytes.Buffer{}
		msgJSON = []Delta{}
	)
	msgBuf.WriteString("GET ")
	msgBuf.WriteString(strconv.Itoa(int(amount)))
	msgBuf.WriteString(" AS JSON")

	msgRecv, recvErr := t.SendMessage(msgBuf.String())
	ffjson.Unmarshal(bytes.Trim([]byte(msgRecv[9:]), "\x00"), &msgJSON) // We get back a message starting with `\uFFFE` - Trim that and all null chars in array

	return &msgJSON, recvErr
}

// GetFrom : Returns items from specified store
func (t *Tectonic) GetFrom(dbName string, amount uint64, asTick bool) (*[]Delta, error) {
	// We use a buffer here to make it easier to maintain
	var (
		msgBuf  = bytes.Buffer{}
		msgJSON = []Delta{}
	)
	msgBuf.WriteString("GET ")
	msgBuf.WriteString(strconv.Itoa(int(amount)))
	msgBuf.WriteString(" FROM ")
	msgBuf.WriteString(dbName)
	msgBuf.WriteString(" AS JSON")

	msgRecv, recvErr := t.SendMessage(msgBuf.String())
	ffjson.Unmarshal(bytes.Trim([]byte(msgRecv[9:]), "\x00"), &msgJSON) // We get back a message starting with `\uFFFE` - Trim that and all null chars in array

	return &msgJSON, recvErr
}

// Insert : Inserts a single tick into the currently selected datastore
func (t *Tectonic) Insert(tick *Delta) error {
	var (
		isTrade = "f"
		isBid   = "f"
	)
	if tick.IsTrade {
		isTrade = "t"
	}
	if tick.IsBid {
		isBid = "t"
	}
	tickString := fmt.Sprintf("%.3f, %d, %s, %s, %f, %f;", tick.Timestamp, tick.Seq, isTrade, isBid, tick.Price, tick.Size)

	_, err := t.SendMessage("INSERT " + tickString)

	return err
}

// InsertInto : Inserts a single tick into the datastore specified by `dbName`
func (t *Tectonic) InsertInto(dbName string, tick *Delta) error {
	var (
		isTrade = "f"
		isBid   = "f"
	)
	if tick.IsTrade {
		isTrade = "t"
	}
	if tick.IsBid {
		isBid = "t"
	}
	tickString := fmt.Sprintf("%.3f, %d, %s, %s, %f, %f;", tick.Timestamp, tick.Seq, isTrade, isBid, tick.Price, tick.Size)

	_, err := t.SendMessage("INSERT " + tickString + " INTO " + dbName)

	return err
}

// Count : "Count of items in current store"
func (t *Tectonic) Count() uint64 {
	msg, _ := t.SendMessage("COUNT")
	count, _ := strconv.Atoi(msg)

	return uint64(count)
}

// CountAll : "Returns total count from all stores"
func (t *Tectonic) CountAll() uint64 {
	msg, _ := t.SendMessage("COUNT ALL")
	count, _ := strconv.Atoi(msg)

	return uint64(count)
}

// Clear : Deletes everything in current store (BE CAREFUL WITH THIS METHOD)
func (t *Tectonic) Clear() (string, error) {
	return t.SendMessage("CLEAR")
}

// ClearAll : "Drops everything in memory"
func (t *Tectonic) ClearAll() (string, error) {
	return t.SendMessage("CLEAR ALL")
}

// Flush : "Flush current store to disk"
func (t *Tectonic) Flush() (string, error) {
	return t.SendMessage("FLUSH")
}

// FlushAll : "Flush everything form memory to disk"
func (t *Tectonic) FlushAll() (string, error) {
	return t.SendMessage("FLUSH ALL")
}

// TODO: Implement Subscribe/Unsubscribe. I figure it isn't used *that* much, so we
// can implement these later.
/* Subscribe : Listen to stream of events
 * func (t *Tectonic) Subscribe(dbName, message chan string) (string, error) {
 *
 * }
 *
 * // Unsubscribe : Stop receiving messages from subscription
 * func (t *Tectonic) Unsubscribe() (string, error) {
 *
 * }
 */

// Exists : Checks if datastore exists
func (t *Tectonic) Exists(dbName string) bool {
	msg, _ := t.SendMessage("EXISTS " + dbName)

	// EXISTS command returns `1` for an existing datastore, and `ERR:...` otherwise
	return msg[0] == 1
}
