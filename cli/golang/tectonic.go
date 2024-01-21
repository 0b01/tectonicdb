package tectonic

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

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

const (
	successByte = 0x1
)

var (
	ErrConnection     = errors.New("Connection error")
	ErrFailedWrite    = errors.New("Failed to write to server")
	ErrFailedRead     = errors.New("Failed to read from server")
	ErrFailedReadSize = errors.New("Failed to read response size")
)

type TectonicError struct {
	Message string
}

func (e *TectonicError) Error() string {
	return e.Message
}

func NewTectonicError(message string) *TectonicError {
	return &TectonicError{Message: message}
}

type TectonicClient struct {
	conn      net.Conn
	host      string
	port      string
	currentDB string
}

func NewTectonicClient(host, port string) (*TectonicClient, error) {
	addr := net.JoinHostPort(host, port)
	fmt.Printf("Connecting to %s\n", addr)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, ErrConnection
	}

	return &TectonicClient{
		conn: conn,
		host: host,
		port: port,
	}, nil
}

func (c *TectonicClient) Cmd(command string) (string, error) {
	if err := c.sendCommand(command); err != nil {
		return "", err
	}

	return c.readResponse()
}

func (c *TectonicClient) sendCommand(command string) error {
	writer := bufio.NewWriter(c.conn)

	length := uint32(len(command))
	if err := binary.Write(writer, binary.BigEndian, length); err != nil {
		fmt.Println(err)
		return ErrFailedWrite
	}
	if _, err := writer.WriteString(command); err != nil {
		fmt.Println(err)
		return ErrFailedWrite
	}
	return writer.Flush()
}

func (c *TectonicClient) readResponse() (string, error) {
	successBuf := make([]byte, 1)
	if _, err := c.conn.Read(successBuf); err != nil {
		return "", err
	}
	success := successBuf[0] == successByte

	// Read response size
	var size uint64
	if err := binary.Read(c.conn, binary.BigEndian, &size); err != nil {
		return "", err
	}

	// Read response data
	buf := make([]byte, size)
	if _, err := c.conn.Read(buf); err != nil {
		return "", err
	}

	res := string(buf)
	if success {
		return res, nil
	}

	return "", c.handleServerError(res)
}

func (c *TectonicClient) handleServerError(response string) error {
	if strings.HasPrefix(response, "ERR: DB") {
		// More robust error parsing, handling spaces in bookName
		parts := strings.SplitN(response, " ", 3)
		if len(parts) < 3 {
			return NewTectonicError("DB error without book name")
		}
		bookName := parts[2]
		return NewTectonicError(fmt.Sprintf("DBNotFoundErrors", bookName))
	}
	return NewTectonicError(fmt.Sprintf("ServerErrors", response))
}

// Help : Return help string from Tectonic server
func (t *TectonicClient) Help() (string, error) {
	return t.Cmd("HELP")
}

// Ping : Sends a ping message to the TectonicDB server
func (t *TectonicClient) Ping() (string, error) {
	return t.Cmd("PING")
}

// Info : From official documentation: "Returns info about table schemas"
func (t *TectonicClient) Info() (string, error) {
	return t.Cmd("INFO")
}

// Perf : From official documentation: "Returns the answercount of items over time"
func (t *TectonicClient) Perf() (string, error) {
	return t.Cmd("PERF")
}

// BulkInsert : Batch-inserts deltas stored in an array into the TectonicDB server. If you want
// to select what data-store you want to insert the batch into, consider using the function `BulkAddInto`.
func (t *TectonicClient) BulkInsert(ticks []*Delta, dbName string) error {
	//_, _ = t.Cmd("BULKADD ")

	for _, tick := range ticks {
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

		if _, err := t.Cmd(fmt.Sprintf("INSERT %.3f, %d, %s, %s, %f, %f; INTO %s", tick.Timestamp, tick.Seq, isTrade, isBid, tick.Price, tick.Size, dbName)); err != nil {
			fmt.Println(err)
		}
	}

	return nil
}

// BulkAdd : Batch-add deltas stored in an array to the specified store
func (t *TectonicClient) BulkAdd(dbName string, ticks []*Delta) error {
	if t.currentDB != dbName {
		t.Use(dbName)
	}
	for _, tick := range ticks {
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

		if _, err := t.Cmd(fmt.Sprintf("ADD %.3f, %d, %s, %s, %f, %f;", tick.Timestamp, tick.Seq, isTrade, isBid, tick.Price, tick.Size)); err != nil {
			fmt.Println(err)
			return err
		}
	}

	return nil
}

// Use : "Switch the current store"
func (t *TectonicClient) Use(dbName string) error {
	_, readErr := t.Cmd("USE " + dbName)

	if readErr == nil {
		t.currentDB = dbName
	}

	return readErr
}

// Create : "Create store"
func (t *TectonicClient) Create(dbName string) error {
	_, readErr := t.Cmd("CREATE " + dbName)
	return readErr
}

// Get : "Returns `amount` items from current store"
func (t *TectonicClient) Get(amount uint64) ([]*Delta, error) {
	// We use a buffer here to make it easier to maintain
	var (
		msgBuf  = bytes.Buffer{}
		msgJSON []*Delta
	)
	msgBuf.WriteString("GET ")
	msgBuf.WriteString(strconv.Itoa(int(amount)))
	msgBuf.WriteString(" AS JSON")

	msgRecv, recvErr := t.Cmd(msgBuf.String())
	fmt.Println(msgRecv, recvErr)
	if recvErr != nil {
		return nil, recvErr
	}
	msgRecv = "[" + msgRecv + "]"
	if err := ffjson.Unmarshal(bytes.Trim([]byte(msgRecv), "\x00"), &msgJSON); err != nil {
		fmt.Println(err)
	} // We get back a message starting with `\uFFFE` - Trim that and all null chars in array

	return msgJSON, recvErr
}

// GetFrom : Returns items from specified store
func (t *TectonicClient) GetFrom(dbName string, amount uint64) ([]Delta, error) {
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

	msgRecv, recvErr := t.Cmd(msgBuf.String())
	if recvErr != nil {
		return nil, recvErr
	}
	msgRecv = "[" + msgRecv + "]"
	if err := ffjson.Unmarshal(bytes.Trim([]byte(msgRecv), "\x00"), &msgJSON); err != nil {
		fmt.Println(err)
	}

	return msgJSON, nil
}

// Insert : Inserts a single tick into the currently selected datastore
func (t *TectonicClient) Insert(tick *Delta) error {
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

	_, err := t.Cmd("INSERT " + tickString)

	return err
}

// InsertInto : Inserts a single tick into the datastore specified by `dbName`
func (t *TectonicClient) InsertInto(dbName string, tick *Delta) error {
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

	_, err := t.Cmd("INSERT " + tickString + " INTO " + dbName)

	return err
}

// Count : "Count of items in current store"
func (t *TectonicClient) Count() uint64 {
	msg, _ := t.Cmd("COUNT")
	count, _ := strconv.Atoi(msg)

	return uint64(count)
}

// CountAll : "Returns total count from all stores"
func (t *TectonicClient) CountAll() uint64 {
	msg, _ := t.Cmd("COUNT ALL")
	count, _ := strconv.Atoi(msg)

	return uint64(count)
}

// Clear : Deletes everything in current store (BE CAREFUL WITH THIS METHOD)
func (t *TectonicClient) Clear() (string, error) {
	return t.Cmd("CLEAR")
}

// ClearAll : "Drops everything in memory"
func (t *TectonicClient) ClearAll() (string, error) {
	return t.Cmd("CLEAR ALL")
}

// Flush : "Flush current store to disk"
func (t *TectonicClient) Flush() (string, error) {
	return t.Cmd("FLUSH")
}

// FlushAll : "Flush everything form memory to disk"
func (t *TectonicClient) FlushAll() (string, error) {
	return t.Cmd("FLUSH ALL")
}

// Exists : Checks if datastore exists
func (t *TectonicClient) Exists(dbName string) bool {
	msg, _ := t.Cmd("EXISTS " + dbName)

	// EXISTS command returns `1` for an existing datastore, and `ERR:...` otherwise
	return msg[0] == 1
}

func main() {
	// Example use
	client, err := NewTectonicClient("localhost", "9001")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.conn.Close()

	response, err := client.Cmd("HELP")
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(response)
	}
	fmt.Println(client.Exists("test"))
	fmt.Println(client.Info())
	fmt.Println(client.Count())
	fmt.Println(client.Count())

	deltas := make([]*Delta, 0, 100)
	for i := 0; i < 100; i++ {
		d := &Delta{
			Timestamp: float64(time.Now().UnixMilli()) / 1000,
			Price:     2000 + float64(i),
			Size:      100 + float64(i),
			Seq:       uint32(i),
			IsBid:     true,
			IsTrade:   false,
		}
		deltas = append(deltas, d)
	}
	if err := client.BulkAdd("test", deltas); err != nil {
		fmt.Println(err)
	}

}
