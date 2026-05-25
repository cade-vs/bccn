// bccn.go - minimal BCCN1: send/recv with envelope+body framing.
package bccn

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

// Message is what bccn.Recv returns.
type Message struct {
	Src     string
	Seq     uint64
	Chan    string
	Payload []byte
	Peer    *net.UDPAddr // sender address from recvfrom
}

// Conn is a BCCN endpoint over an already-configured UDP socket.
type Conn struct {
	sock *net.UDPConn
	dst  *net.UDPAddr // destination for outgoing datagrams

	mu  sync.Mutex
	src string
	seq uint64
}

// Open creates a UDP socket bound to <port> on all interfaces,
// configures SO_REUSEADDR, SO_REUSEPORT, and SO_BROADCAST before
// binding, and wires it up for sending to <bcastAddr>:<port>.
// bcastAddr is typically "255.255.255.255" (limited broadcast)
// or a subnet broadcast such as "10.0.0.255".
func Open(bcastAddr string, port int) (*Conn, error) {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var setErr error
			ctrlErr := c.Control(func(fd uintptr) {
				setErr = syscall.SetsockoptInt(int(fd),
					syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
				if setErr != nil {
					return
				}
				// SO_REUSEPORT: 0x0f on Linux. Lets multiple processes
				// on one host all receive every incoming broadcast.
				setErr = syscall.SetsockoptInt(int(fd),
					syscall.SOL_SOCKET, 0x0f, 1)
				if setErr != nil {
					return
				}
				setErr = syscall.SetsockoptInt(int(fd),
					syscall.SOL_SOCKET, syscall.SO_BROADCAST, 1)
			})
			if ctrlErr != nil {
				return ctrlErr
			}
			return setErr
		},
	}

	pc, err := lc.ListenPacket(context.Background(),
		"udp4", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("listen udp4 :%d: %w", port, err)
	}

	sock, ok := pc.(*net.UDPConn)
	if !ok {
		pc.Close()
		return nil, errors.New("ListenPacket did not return *net.UDPConn")
	}

	ip := net.ParseIP(bcastAddr)
	if ip == nil {
		sock.Close()
		return nil, fmt.Errorf("invalid broadcast address %q", bcastAddr)
	}
	dst := &net.UDPAddr{IP: ip, Port: port}

	return New(sock, dst), nil
}

// New wraps an existing *net.UDPConn. The caller is responsible for
// binding, SO_BROADCAST or multicast group join, SO_REUSEADDR, etc.
// dst is the destination address for Send.
func New(sock *net.UDPConn, dst *net.UDPAddr) *Conn {
	return &Conn{
		sock: sock,
		dst:  dst,
		src:  "?",
		seq:  1,
	}
}

// Close closes the underlying socket.
func (c *Conn) Close() error {
	return c.sock.Close()
}

// SetName sets the sender's src ("from" identity). Empty string reverts
// to the unknown-sender placeholder "?".
func (c *Conn) SetName(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if name == "" {
		c.src = "?"
	} else {
		c.src = name
	}
}

// GetName returns the current src.
func (c *Conn) GetName() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.src
}

// SetSeq overrides the outgoing sequence counter.
func (c *Conn) SetSeq(n uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq = n
}

// GetSeq returns the current outgoing sequence value.
func (c *Conn) GetSeq() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seq
}

// Send builds a BCCN1 datagram and transmits it to the configured dst.
func (c *Conn) Send(chanName string, payload []byte) error {
	if strings.ContainsAny(chanName, " \t\r\n|:") {
		return errors.New("chan contains forbidden character")
	}

	c.mu.Lock()
	src := c.src
	seq := c.seq
	c.seq++
	c.mu.Unlock()

	if strings.ContainsAny(src, " \t\r\n|:") {
		return errors.New("src contains forbidden character")
	}

	body := buildBody(src, seq, chanName, payload)
	envelope := buildEnvelope(body)

	n, err := c.sock.WriteToUDP(envelope, c.dst)
	if err != nil {
		return fmt.Errorf("sendto: %w", err)
	}
	if n != len(envelope) {
		return fmt.Errorf("short send: %d/%d", n, len(envelope))
	}
	return nil
}

// Recv blocks until a datagram arrives, parses the envelope and body,
// and returns the message. Malformed datagrams produce an error.
func (c *Conn) Recv() (*Message, error) {
	buf := make([]byte, 1500)
	n, peer, err := c.sock.ReadFromUDP(buf)
	if err != nil {
		return nil, err
	}
	return parse(buf[:n], peer)
}

// -- Framing --

func buildBody(src string, seq uint64, chanName string, payload []byte) []byte {
	hdr := fmt.Sprintf("%s:%d:%s|", src, seq, chanName)
	body := make([]byte, 0, len(hdr)+len(payload))
	body = append(body, hdr...)
	body = append(body, payload...)
	return body
}

func buildEnvelope(body []byte) []byte {
	prefix := "BCCN1[" + strconv.Itoa(len(body)) + "]"
	out := make([]byte, 0, len(prefix)+len(body))
	out = append(out, prefix...)
	out = append(out, body...)
	return out
}

func parse(buf []byte, peer *net.UDPAddr) (*Message, error) {
	// Envelope: BCCN1[<len>]<body>
	const magic = "BCCN1["
	if len(buf) < len(magic) || string(buf[:len(magic)]) != magic {
		return nil, errors.New("bad magic")
	}
	rb := bytesIndex(buf, ']')
	if rb < 0 {
		return nil, errors.New("envelope: no ']'")
	}
	meta := string(buf[len(magic):rb])
	body := buf[rb+1:]

	bodyLen, err := strconv.Atoi(meta)
	if err != nil {
		return nil, fmt.Errorf("envelope: bad length %q: %w", meta, err)
	}
	if bodyLen != len(body) {
		return nil, fmt.Errorf("envelope: length mismatch: header=%d actual=%d",
			bodyLen, len(body))
	}

	// Body: <src>:<seq>:<chan>|<payload>
	pipe := bytesIndex(body, '|')
	if pipe < 0 {
		return nil, errors.New("body: no '|'")
	}
	hdr := string(body[:pipe])
	payload := body[pipe+1:]

	parts := strings.SplitN(hdr, ":", 3)
	if len(parts) != 3 {
		return nil, errors.New("body: header must have three ':'-separated fields")
	}

	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("body: bad seq %q: %w", parts[1], err)
	}

	return &Message{
		Src:     parts[0],
		Seq:     seq,
		Chan:    parts[2],
		Payload: append([]byte(nil), payload...), // copy, buf is reused
		Peer:    peer,
	}, nil
}

func bytesIndex(buf []byte, b byte) int {
	for i, c := range buf {
		if c == b {
			return i
		}
	}
	return -1
}
