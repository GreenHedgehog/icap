// Copyright 2011 Andy Balholm. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Network connections and request dispatch for the ICAP server.

package icap

import (
	"bufio"
	"github.com/pkg/errors"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
)

// Objects implementing the Handler interface can be registered
// to serve ICAP requests.
//
// ServeICAP should write reply headers and data to the ResponseWriter
// and then return.
type Handler interface {
	ServeICAP(ResponseWriter, *Request)
}

// The HandlerFunc type is an adapter to allow the use of
// ordinary functions as ICAP handlers.  If f is a function
// with the appropriate signature, HandlerFunc(f) is a
// Handler object that calls f.
type HandlerFunc func(ResponseWriter, *Request)

// ServeICAP calls f(w, r).
func (f HandlerFunc) ServeICAP(w ResponseWriter, r *Request) {
	f(w, r)
}

// A conn represents the server side of an ICAP connection.
type conn struct {
	remoteAddr string            // network address of remote side
	handler    Handler           // request handler
	rwc        net.Conn          // i/o connection
	buf        *bufio.ReadWriter // buffered rwc

	server *Server
}

// Read next request from connection.
func (c *conn) readRequest() (w *respWriter, err error) {
	var req *Request
	if req, err = ReadRequest(c.buf); err != nil {
		return nil, err
	}

	req.RemoteAddr = c.remoteAddr

	w = new(respWriter)
	w.conn = c
	w.req = req
	w.header = make(http.Header)
	return w, nil
}

// Close the connection.
func (c *conn) close() {
	if c.buf != nil {
		c.buf.Flush()
		c.buf = nil
	}
	if c.rwc != nil {
		c.rwc.Close()
		c.rwc = nil
	}
}

// Serve a new connection.
func (c *conn) serve() {
	defer func() {
		if err := recover(); err != nil {
			// var buf bytes.Buffer
			// fmt.Fprintf(&buf, "icap: panic serving %v: %v\n", c.remoteAddr, err)
			// buf.Write(debug.Stack())
			// log.Print(buf.String())
		}
		c.server.untrackConn(c)
		c.close()
	}()

	w, err := c.readRequest()
	if err != nil {
		// log.Println("error while reading request:", err)
		return
	}

	c.handler.ServeICAP(w, w.req)
	w.finishRequest()
}

// A Server defines parameters for running an ICAP server.
type Server struct {
	Addr    string  // TCP address to listen on, ":1344" if empty
	Handler Handler // handler to invoke

	isClosed   int32
	mu         sync.Mutex
	activeConn map[*conn]struct{}
}

// ListenAndServe listens on the TCP network address srv.Addr and then
// calls Serve to handle requests on incoming connections.  If
// srv.Addr is blank, ":1344" is used.
func (srv *Server) ListenAndServe() error {
	addr := srv.Addr
	if addr == "" {
		addr = ":1344"
	}
	l, e := net.Listen("tcp", addr)
	if e != nil {
		return e
	}
	return srv.Serve(l)
}

// Serve accepts incoming connections on the Listener l, creating a
// new service thread for each.  The service threads read requests and
// then call srv.Handler to reply to them.
func (srv *Server) Serve(l net.Listener) error {
	defer l.Close()
	handler := srv.Handler
	if handler == nil {
		handler = DefaultServeMux
	}

	srv.startTracking()

	for {
		if atomic.LoadInt32(&srv.isClosed) == 1 {
			return ErrServerClosed
		}

		rw, e := l.Accept()
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				// log.Printf("icap: Accept error: %v", e)
				continue
			}
			return e
		}

		// c := newConn(rw, handler)
		c := srv.newConn(rw)
		srv.trackConn(c)
		go c.serve()
	}

}

// Serve accepts incoming ICAP connections on the listener l,
// creating a new service thread for each.  The service threads
// read requests and then call handler to reply to them.
func Serve(l net.Listener, handler Handler) error {
	srv := &Server{Handler: handler}
	return srv.Serve(l)
}

// ListenAndServe listens on the TCP network address addr
// and then calls Serve with handler to handle requests
// on incoming connections.
func ListenAndServe(addr string, handler Handler) error {
	server := &Server{Addr: addr, Handler: handler}
	return server.ListenAndServe()
}

/**
		NEW PART
**/

var ErrServerClosed = errors.New("icap: Server closed")

func (srv *Server) Close() {
	atomic.StoreInt32(&srv.isClosed, 1)
	for c := range srv.activeConn {
		c.close()
		delete(srv.activeConn, c)
	}
}

func (srv *Server) startTracking() {
	srv.activeConn = make(map[*conn]struct{})
}

func (srv *Server) trackConn(c *conn) {
	srv.mu.Lock()
	srv.activeConn[c] = struct{}{}
	srv.mu.Unlock()
}

func (srv *Server) untrackConn(c *conn) {
	srv.mu.Lock()
	delete(srv.activeConn, c)
	srv.mu.Unlock()
}

func (srv *Server) newConn(rwc net.Conn) *conn {
	return &conn{
		server:     srv,
		rwc:        rwc,
		handler:    srv.Handler,
		buf:        bufio.NewReadWriter(bufio.NewReader(rwc), bufio.NewWriter(rwc)),
		remoteAddr: rwc.RemoteAddr().String(),
	}
}
