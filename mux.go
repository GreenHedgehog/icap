/*
Copyright © 2011, Andy Balholm
All rights reserved.

Based in part on the http package in the Go standard library (© 2009, the Go Authors).

Redistribution and use in source and binary forms, with or without modification, 
are permitted provided that the following conditions are met:

• Redistributions of source code must retain the above copyright notice, 
this list of conditions and the following disclaimer.

• Redistributions in binary form must reproduce the above copyright notice, 
this list of conditions and the following disclaimer 
in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, 
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, 
OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; 
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY 
OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package icap

import (
	"http"
	"path"
	"strings"
)

// ServeMux is an ICAP request multiplexer.
// It matches the URL of each incoming request against a list of registered
// patterns and calls the handler for the pattern that
// most closely matches the URL.
// 
// For more details, see the documentation for http.ServeMux
type ServeMux struct {
	m map[string]Handler
}

// NewServeMux allocates and returns a new ServeMux.
func NewServeMux() *ServeMux { return &ServeMux{make(map[string]Handler)} }

// DefaultServeMux is the default ServeMux used by Serve.
var DefaultServeMux = NewServeMux()

// Does path match pattern?
func pathMatch(pattern, path string) bool {
	if len(pattern) == 0 {
		// should not happen
		return false
	}
	n := len(pattern)
	if pattern[n-1] != '/' {
		return pattern == path
	}
	return len(path) >= n && path[0:n] == pattern
}

// Return the canonical path for p, eliminating . and .. elements.
func cleanPath(p string) string {
	if p == "" {
		return "/"
	}
	if p[0] != '/' {
		p = "/" + p
	}
	np := path.Clean(p)
	// path.Clean removes trailing slash except for root;
	// put the trailing slash back if necessary.
	if p[len(p)-1] == '/' && np != "/" {
		np += "/"
	}
	return np
}

// Find a handler on a handler map given a path string
// Most-specific (longest) pattern wins
func (mux *ServeMux) match(path string) Handler {
	var h Handler
	var n = 0
	for k, v := range mux.m {
		if !pathMatch(k, path) {
			continue
		}
		if h == nil || len(k) > n {
			n = len(k)
			h = v
		}
	}
	return h
}

// ServeICAP dispatches the request to the handler whose
// pattern most closely matches the request URL.
func (mux *ServeMux) ServeICAP(w ResponseWriter, r *Request) {
	// Clean path to canonical form and redirect.
	if p := cleanPath(r.URL.Path); p != r.URL.Path {
		w.Header().Set("Location", p)
		w.WriteHeader(http.StatusMovedPermanently, nil, false)
		return
	}
	// Host-specific pattern takes precedence over generic ones
	h := mux.match(r.URL.Host + r.URL.Path)
	if h == nil {
		h = mux.match(r.URL.Path)
	}
	if h == nil {
		h = NotFoundHandler()
	}
	h.ServeICAP(w, r)
}

// Handle registers the handler for the given pattern.
func (mux *ServeMux) Handle(pattern string, handler Handler) {
	if pattern == "" {
		panic("icap: invalid pattern " + pattern)
	}

	mux.m[pattern] = handler

	// Helpful behavior:
	// If pattern is /tree/, insert permanent redirect for /tree.
	n := len(pattern)
	if n > 0 && pattern[n-1] == '/' {
		mux.m[pattern[0:n-1]] = RedirectHandler(pattern, http.StatusMovedPermanently)
	}
}

// HandleFunc registers the handler function for the given pattern.
func (mux *ServeMux) HandleFunc(pattern string, handler func(ResponseWriter, *Request)) {
	mux.Handle(pattern, HandlerFunc(handler))
}

// Handle registers the handler for the given pattern
// in the DefaultServeMux.
// The documentation for ServeMux explains how patterns are matched.
func Handle(pattern string, handler Handler) { DefaultServeMux.Handle(pattern, handler) }

// HandleFunc registers the handler function for the given pattern
// in the DefaultServeMux.
// The documentation for ServeMux explains how patterns are matched.
func HandleFunc(pattern string, handler func(ResponseWriter, *Request)) {
	DefaultServeMux.HandleFunc(pattern, handler)
}

// NotFound replies to the request with an HTTP 404 not found error.
func NotFound(w ResponseWriter, r *Request) {
	w.WriteHeader(http.StatusNotFound, nil, false)
}

// NotFoundHandler returns a simple request handler
// that replies to each request with a ``404 page not found'' reply.
func NotFoundHandler() Handler { return HandlerFunc(NotFound) }

// Redirect to a fixed URL
type redirectHandler struct {
	url  string
	code int
}

func (rh *redirectHandler) ServeICAP(w ResponseWriter, r *Request) {
	Redirect(w, r, rh.url, rh.code)
}

// RedirectHandler returns a request handler that redirects
// each request it receives to the given url using the given
// status code.
func RedirectHandler(url string, code int) Handler {
	return &redirectHandler{url, code}
}

// Redirect replies to the request with a redirect to url,
// which may be a path relative to the request path.
func Redirect(w ResponseWriter, r *Request, url string, code int) {
	if u, err := http.ParseURL(url); err == nil {
		// If url was relative, make absolute by
		// combining with request path.
		// The browser would probably do this for us,
		// but doing it ourselves is more reliable.
		oldpath := r.URL.Path
		if oldpath == "" { // should not happen, but avoid a crash if it does
			oldpath = "/"
		}
		if u.Scheme == "" {
			// no leading icap://server
			if url == "" || url[0] != '/' {
				// make relative path absolute
				olddir, _ := path.Split(oldpath)
				url = olddir + url
			}

			var query string
			if i := strings.Index(url, "?"); i != -1 {
				url, query = url[:i], url[i:]
			}

			// clean up but preserve trailing slash
			trailing := url[len(url)-1] == '/'
			url = path.Clean(url)
			if trailing && url[len(url)-1] != '/' {
				url += "/"
			}
			url += query
		}
	}

	w.Header().Set("Location", url)
	w.WriteHeader(code, nil, false)
}
