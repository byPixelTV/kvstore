// Package tui implements the interactive kvctl terminal console.
package tui

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"
	"github.com/byPixelTV/kvstore/internal/clienttls"
	"github.com/byPixelTV/kvstore/internal/console"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	requestTimeout = 10 * time.Second
	scanPageSize   = 100
)

type tab uint8

const (
	dataTab tab = iota
	watchTab
	clusterTab
	accessTab
)

var tabNames = [...]string{"Data", "Watch", "Cluster", "Access Keys"}

type requestSlot uint8

const (
	namespacesSlot requestSlot = iota
	scanSlot
	getSlot
	headSlot
	keysSlot
	writeSlot
	requestCount
)

type requestState struct {
	id      uint64
	pending bool
}

type notice struct {
	text    string
	failure bool
}

type keyItem struct {
	key      string
	revision uint64
}

type keyPage struct {
	items []keyItem
	limit int
	more  bool
}

type model struct {
	ctx      context.Context
	connect  console.Connector
	owner    *sessionOwner
	endpoint console.Endpoint
	width    int
	height   int
	login    loginForm
	joining  bool
	client   console.Client
	identity console.AccessKey
	tab      tab
	status   notice
	requests [requestCount]requestState

	namespaces []string
	namespace  string
	pages      map[string]keyPage
	filter     string
	keyIndex   int
	selected   *console.Record
	valueMode  valueMode
	valueLines []string
	valueTop   int
	batchLog   []string

	watch      watchState
	head       *console.Head
	accessKeys []console.AccessKey
	accessPick int

	dialog *dialog
}

// sessionOwner closes a connection even if the program quits while the login
// command is on its way back to the event loop.
type sessionOwner struct {
	mu     sync.Mutex
	client console.Client
	closed bool
}

func (o *sessionOwner) adopt(client console.Client) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return false
	}
	o.client = client
	return true
}

func (o *sessionOwner) close() {
	o.mu.Lock()
	o.closed = true
	client := o.client
	o.client = nil
	o.mu.Unlock()
	if client != nil {
		_ = client.Close()
	}
}

// Run opens the terminal console. The connector is called after the operator
// enters an access key on the sign-in screen.
func Run(endpoint console.Endpoint, connect console.Connector) error {
	ctx, cancel := context.WithCancel(context.Background())
	m := newModel(ctx, endpoint, connect)
	defer m.owner.close()
	defer cancel()
	_, err := tea.NewProgram(m).Run()
	return err
}

func newModel(ctx context.Context, endpoint console.Endpoint, connect console.Connector) *model {
	m := &model{
		ctx: ctx, endpoint: endpoint, connect: connect, owner: &sessionOwner{},
		width: 80, height: 24, pages: make(map[string]keyPage),
	}
	m.login = newLogin(endpoint)
	m.resize()
	return m
}

func (m *model) Init() tea.Cmd { return m.login.focusField() }

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.resize()
		return m, nil
	case loginResult:
		return m, m.finishLogin(msg)
	case rpcResult:
		return m, m.finishRequest(msg)
	case watchOpened:
		return m, m.receiveWatch(msg)
	case watchUpdate:
		return m, m.receiveEvent(msg)
	case tea.KeyPressMsg:
		if msg.String() == "ctrl+c" {
			return m, tea.Quit
		}
	}
	if m.client == nil {
		return m, m.updateLogin(msg)
	}
	if m.dialog != nil {
		return m, m.updateDialog(msg)
	}
	if key, ok := msg.(tea.KeyPressMsg); ok {
		return m, m.updateDashboard(key.String())
	}
	return m, nil
}

type transportMode uint8

const (
	plaintext transportMode = iota
	systemTLS
	customTLS
)

var transportNames = [...]string{"No TLS", "TLS · system trust", "TLS · custom CA"}

type loginField uint8

const (
	loginAddress loginField = iota
	loginTransport
	loginCA
	loginKey
)

type loginForm struct {
	address, ca, key textinput.Model
	transport        transportMode
	field            loginField
}

func newLogin(endpoint console.Endpoint) loginForm {
	l := loginForm{
		address: newInput("localhost:50051", endpoint.Address, 1024),
		ca:      newInput("Path to a PEM certificate", endpoint.Transport.CA, 4096),
		key:     newInput("Enter access key", "", 512),
		field:   loginKey,
	}
	l.key.EchoMode = textinput.EchoPassword
	l.key.EchoCharacter = '•'
	if endpoint.Transport.CA != "" {
		l.transport = customTLS
	} else if endpoint.Transport.Enabled() {
		l.transport = systemTLS
	}
	return l
}

func (l *loginForm) input() *textinput.Model {
	switch l.field {
	case loginAddress:
		return &l.address
	case loginCA:
		return &l.ca
	case loginKey:
		return &l.key
	}
	return nil
}

func (l *loginForm) focusField() tea.Cmd {
	l.address.Blur()
	l.ca.Blur()
	l.key.Blur()
	if input := l.input(); input != nil {
		return input.Focus()
	}
	return nil
}

func (l *loginForm) step(delta int) tea.Cmd {
	fields := []loginField{loginAddress, loginTransport, loginKey}
	if l.transport == customTLS {
		fields = []loginField{loginAddress, loginTransport, loginCA, loginKey}
	}
	l.field = fields[(slices.Index(fields, l.field)+delta+len(fields))%len(fields)]
	return l.focusField()
}

func (m *model) updateLogin(msg tea.Msg) tea.Cmd {
	if key, ok := msg.(tea.KeyPressMsg); ok {
		if key.String() == "esc" {
			return tea.Quit
		}
		if m.joining {
			return nil
		}
		switch key.String() {
		case "tab":
			return m.login.step(1)
		case "shift+tab":
			return m.login.step(-1)
		case "enter":
			if m.login.field == loginKey {
				return m.beginLogin()
			}
			return m.login.step(1)
		case "left", "right", " ", "space":
			if m.login.field == loginTransport {
				delta := 1
				if key.String() == "left" {
					delta = len(transportNames) - 1
				}
				m.login.transport = transportMode((int(m.login.transport) + delta) % len(transportNames))
				return nil
			}
		}
	}
	if input := m.login.input(); input != nil && !m.joining {
		var cmd tea.Cmd
		*input, cmd = input.Update(msg)
		return cmd
	}
	return nil
}

type loginResult struct {
	client   console.Client
	identity console.AccessKey
	endpoint console.Endpoint
	err      error
}

func (m *model) beginLogin() tea.Cmd {
	address := strings.TrimSpace(m.login.address.Value())
	secret := m.login.key.Value()
	if address == "" || strings.TrimSpace(secret) == "" {
		m.fail("Enter a server address and access key.")
		return nil
	}
	options := m.endpoint.Transport
	options.TLS, options.CA = true, ""
	switch m.login.transport {
	case plaintext:
		options = clienttls.Options{}
	case customTLS:
		options.CA = strings.TrimSpace(m.login.ca.Value())
		if options.CA == "" {
			m.fail("Enter the path to the CA certificate.")
			return nil
		}
	}
	m.login.key.Reset()
	m.joining = true
	m.say("Signing in…")
	endpoint := console.Endpoint{Address: address, Transport: options}
	connect, ctx, owner := m.connect, m.ctx, m.owner
	return func() tea.Msg {
		request, cancel := context.WithTimeout(ctx, requestTimeout)
		defer cancel()
		client, identity, err := connect(request, endpoint, secret)
		if client != nil && (err != nil || !owner.adopt(client)) {
			_ = client.Close()
			client = nil
		}
		return loginResult{client: client, identity: identity, endpoint: endpoint, err: err}
	}
}

func (m *model) finishLogin(result loginResult) tea.Cmd {
	m.joining = false
	if result.err != nil || result.client == nil {
		m.fail(rpcError("Sign in", result.err))
		return m.login.focusField()
	}
	m.client, m.identity, m.endpoint = result.client, result.identity, result.endpoint
	m.say("Connected as " + safeLine(m.identity.Name))
	return m.discover()
}

type rpcResult struct {
	slot  requestSlot
	id    uint64
	label string
	apply func(*model) tea.Cmd
	err   error
}

func (m *model) request(slot requestSlot, label string, run func(context.Context, console.Client) (func(*model) tea.Cmd, error)) tea.Cmd {
	state := &m.requests[slot]
	state.id++
	state.pending = true
	id, ctx, client := state.id, m.ctx, m.client
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(ctx, requestTimeout)
		defer cancel()
		apply, err := run(ctx, client)
		return rpcResult{slot: slot, id: id, label: label, apply: apply, err: err}
	}
}

func (m *model) finishRequest(result rpcResult) tea.Cmd {
	state := &m.requests[result.slot]
	if result.id != state.id {
		return nil
	}
	state.pending = false
	if result.err != nil {
		m.fail(rpcError(result.label, result.err))
		return nil
	}
	return result.apply(m)
}

func (m *model) invalidate(slot requestSlot) {
	m.requests[slot].id++
	m.requests[slot].pending = false
}

func (m *model) say(s string)  { m.status = notice{text: s} }
func (m *model) fail(s string) { m.status = notice{text: s, failure: true} }

// RPC text can contain anything the remote server sends. Use the canonical
// status code and locally written messages so an error can never echo a key.
func rpcError(label string, err error) string {
	detail := "Check the server address, transport settings and connectivity."
	switch status.Code(err) {
	case codes.Unauthenticated:
		detail = "The access key was rejected."
	case codes.PermissionDenied:
		detail = "This access key lacks the required permission."
	case codes.Unavailable:
		detail = "The server is unavailable. Check the address and TLS settings."
	case codes.DeadlineExceeded:
		detail = "The request timed out. Check the current state before retrying a write."
	case codes.AlreadyExists:
		detail = "That name already exists."
	case codes.NotFound:
		detail = "The requested item no longer exists."
	case codes.InvalidArgument:
		detail = "The server rejected the request fields."
	case codes.FailedPrecondition:
		detail = "The server cannot apply this operation in its current state."
	case codes.ResourceExhausted:
		detail = "The request exceeded a server limit."
	case codes.Unimplemented:
		detail = "This operation is unavailable on the connected server."
	case codes.OutOfRange:
		detail = "The watch position expired. Start a new watch."
	case codes.Canceled:
		detail = "The request was canceled."
	}
	return fmt.Sprintf("%s failed. %s", label, detail)
}
