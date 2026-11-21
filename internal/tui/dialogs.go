package tui

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"charm.land/bubbles/v2/textarea"
	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"
	"github.com/byPixelTV/kvstore/internal/console"
)

type dialogKind uint8

const (
	paletteDialog dialogKind = iota
	filterDialog
	setDialog
	batchDialog
	createDialog
	confirmDialog
	secretDialog
)

type field struct {
	label string
	input *textinput.Model
	area  *textarea.Model
	check bool
}

func inputField(label, placeholder, value string, limit int) field {
	input := newInput(placeholder, value, limit)
	return field{label: label, input: &input}
}

func newInput(placeholder, value string, limit int) textinput.Model {
	input := textinput.New()
	input.Prompt = ""
	input.Placeholder = placeholder
	input.CharLimit = limit
	input.SetValue(value)
	return input
}

func areaField(label, placeholder, value string) field {
	area := textarea.New()
	area.Prompt = ""
	area.ShowLineNumbers = false
	area.EndOfBufferCharacter = ' '
	area.Placeholder = placeholder
	area.CharLimit = 384 * 1024
	area.MaxWidth, area.MaxHeight = 0, 0
	area.SetValue(value)
	return field{label: label, area: &area}
}

type dialog struct {
	kind        dialogKind
	title, hint string
	fields      []field
	focus       int
	problem     string
	lines       []string
	confirm     func(*model) tea.Cmd
	completion  []string
	completeAt  int
	secret      string
	copied      bool
}

func (m *model) open(d *dialog) tea.Cmd {
	m.dialog = d
	m.resize()
	return d.focusField()
}

func (d *dialog) focusField() tea.Cmd {
	for _, item := range d.fields {
		if item.input != nil {
			item.input.Blur()
		}
		if item.area != nil {
			item.area.Blur()
		}
	}
	if len(d.fields) == 0 {
		return nil
	}
	item := d.fields[d.focus]
	if item.input != nil {
		return item.input.Focus()
	}
	if item.area != nil {
		return item.area.Focus()
	}
	return nil
}

func (m *model) closeDialog() {
	if m.dialog.kind == filterDialog {
		m.filter, m.keyIndex = m.dialog.hint, 0
	}
	m.dialog.secret = ""
	m.dialog = nil
}

func (m *model) updateDialog(msg tea.Msg) tea.Cmd {
	d := m.dialog
	if key, ok := msg.(tea.KeyPressMsg); ok {
		if key.String() == "esc" {
			m.closeDialog()
			return nil
		}
		switch d.kind {
		case confirmDialog:
			switch key.String() {
			case "y", "Y":
				commit := d.confirm
				m.closeDialog()
				return commit(m)
			case "n", "N":
				m.closeDialog()
			}
			return nil
		case secretDialog:
			switch key.String() {
			case "c":
				if d.secret != "" {
					d.copied = true
					return tea.SetClipboard(d.secret)
				}
			case "enter":
				m.closeDialog()
			}
			return nil
		}
		return m.updateFormKey(key)
	}
	return m.updateField(msg)
}

func (m *model) updateFormKey(key tea.KeyPressMsg) tea.Cmd {
	d := m.dialog
	item := &d.fields[d.focus]
	switch key.String() {
	case "tab", "shift+tab":
		delta := 1
		if key.String() == "shift+tab" {
			delta = -1
		}
		if d.kind == paletteDialog {
			m.complete(delta)
			return nil
		}
		d.focus = (d.focus + delta + len(d.fields)) % len(d.fields)
		return d.focusField()
	case "up", "down":
		if d.kind == paletteDialog {
			delta := 1
			if key.String() == "up" {
				delta = -1
			}
			m.complete(delta)
			return nil
		}
	case "ctrl+s":
		return m.submit()
	case "enter":
		if d.kind <= filterDialog || d.focus == len(d.fields)-1 && item.area == nil {
			return m.submit()
		}
		if item.area == nil {
			d.focus++
			return d.focusField()
		}
	case " ", "space":
		if item.input == nil && item.area == nil {
			item.check = !item.check
			return nil
		}
	}
	return m.updateField(key)
}

func (m *model) updateField(msg tea.Msg) tea.Cmd {
	d := m.dialog
	if len(d.fields) == 0 {
		return nil
	}
	item := d.fields[d.focus]
	if item.area != nil {
		var cmd tea.Cmd
		*item.area, cmd = item.area.Update(msg)
		return cmd
	}
	if item.input == nil {
		return nil
	}
	before := item.input.Value()
	var cmd tea.Cmd
	*item.input, cmd = item.input.Update(msg)
	if after := item.input.Value(); before != after {
		d.completion = nil
		d.problem = ""
		if d.kind == filterDialog {
			m.filter, m.keyIndex = after, 0
		}
	}
	return cmd
}

func (m *model) confirm(title string, lines []string, confirm func(*model) tea.Cmd) tea.Cmd {
	return m.open(&dialog{kind: confirmDialog, title: title, lines: lines, confirm: confirm})
}

func (m *model) palette(value string) tea.Cmd {
	return m.open(&dialog{
		kind: paletteDialog, title: "Commands", fields: []field{inputField("Command", "Type a command…", value, 4096)},
	})
}

func (m *model) openFilter() tea.Cmd {
	return m.open(&dialog{
		kind: filterDialog, title: "Filter loaded keys", hint: m.filter,
		fields: []field{inputField("Contains", "Filter by key name", m.filter, 4096)},
	})
}

func (m *model) openSet(key string) tea.Cmd {
	return m.openValueEditor(key, "", "text", "")
}

func (m *model) openEdit(record console.Record) tea.Cmd {
	value, encoding := string(record.Value), "text"
	if safeText(value, true) != value {
		value, encoding = base64.StdEncoding.EncodeToString(record.Value), "base64"
	}
	var ttl string
	if !record.ExpiresAt.IsZero() {
		ttl = record.ExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	return m.openValueEditor(record.Key, value, encoding, ttl)
}

func (m *model) openValueEditor(key, value, encoding, ttl string) tea.Cmd {
	if !m.canWrite() {
		return nil
	}
	valueField := areaField("Value", "Text, JSON, or encoded bytes", value)
	if valueField.area.Value() != value {
		m.fail("The value is too large for this editor.")
		return nil
	}
	return m.open(&dialog{
		kind: setDialog, title: "Set key", hint: "Namespace  " + safeLine(m.namespace),
		fields: []field{
			inputField("Key", "Exact key", literal(key), 4096),
			valueField,
			inputField("Encoding", "text / hex / base64", encoding, 16),
			inputField("TTL", "e.g. 15m, an RFC3339 timestamp, or blank", ttl, 64),
		},
	})
}

func (m *model) openBatch() tea.Cmd {
	if !m.canWrite() {
		return nil
	}
	return m.open(&dialog{
		kind: batchDialog, title: "Batch write", hint: "Default namespace  " + safeLine(m.namespace),
		fields: []field{areaField("Operations (JSON)", "Paste a JSON array of operations", `[
  {"key": "example", "value": "hello", "ttl": "15m"}
]`)},
	})
}

func (m *model) openCreate() tea.Cmd {
	if !m.canManage() {
		return nil
	}
	return m.open(&dialog{
		kind: createDialog, title: "Create access key",
		fields: []field{
			inputField("Name", "e.g. reporting", "", 256),
			{label: "Read", check: true}, {label: "Write"}, {label: "Manage"},
			inputField("Namespace scope", `Blank for all, or ["inventory"]`, "", 4096),
		},
	})
}

func (m *model) submit() tea.Cmd {
	d := m.dialog
	switch d.kind {
	case paletteDialog:
		return m.runPalette(d.fields[0].input.Value())
	case filterDialog:
		m.dialog = nil
	case setDialog:
		key, err := parseLiteral(d.fields[0].input.Value())
		if err != nil || key == "" {
			d.problem = "Enter an exact key. Use double quotes for escaped keys."
			return nil
		}
		expiresAt, err := expiry(time.Now(), d.fields[3].input.Value())
		if err != nil {
			d.problem = err.Error()
			return nil
		}
		value, err := decodeValue(d.fields[2].input.Value(), d.fields[1].area.Value())
		if err != nil {
			d.problem = err.Error()
			return nil
		}
		m.closeDialog()
		return m.store(console.Record{Namespace: m.namespace, Key: key, Value: value, ExpiresAt: expiresAt})
	case batchDialog:
		operations, err := parseBatch(d.fields[0].area.Value(), m.namespace, time.Now())
		if err != nil {
			d.problem = err.Error()
			return nil
		}
		var deletes int
		for _, op := range operations {
			if !m.canUse(op.Namespace) {
				d.problem = "The access key does not cover all batch namespaces."
				return nil
			}
			if op.Delete {
				deletes++
			}
		}
		lines := []string{
			fmt.Sprintf("%d sets · %d deletes", len(operations)-deletes, deletes),
			"Review the operation results before retrying any change.",
		}
		return m.confirm("Apply batch write", lines, func(m *model) tea.Cmd { return m.sendBatch(operations) })
	case createDialog:
		name := strings.TrimSpace(d.fields[0].input.Value())
		read, write, manage := d.fields[1].check, d.fields[2].check, d.fields[3].check
		if name == "" || !read && !write && !manage {
			d.problem = "Enter a name and select at least one permission."
			return nil
		}
		namespaces, err := parseScope(d.fields[4].input.Value())
		if err != nil {
			d.problem = err.Error()
			return nil
		}
		m.closeDialog()
		return m.createAccessKey(console.AccessKey{Name: name, Read: read, Write: write, Manage: manage, Namespaces: namespaces})
	}
	return nil
}

type paletteAction struct {
	word        string
	description string
	permission  string
}

var actions = []paletteAction{
	{"use", "Switch or enter a namespace", ""},
	{"get", "Fetch an exact key", "read"},
	{"set", "Write a value with optional TTL", "write"},
	{"delete", "Delete an exact key", "write"},
	{"watch", "Stream changes for a namespace or key", "read"},
	{"stop", "Stop the live watch", ""},
	{"batch", "Apply a batch of sets and deletes", "write"},
	{"head", "View cluster status", ""},
	{"keys", "List access keys", "manage"},
	{"create-key", "Issue an access key", "manage"},
	{"revoke-key", "Revoke the selected access key", "manage"},
	{"refresh", "Refresh the current view", ""},
	{"quit", "Close kvctl", ""},
}

func (m *model) matches(value string) []string {
	word, arg, hasArg := strings.Cut(strings.TrimLeft(value, " "), " ")
	if !hasArg {
		var result []string
		for _, action := range actions {
			allowed := action.permission == "" || action.permission == "read" && m.identity.Read || action.permission == "write" && m.identity.Write || action.permission == "manage" && m.identity.Manage
			if allowed && strings.HasPrefix(action.word, strings.ToLower(word)) {
				result = append(result, action.word)
			}
		}
		return result
	}
	var candidates []string
	switch word {
	case "use":
		candidates = m.namespaces
	case "get", "set", "delete", "watch":
		for _, item := range m.pages[m.namespace].items {
			candidates = append(candidates, item.key)
		}
	}
	var result []string
	for _, candidate := range candidates {
		encoded := literal(candidate)
		if strings.HasPrefix(encoded, arg) || strings.HasPrefix(candidate, arg) {
			result = append(result, word+" "+encoded)
		}
	}
	return result
}

func (m *model) complete(delta int) {
	d := m.dialog
	input := d.fields[0].input
	if d.completion == nil {
		value := input.Value()
		if slices.ContainsFunc(actions, func(action paletteAction) bool { return action.word == value }) && value != "" {
			switch value {
			case "use", "get", "set", "delete", "watch":
				input.SetValue(value + " ")
				input.CursorEnd()
				return
			}
		}
		d.completion = m.matches(value)
		d.completeAt = -1
		if delta < 0 {
			d.completeAt = 0
		}
	}
	if len(d.completion) == 0 {
		return
	}
	d.completeAt = (d.completeAt + delta + len(d.completion)) % len(d.completion)
	value := d.completion[d.completeAt]
	input.SetValue(value)
	input.CursorEnd()
	d.problem = ""
}

func (m *model) runPalette(value string) tea.Cmd {
	word, arg, _ := strings.Cut(strings.TrimLeft(value, " "), " ")
	if !slices.ContainsFunc(actions, func(action paletteAction) bool { return action.word == word }) {
		m.dialog.problem = "Type a listed command or press Tab to complete."
		return nil
	}
	arg, err := parseLiteral(arg)
	if err != nil {
		m.dialog.problem = "Close the quoted argument or correct its escape sequence."
		return nil
	}
	if (word == "get" || word == "use" || word == "delete") && arg == "" {
		m.dialog.problem = "Enter a name or press Tab to complete."
		m.dialog.fields[0].input.SetValue(word + " ")
		return nil
	}
	m.closeDialog()
	switch word {
	case "use":
		m.tab = dataTab
		return m.selectNamespace(arg)
	case "get":
		m.tab = dataTab
		return m.get(arg)
	case "set":
		m.tab = dataTab
		return m.openSet(arg)
	case "delete":
		m.tab = dataTab
		return m.confirmDelete(arg)
	case "watch":
		return m.startWatch(arg)
	case "stop":
		m.stopWatch(true)
	case "batch":
		m.tab = dataTab
		return m.openBatch()
	case "head":
		return m.switchTab(clusterTab)
	case "keys":
		return m.switchTab(accessTab)
	case "create-key":
		if m.identity.Manage {
			m.tab = accessTab
		}
		return m.openCreate()
	case "revoke-key":
		if m.identity.Manage {
			m.tab = accessTab
		}
		return m.confirmRevoke()
	case "refresh":
		return m.refresh()
	case "quit":
		return tea.Quit
	}
	return nil
}

func literal(value string) string {
	if value != safeLine(value) || strings.TrimSpace(value) != value || strings.HasPrefix(value, `"`) {
		return strconv.QuoteToASCII(value)
	}
	return value
}

func parseLiteral(value string) (string, error) {
	if strings.HasPrefix(value, `"`) {
		return strconv.Unquote(value)
	}
	return value, nil
}

func expiry(now time.Time, value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, nil
	}
	if timestamp, err := time.Parse(time.RFC3339Nano, value); err == nil {
		if timestamp.After(now) {
			return timestamp, nil
		}
		return time.Time{}, fmt.Errorf("The expiration timestamp must be in the future.")
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration < time.Millisecond {
		return time.Time{}, fmt.Errorf("Enter a TTL of at least 1ms or a future RFC3339 timestamp.")
	}
	return now.Add(duration), nil
}

func decodeValue(encoding, value string) ([]byte, error) {
	switch strings.TrimSpace(strings.ToLower(encoding)) {
	case "", "text":
		return []byte(value), nil
	case "hex":
		decoded, err := hex.DecodeString(strings.TrimSpace(value))
		if err != nil {
			return nil, fmt.Errorf("Value must contain valid hex bytes.")
		}
		return decoded, nil
	case "base64":
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(value))
		if err != nil {
			return nil, fmt.Errorf("Value must contain valid Base64.")
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("Encoding must be text, hex, or base64.")
	}
}

func parseScope(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var namespaces []string
	if err := json.Unmarshal([]byte(raw), &namespaces); err != nil || namespaces == nil {
		return nil, fmt.Errorf(`Enter a JSON array of namespaces, e.g. ["inventory"].`)
	}
	seen := make(map[string]bool, len(namespaces))
	for _, namespace := range namespaces {
		if namespace == "" || seen[namespace] {
			return nil, fmt.Errorf("Namespaces must be nonempty and unique.")
		}
		seen[namespace] = true
	}
	return namespaces, nil
}

func parseBatch(raw, namespace string, now time.Time) ([]console.Mutation, error) {
	var items []struct {
		Namespace string  `json:"namespace"`
		Key       string  `json:"key"`
		Value     *string `json:"value"`
		Hex       *string `json:"hex"`
		TTL       string  `json:"ttl"`
		Delete    bool    `json:"delete"`
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&items) != nil || decoder.Decode(new(any)) != io.EOF || len(items) == 0 || len(items) > 128 {
		return nil, fmt.Errorf("Enter a JSON array of 1–128 operations using namespace, key, value, hex, ttl or delete.")
	}
	operations := make([]console.Mutation, len(items))
	var total int
	for i, item := range items {
		if item.Namespace == "" {
			item.Namespace = namespace
		}
		if item.Namespace == "" || item.Key == "" {
			return nil, fmt.Errorf("Operation %d needs a namespace and key.", i+1)
		}
		if !item.Delete && (item.Value == nil) == (item.Hex == nil) || item.Delete && (item.Value != nil || item.Hex != nil || item.TTL != "") {
			return nil, fmt.Errorf("Operation %d must set value or hex, or specify delete without a value or TTL.", i+1)
		}
		expiresAt, err := expiry(now, item.TTL)
		if err != nil {
			return nil, fmt.Errorf("Operation %d TTL must be at least 1ms.", i+1)
		}
		op := console.Mutation{Namespace: item.Namespace, Key: item.Key, ExpiresAt: expiresAt, Delete: item.Delete}
		if item.Value != nil {
			op.Value = []byte(*item.Value)
		}
		if item.Hex != nil {
			op.Value, err = hex.DecodeString(*item.Hex)
			if err != nil {
				return nil, fmt.Errorf("Operation %d contains invalid hex.", i+1)
			}
		}
		total += len(op.Value)
		operations[i] = op
	}
	if total > 192*1024 {
		return nil, fmt.Errorf("Batch values exceed 192 KiB.")
	}
	return operations, nil
}
