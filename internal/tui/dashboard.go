package tui

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	tea "charm.land/bubbletea/v2"
	"github.com/byPixelTV/kvstore/internal/console"
	"github.com/charmbracelet/x/ansi"
)

func (m *model) updateDashboard(key string) tea.Cmd {
	switch key {
	case "ctrl+k", ":", "?":
		return m.palette("")
	case "1":
		return m.switchTab(dataTab)
	case "2":
		return m.switchTab(watchTab)
	case "3":
		return m.switchTab(clusterTab)
	case "4":
		return m.switchTab(accessTab)
	case "tab", "shift+tab":
		n := len(tabNames)
		if !m.identity.Manage {
			n--
		}
		delta := 1
		if key == "shift+tab" {
			delta = n - 1
		}
		return m.switchTab(tab((int(m.tab) + delta) % n))
	case "q":
		return tea.Quit
	case "r":
		return m.refresh()
	}
	switch m.tab {
	case dataTab:
		return m.updateData(key)
	case watchTab:
		return m.updateWatch(key)
	case accessTab:
		switch key {
		case "up", "k":
			m.accessPick = max(0, m.accessPick-1)
		case "down", "j":
			m.accessPick = min(max(0, len(m.accessKeys)-1), m.accessPick+1)
		case "c":
			return m.openCreate()
		case "d", "delete":
			return m.confirmRevoke()
		}
	}
	return nil
}

func (m *model) updateData(key string) tea.Cmd {
	switch key {
	case "n":
		return m.palette("use ")
	case "/":
		return m.openFilter()
	case "esc":
		if m.selected != nil || len(m.batchLog) > 0 {
			m.setSelected(nil)
			m.batchLog = nil
		} else {
			selected := m.currentKey()
			m.filter = ""
			m.restoreSelection(selected)
		}
	case "up", "k":
		m.keyIndex = max(0, m.keyIndex-1)
	case "down", "j":
		m.keyIndex = min(max(0, len(m.visibleKeys())-1), m.keyIndex+1)
	case "home":
		m.keyIndex = 0
	case "end":
		m.keyIndex = max(0, len(m.visibleKeys())-1)
	case "enter":
		return m.get(m.currentKey())
	case "g":
		return m.palette("get ")
	case "s":
		return m.openSet("")
	case "e":
		if m.selected != nil {
			return m.openEdit(*m.selected)
		}
		m.fail("Load a value with Enter or Get before editing it.")
	case "d", "delete":
		return m.confirmDelete(m.currentKey())
	case "w":
		return m.startWatch("")
	case "b":
		return m.openBatch()
	case "m":
		return m.scan(true)
	case "v":
		m.valueMode = (m.valueMode + 1) % valueMode(len(valueModes))
		m.valueTop = 0
		m.prepareValue()
	case "pgup", "pgdown":
		step := max(1, m.detailHeight()-6)
		if key == "pgup" {
			step = -step
		}
		m.valueTop = min(max(0, m.valueTop+step), max(0, len(m.dataLines())-1))
	}
	return nil
}

func (m *model) updateWatch(key string) tea.Cmd {
	switch key {
	case "w":
		return m.palette("watch ")
	case "x", "esc":
		m.stopWatch(true)
	case "pgup", "up", "k":
		m.watch.top = min(max(0, len(m.watch.events)-1), m.watch.top+max(1, m.bodyHeight()-5))
	case "pgdown", "down", "j":
		m.watch.top = max(0, m.watch.top-max(1, m.bodyHeight()-5))
	case "end":
		m.watch.top = 0
	}
	return nil
}

func (m *model) switchTab(next tab) tea.Cmd {
	if next == accessTab && !m.identity.Manage {
		m.fail("This access key does not have management permission.")
		return nil
	}
	m.tab = next
	switch next {
	case clusterTab:
		return m.loadHead()
	case accessTab:
		return m.listAccessKeys()
	}
	return nil
}

func (m *model) refresh() tea.Cmd {
	switch m.tab {
	case dataTab:
		return tea.Batch(m.discover(), m.scan(false))
	case clusterTab:
		return m.loadHead()
	case accessTab:
		return m.listAccessKeys()
	case watchTab:
		m.watch.top = 0
	}
	return nil
}

type valueMode uint8

const (
	textValue valueMode = iota
	jsonValue
	hexValue
)

var valueModes = [...]string{"TEXT", "JSON", "HEX"}

func (m *model) setSelected(record *console.Record) {
	m.selected = record
	m.valueTop = 0
	m.prepareValue()
}

func (m *model) dataLines() []string {
	if len(m.batchLog) != 0 {
		return m.batchLog
	}
	return m.valueLines
}

func (m *model) prepareValue() {
	m.valueLines = nil
	if m.selected == nil {
		return
	}
	value := m.selected.Value
	if len(value) == 0 {
		m.valueLines = []string{"(empty value)"}
		return
	}
	var raw string
	switch m.valueMode {
	case textValue:
		raw = safeText(string(value), true)
	case jsonValue:
		var indented bytes.Buffer
		if err := json.Indent(&indented, value, "", "  "); err != nil {
			m.valueLines = []string{"This value is not valid JSON. Press V to change the view."}
			return
		}
		raw = safeText(indented.String(), true)
	case hexValue:
		raw = strings.TrimSuffix(hex.Dump(value), "\n")
	}
	m.valueLines = strings.Split(ansi.Hardwrap(raw, max(1, m.detailWidth()-4), true), "\n")
	m.valueTop = min(m.valueTop, max(0, len(m.valueLines)-1))
}

func safeLine(value string) string { return safeText(value, false) }

// Values and labels are data, including any ESC, C1, bidi or other formatting
// characters they contain. Render those characters as visible escapes.
func safeText(value string, multiline bool) string {
	var out strings.Builder
	out.Grow(len(value))
	for len(value) > 0 {
		r, size := utf8.DecodeRuneInString(value)
		switch {
		case r == utf8.RuneError && size == 1:
			fmt.Fprintf(&out, `\x%02x`, value[0])
		case r == '\n' && multiline:
			out.WriteByte('\n')
		case unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || unicode.Is(unicode.Zl, r) || unicode.Is(unicode.Zp, r):
			escaped := strconv.QuoteRuneToASCII(r)
			out.WriteString(escaped[1 : len(escaped)-1])
		default:
			out.WriteString(value[:size])
		}
		value = value[size:]
	}
	return out.String()
}
