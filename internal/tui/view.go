package tui

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/byPixelTV/kvstore/internal/console"
	"github.com/charmbracelet/x/ansi"
)

var (
	mint     = lipgloss.Color("#68E1C1")
	lavender = lipgloss.Color("#C2BDFA")
	pale     = lipgloss.Color("#EEF1FA")
	muted    = lipgloss.Color("#A2ACC5")
	line     = lipgloss.Color("#62739B")
	navy     = lipgloss.Color("#17233A")
	backdrop = lipgloss.Color("#0C1424")
	errorInk = lipgloss.Color("#FFAB9B")

	strong = lipgloss.NewStyle().Foreground(pale).Bold(true)
	quiet  = lipgloss.NewStyle().Foreground(muted)
	accent = lipgloss.NewStyle().Foreground(mint).Bold(true)
	alert  = lipgloss.NewStyle().Foreground(errorInk).Bold(true)
	badge  = lipgloss.NewStyle().Foreground(navy).Background(mint).Bold(true)
	active = lipgloss.NewStyle().Foreground(pale).Background(navy).Bold(true)
	group  = lipgloss.NewStyle().Foreground(lavender).Bold(true)
)

const (
	minWidth      = 26
	minHeight     = 10
	widePanes     = 78
	verticalPanes = 20
)

func (m *model) View() tea.View {
	var content string
	switch {
	case m.width < minWidth || m.height < minHeight:
		content = crop("Resize terminal to continue", max(1, m.width))
	case m.client == nil:
		content = m.loginView()
	default:
		content = m.dashboardView()
	}
	view := tea.NewView(content)
	view.AltScreen = true
	view.WindowTitle = "kvctl | KVStore"
	if os.Getenv("NO_COLOR") == "" {
		view.BackgroundColor = backdrop
		view.ForegroundColor = pale
	}
	return view
}

func (m *model) contentWidth() int {
	if m.width >= 42 {
		return m.width - 4
	}
	return max(1, m.width-2)
}

func (m *model) bodyHeight() int { return max(5, m.height-6) }

func (m *model) modalWidth() int { return min(m.contentWidth(), 72) }

func (m *model) detailWidth() int {
	w := m.contentWidth()
	if w >= widePanes {
		return w - m.listWidth() - 1
	}
	return w
}

func (m *model) listWidth() int { return min(44, m.contentWidth()*36/100) }

func (m *model) detailHeight() int {
	if m.contentWidth() < widePanes && m.bodyHeight() >= verticalPanes {
		return m.bodyHeight() - m.bodyHeight()/3
	}
	return m.bodyHeight()
}

func (m *model) resize() {
	loginInputWidth := max(4, min(m.contentWidth(), 69)-6)
	m.login.address.SetWidth(loginInputWidth)
	m.login.ca.SetWidth(loginInputWidth)
	m.login.key.SetWidth(loginInputWidth)
	if d := m.dialog; d != nil {
		for _, item := range d.fields {
			if item.input != nil {
				item.input.SetWidth(max(4, m.modalWidth()-6))
			}
			if item.area != nil {
				item.area.SetWidth(max(4, m.modalWidth()-6))
				height := 6
				if d.kind == batchDialog {
					height = 12
				}
				item.area.SetHeight(min(height, max(2, m.bodyHeight()-10)))
			}
		}
	}
	m.prepareValue()
}

func (m *model) loginView() string {
	w := min(m.contentWidth(), 69)
	l := m.login
	rows := []string{brand(), "", strong.Render("Sign in to KVStore"), ""}
	rows = append(rows, inputRows("Server address", l.address, l.field == loginAddress)...)
	transport := transportNames[l.transport]
	if l.field == loginTransport {
		transport = "‹ " + transport + " ›"
	}
	rows = append(rows, fieldLabel("Transport", l.field == loginTransport), "  "+group.Render(transport))
	if l.transport == customTLS {
		rows = append(rows, inputRows("CA certificate", l.ca, l.field == loginCA)...)
	}
	rows = append(rows, inputRows("Access key", l.key, l.field == loginKey)...)
	rows = append(rows, "", hotkeys("Enter", "sign in", "Tab", "change field", "← →", "transport", "Esc", "quit"))
	if m.status.text != "" {
		rows = append(rows, m.statusLine())
	}
	if m.height < len(rows)+2 {
		// Keep the focused control and feedback visible on short terminals.
		rows = []string{brand()}
		switch l.field {
		case loginTransport:
			rows = append(rows, fieldLabel("Transport", true), "  "+group.Render(transport))
		case loginAddress:
			rows = append(rows, inputRows("Server address", l.address, true)...)
		case loginCA:
			rows = append(rows, inputRows("CA certificate", l.ca, true)...)
		case loginKey:
			rows = append(rows, inputRows("Access key", l.key, true)...)
		}
		rows = append(rows, hotkeys("Enter", "continue", "Tab", "fields", "← →", "transport"), m.statusLine())
	}
	command := "kvstore apikey create --dir ... --name admin --manage --read --write"
	hint := ansi.Hardwrap("Create the first manager locally on the KVStore host with", w-4, false)
	hint += "\n" + ansi.Hardwrap(command, w-4, false)
	hintRows := strings.Split(hint, "\n")
	if m.height >= len(rows)+len(hintRows)+3 {
		rows = append(rows, "")
		for _, row := range hintRows {
			rows = append(rows, quiet.Render(row))
		}
	}
	card := panel(w, 0, rows, true)
	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, card)
}

func brand() string { return badge.Render(" kvctl ") + "  " + strong.Render("KVSTORE CONSOLE") }

func (m *model) dashboardView() string {
	w := m.contentWidth()
	first := brand()
	if w >= 64 {
		target := quiet.Render(safeLine(m.endpoint.Address))
		first = between(first, accent.Render("●")+"  "+target, w)
	}
	tabs := make([]string, 0, len(tabNames))
	for i, label := range tabNames {
		if tab(i) == accessTab && !m.identity.Manage {
			continue
		}
		if w < 48 && tab(i) == accessTab {
			label = "Keys"
		}
		if w >= 46 {
			label = strconv.Itoa(i+1) + " " + label
		}
		if m.tab == tab(i) {
			tabs = append(tabs, badge.Render(" "+label+" "))
		} else {
			tabs = append(tabs, quiet.Render(label))
		}
	}
	tabRow := strings.Join(tabs, "   ")
	if w < 48 {
		tabRow = strings.Join(tabs, " ")
	}
	var body string
	switch {
	case m.dialog != nil:
		body = m.dialogView()
	case m.tab == dataTab:
		body = m.dataView()
	case m.tab == watchTab:
		body = m.watchView()
	case m.tab == clusterTab:
		body = m.clusterView()
	case m.tab == accessTab:
		body = m.accessView()
	}
	status := m.statusLine()
	if w >= 84 {
		transport := "no TLS"
		if m.endpoint.Transport.Enabled() {
			transport = "TLS"
		}
		session := quiet.Render(safeLine(m.identity.Name) + "  ·  " + transport)
		status = between(status, session, w)
	}
	parts := []string{first, tabRow, "", body, "", m.shortcuts(), status}
	return strings.Join(mapRows(strings.Join(parts, "\n"), func(s string) string {
		return strings.Repeat(" ", (m.width-w)/2) + crop(s, w)
	}), "\n")
}

func (m *model) dataView() string {
	w, h := m.contentWidth(), m.bodyHeight()
	if w >= widePanes {
		return lipgloss.JoinHorizontal(lipgloss.Top, m.keyList(m.listWidth(), h), " ", m.detail(m.detailWidth(), h))
	}
	if h >= verticalPanes {
		return m.keyList(w, h/3) + "\n" + m.detail(w, m.detailHeight())
	}
	if m.selected != nil || len(m.batchLog) != 0 || m.requests[getSlot].pending {
		return m.detail(w, h)
	}
	return m.keyList(w, h)
}

func (m *model) keyList(w, h int) string {
	rows := []string{group.Render("NAMESPACE")}
	if h <= 6 {
		rows[0] += "  " + strong.Render(safeLine(m.namespace))
	} else if m.namespace == "" {
		rows = append(rows, quiet.Render("Press N to choose a namespace"))
	} else {
		rows = append(rows, strong.Render(safeLine(m.namespace)))
	}
	if m.namespace == "" && m.requests[namespacesSlot].pending {
		rows = append(rows, quiet.Render("Finding namespaces…"))
	}
	if m.namespace == "" || !m.identity.Read {
		if m.namespace != "" && !m.identity.Read {
			rows = append(rows, quiet.Render("Read permission required."))
		}
		return panel(w, h, rows, false)
	}
	visible, page := m.visibleKeys(), m.pages[m.namespace]
	count := fmt.Sprintf("%d keys", len(page.items))
	if m.filter != "" {
		count = fmt.Sprintf("%d of %d  ·  /%s", len(visible), len(page.items), safeLine(m.filter))
	}
	if page.more {
		count = "M load more   " + count
	}
	if m.requests[scanSlot].pending {
		count = "Loading keys…"
	}
	rows = append(rows, quiet.Render(count))
	if len(visible) == 0 && !m.requests[scanSlot].pending {
		empty := "No keys in this namespace."
		if m.filter != "" {
			empty = "No loaded keys match."
		}
		rows = append(rows, quiet.Render(empty))
	}
	room := max(0, h-len(rows)-2)
	start := min(max(0, m.keyIndex-room/2), max(0, len(visible)-room))
	for i := start; i < min(len(visible), start+room); i++ {
		item := visible[i]
		label := safeLine(item.key)
		if i == m.keyIndex {
			rows = append(rows, active.Render(pad("› "+label, w-4)))
		} else {
			rows = append(rows, "  "+label)
		}
	}
	return panel(w, h, rows, true)
}

func (m *model) detail(w, h int) string {
	rows := []string{between(group.Render("VALUE"), modeBadges(m.valueMode), w-4)}
	switch {
	case m.requests[getSlot].pending:
		rows = append(rows, "", quiet.Render("Fetching value…"))
	case len(m.batchLog) > 0:
		rows = []string{group.Render("BATCH RESULTS"), ""}
		rows = appendPage(rows, m.batchLog, m.valueTop, h-2)
	case m.selected != nil:
		r := m.selected
		rows = append(rows, strong.Render(safeLine(r.Key)))
		attributes := fmt.Sprintf("%d bytes  ·  revision %d", len(r.Value), r.Revision)
		if !r.ExpiresAt.IsZero() {
			attributes += "  ·  expires " + r.ExpiresAt.Local().Format("2006-01-02 15:04:05 MST")
		}
		rows = append(rows, quiet.Render(attributes), "")
		rows = appendPage(rows, m.valueLines, m.valueTop, h-2)
	default:
		rows = append(rows, "", strong.Render("No value selected"), quiet.Render("Press Enter on a key or G for exact lookup."))
	}
	return panel(w, h, rows, false)
}

func modeBadges(mode valueMode) string {
	var labels []string
	for i, label := range valueModes {
		if mode == valueMode(i) {
			labels = append(labels, accent.Render(label))
		} else {
			labels = append(labels, quiet.Render(label))
		}
	}
	return strings.Join(labels, "  ")
}

func (m *model) watchView() string {
	state, caption := "IDLE", "Press W to watch the current namespace or an exact key."
	if m.watch.namespace != "" {
		caption = safeLine(m.watch.namespace) + " / all keys"
		if m.watch.key != "" {
			caption = safeLine(m.watch.namespace) + " / " + safeLine(m.watch.key)
		}
		state = "STOPPED"
		if m.watch.cancel != nil {
			state = "LIVE"
		}
		if m.watch.starting {
			state = "CONNECTING"
		}
	}
	w, h := m.contentWidth(), m.bodyHeight()
	rows := []string{between(group.Render("CHANGE STREAM"), accent.Render(state), w-4), quiet.Render(caption)}
	if m.watch.cancel != nil && len(m.watch.events) == 0 {
		rows = append(rows, "", quiet.Render("Waiting for changes…"))
	}
	if len(m.watch.events) > 0 {
		rows = append(rows, quiet.Render(fmt.Sprintf("%d events", m.watch.total)), "")
		room := max(0, h-len(rows)-2)
		last := max(0, len(m.watch.events)-m.watch.top)
		rows = append(rows, m.watch.events[max(0, last-room):last]...)
	}
	return panel(w, h, rows, true)
}

func (m *model) clusterView() string {
	w, h := m.contentWidth(), m.bodyHeight()
	rows := []string{group.Render("CLUSTER STATUS"), ""}
	if m.requests[headSlot].pending {
		rows = append(rows, quiet.Render("Checking the cluster head…"))
	} else if m.head != nil {
		if m.head.Cursor != "" {
			rows = append(rows, strong.Render("Multi-shard cursor"), quiet.Render(safeLine(m.head.Cursor)), "")
		} else {
			rows = append(rows, fmt.Sprintf("%-20s %d", "Head revision", m.head.Revision), fmt.Sprintf("%-20s %d", "Oldest revision", m.head.OldestRevision), "")
		}
		expiration := "Unavailable"
		if m.head.SupportsExpiration {
			expiration = "Supported"
		}
		rows = append(rows, "Expiration  "+strong.Render(expiration))
	} else {
		rows = append(rows, quiet.Render("Press R to refresh."))
	}
	return panel(w, h, rows, true)
}

func (m *model) accessView() string {
	w, h := m.contentWidth(), m.bodyHeight()
	rows := []string{between(group.Render("ACCESS KEYS"), quiet.Render(fmt.Sprintf("%d keys", len(m.accessKeys))), w-4)}
	if m.requests[keysSlot].pending {
		rows = append(rows, quiet.Render("Loading access keys…"))
	} else if len(m.accessKeys) == 0 {
		rows = append(rows, "", quiet.Render("Press C to create an access key."))
	}
	room := max(0, h-len(rows)-2)
	stride := 2
	if room < 4 {
		stride = 1
	}
	room /= stride
	start := min(max(0, m.accessPick-room/2), max(0, len(m.accessKeys)-room))
	for i := start; i < min(len(m.accessKeys), start+room); i++ {
		key := m.accessKeys[i]
		name := safeLine(key.Name)
		if key.Name == m.identity.Name {
			name += "  (current session)"
		}
		entry := between("  "+name, roles(key), w-4)
		if i == m.accessPick {
			entry = active.Render("›" + entry[1:])
		}
		rows = append(rows, entry)
		if stride == 2 {
			scope := "All namespaces"
			if len(key.Namespaces) != 0 {
				scope = safeLine(strings.Join(key.Namespaces, ", "))
			}
			rows = append(rows, quiet.Render("    "+scope))
		}
	}
	return panel(w, h, rows, true)
}

func roles(key console.AccessKey) string {
	var labels []string
	if key.Read {
		labels = append(labels, "READ")
	}
	if key.Write {
		labels = append(labels, "WRITE")
	}
	if key.Manage {
		labels = append(labels, "MANAGE")
	}
	return strings.Join(labels, " · ")
}

func (m *model) dialogView() string {
	d, w, area := m.dialog, m.modalWidth(), m.bodyHeight()
	rows := []string{strong.Render(d.title)}
	switch d.kind {
	case confirmDialog:
		rows = append(rows, d.lines...)
		rows = append(rows, "", hotkeys("Y", "confirm", "N / Esc", "cancel"))
	case secretDialog:
		rows = append(rows, quiet.Render(d.hint), "", alert.Render("Save this secret before closing."))
		for _, s := range strings.Split(ansi.Hardwrap(safeLine(d.secret), w-4, true), "\n") {
			rows = append(rows, accent.Render(s))
		}
		if d.copied {
			rows = append(rows, quiet.Render("Clipboard requested. Terminal support is required."))
		}
		rows = append(rows, hotkeys("C", "copy", "Enter / Esc", "close"))
	default:
		rows = m.formRows(rows, w, area)
	}
	return lipgloss.Place(m.contentWidth(), area, lipgloss.Center, lipgloss.Center, panel(w, min(area, len(rows)+2), rows, true))
}

func (m *model) formRows(rows []string, w, area int) []string {
	d := m.dialog
	if d.hint != "" && d.kind != filterDialog && area >= 12 {
		rows = append(rows, quiet.Render(d.hint))
	}
	var fields []string
	for i, item := range d.fields {
		focused := i == d.focus
		if item.input != nil {
			fields = append(fields, inputRows(item.label, *item.input, focused)...)
		} else if item.area != nil {
			fields = append(fields, fieldLabel(item.label, focused))
			fields = append(fields, mapRows(item.area.View(), func(row string) string { return "  " + row })...)
		} else {
			fields = append(fields, checkRow(item, focused))
		}
	}
	help := hotkeys("Tab", "next field", "Ctrl+S", "submit", "Esc", "cancel")
	if d.kind == createDialog {
		help = hotkeys("Tab", "field", "Space", "toggle", "Ctrl+S", "create", "Esc", "cancel")
	}
	if d.kind == filterDialog {
		help = hotkeys("Enter", "apply", "Esc", "cancel")
	}
	if d.kind == paletteDialog {
		help = hotkeys("Tab / ↑↓", "complete", "Enter", "run", "Esc", "cancel")
		fields = append(fields, m.paletteHints(w, area-len(rows)-len(fields)-4)...)
	}
	var tail []string
	if d.problem != "" {
		tail = append(tail, alert.Render(d.problem))
	}
	if area >= 7 || len(tail) == 0 {
		tail = append(tail, help)
	}
	if limit := area - len(rows) - len(tail) - 2; len(fields) > limit {
		item := d.fields[d.focus]
		switch {
		case item.input != nil:
			fields = inputRows(item.label, *item.input, true)
		case item.area != nil:
			fields = append([]string{fieldLabel(item.label, true)}, mapRows(item.area.View(), func(s string) string { return "  " + s })...)
		default:
			fields = []string{checkRow(item, true)}
		}
		if limit == 1 && item.input != nil {
			fields = fields[1:]
		}
		fields = fields[:min(len(fields), max(0, limit))]
	}
	return append(append(rows, fields...), tail...)
}

func (m *model) paletteHints(w, maxRows int) []string {
	d := m.dialog
	value := d.fields[0].input.Value()
	matches := d.completion
	if matches == nil {
		matches = m.matches(value)
	}
	maxRows = max(0, min(8, maxRows))
	start := min(max(0, d.completeAt-maxRows/2), max(0, len(matches)-maxRows))
	rows := make([]string, 0, maxRows)
	for i := start; i < min(len(matches), start+maxRows); i++ {
		item := matches[i]
		row := "  " + item
		if i == d.completeAt && len(d.completion) > 0 {
			row = accent.Render("› " + item)
		}
		if !strings.Contains(item, " ") && w >= 46 {
			for _, action := range actions {
				if action.word == item {
					row = pad(row, 17) + quiet.Render(action.description)
					break
				}
			}
		}
		rows = append(rows, row)
	}
	return rows
}

func (m *model) statusLine() string {
	if m.requests[writeSlot].pending {
		return quiet.Render("●  Applying change…")
	}
	if m.status.failure {
		return alert.Render("!  " + m.status.text)
	}
	if m.status.text != "" {
		return accent.Render("●") + "  " + quiet.Render(m.status.text)
	}
	return ""
}

func (m *model) shortcuts() string {
	w := m.contentWidth()
	if m.dialog != nil {
		if m.dialog.kind == secretDialog {
			return quiet.Render("This secret is shown once.")
		}
		return ""
	}
	var pairs []string
	switch m.tab {
	case dataTab:
		pairs = []string{"N", "namespace", "/", "filter", "Enter", "open", "G", "get", "V", "view", "Ctrl+K", "commands"}
		if w < 76 {
			pairs = []string{"N", "namespace", "/", "filter", "Ctrl+K", "commands"}
		}
	case watchTab:
		pairs = []string{"W", "watch", "X", "stop", "PgUp / PgDn", "scroll", "Ctrl+K", "commands"}
	case clusterTab:
		pairs = []string{"R", "refresh", "Ctrl+K", "commands"}
	case accessTab:
		pairs = []string{"C", "create", "D", "revoke", "R", "refresh", "Ctrl+K", "commands"}
	}
	if w < 42 {
		return hotkeys("Ctrl+K", "commands", pairs[0], pairs[1])
	}
	return hotkeys(pairs...)
}

func inputRows(label string, input textinput.Model, selected bool) []string {
	return []string{fieldLabel(label, selected), "  " + input.View()}
}

func checkRow(item field, selected bool) string {
	mark := "[ ]"
	if item.check {
		mark = "[x]"
	}
	return fieldLabel(mark+" "+item.label, selected)
}

func fieldLabel(label string, selected bool) string {
	if selected {
		return accent.Render("› " + label)
	}
	return quiet.Render("  " + label)
}

func hotkeys(pairs ...string) string {
	items := make([]string, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		items = append(items, group.Render(pairs[i])+" "+quiet.Render(pairs[i+1]))
	}
	return strings.Join(items, "  ·  ")
}

func panel(w, h int, rows []string, focused bool) string {
	if h > 0 {
		rows = rows[:min(len(rows), h-2)]
		for len(rows) < h-2 {
			rows = append(rows, "")
		}
	}
	for i := range rows {
		rows[i] = pad(rows[i], w-4)
	}
	border := line
	if focused {
		border = mint
	}
	return lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(border).Padding(0, 1).Render(strings.Join(rows, "\n"))
}

func appendPage(rows, data []string, top, height int) []string {
	room := max(0, height-len(rows))
	if len(data) == 0 || room == 0 {
		return rows
	}
	if top > 0 || len(data) > room {
		room--
		rows = append(rows, quiet.Render(fmt.Sprintf("Lines %d–%d of %d  ·  PgUp / PgDn", top+1, min(top+room, len(data)), len(data))))
	}
	return append(rows, data[top:min(top+room, len(data))]...)
}

func between(left, right string, width int) string {
	r := ansi.StringWidth(right)
	if r+2 >= width {
		return pad(left, width)
	}
	return pad(left, width-r-1) + " " + right
}

func crop(s string, width int) string { return ansi.Truncate(s, max(0, width), "…") }

func pad(s string, width int) string {
	s = crop(s, width)
	return s + strings.Repeat(" ", max(0, width-ansi.StringWidth(s)))
}

func mapRows(s string, f func(string) string) []string {
	rows := strings.Split(s, "\n")
	for i, row := range rows {
		rows[i] = f(row)
	}
	return rows
}
