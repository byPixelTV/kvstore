package tui

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/byPixelTV/kvstore/internal/console"
)

func (m *model) discover() tea.Cmd {
	return m.request(namespacesSlot, "Namespace discovery", func(ctx context.Context, client console.Client) (func(*model) tea.Cmd, error) {
		namespaces, err := client.Namespaces(ctx)
		return func(m *model) tea.Cmd {
			m.namespaces = append(namespaces, m.identity.Namespaces...)
			slices.Sort(m.namespaces)
			m.namespaces = slices.Compact(m.namespaces)
			if m.namespace == "" && len(m.namespaces) > 0 {
				return m.selectNamespace(m.namespaces[0])
			}
			return nil
		}, err
	})
}

func (m *model) selectNamespace(namespace string) tea.Cmd {
	if namespace == "" {
		m.fail("Enter a namespace.")
		return nil
	}
	if !m.canUse(namespace) {
		m.fail("This access key does not cover that namespace.")
		return nil
	}
	m.namespace, m.filter, m.keyIndex = namespace, "", 0
	m.invalidate(getSlot)
	m.setSelected(nil)
	m.batchLog = nil
	m.say("Namespace " + safeLine(namespace))
	return m.scan(false)
}

func (m *model) canUse(namespace string) bool {
	return len(m.identity.Namespaces) == 0 || slices.Contains(m.identity.Namespaces, namespace)
}

func (m *model) scan(more bool) tea.Cmd {
	if !m.identity.Read || m.namespace == "" {
		return nil
	}
	namespace := m.namespace
	limit := max(scanPageSize, m.pages[namespace].limit)
	if more {
		if !m.pages[namespace].more {
			return nil
		}
		limit += scanPageSize
	}
	return m.request(scanSlot, "Browse keys", func(ctx context.Context, client console.Client) (func(*model) tea.Cmd, error) {
		page, err := client.Scan(ctx, namespace, limit)
		items := make([]keyItem, 0, len(page.Records))
		for _, record := range page.Records {
			items = append(items, keyItem{key: record.Key, revision: record.Revision})
		}
		slices.SortFunc(items, func(a, b keyItem) int { return strings.Compare(a.key, b.key) })
		return func(m *model) tea.Cmd {
			selection := m.currentKey()
			m.pages[namespace] = keyPage{items: items, limit: limit, more: page.More}
			if m.namespace == namespace {
				m.restoreSelection(selection)
			}
			return nil
		}, err
	})
}

func (m *model) visibleKeys() []keyItem {
	items := m.pages[m.namespace].items
	if m.filter == "" {
		return items
	}
	result := make([]keyItem, 0, len(items))
	search := strings.ToLower(m.filter)
	for _, item := range items {
		if strings.Contains(strings.ToLower(item.key), search) {
			result = append(result, item)
		}
	}
	return result
}

func (m *model) currentKey() string {
	items := m.visibleKeys()
	if m.keyIndex < len(items) {
		return items[m.keyIndex].key
	}
	return ""
}

func (m *model) restoreSelection(key string) {
	items := m.visibleKeys()
	if index := slices.IndexFunc(items, func(item keyItem) bool { return item.key == key }); index >= 0 {
		m.keyIndex = index
	} else {
		m.keyIndex = min(m.keyIndex, max(0, len(items)-1))
	}
}

func (m *model) get(key string) tea.Cmd {
	if !m.canRead() || !m.requireKey(key) {
		return nil
	}
	namespace := m.namespace
	m.batchLog = nil
	m.setSelected(nil)
	return m.request(getSlot, "Get key", func(ctx context.Context, client console.Client) (func(*model) tea.Cmd, error) {
		record, found, err := client.Get(ctx, namespace, key)
		return func(m *model) tea.Cmd {
			if !found {
				m.say("Key not found: " + safeLine(key))
				return nil
			}
			m.restoreSelection(key)
			m.setSelected(&record)
			m.say("Loaded " + safeLine(key))
			return nil
		}, err
	})
}

func (m *model) store(record console.Record) tea.Cmd {
	return m.request(writeSlot, "Set key", func(ctx context.Context, client console.Client) (func(*model) tea.Cmd, error) {
		result, err := client.Set(ctx, record)
		return func(m *model) tea.Cmd {
			m.say(fmt.Sprintf("Saved %s at revision %d.", safeLine(record.Key), result.Revision))
			if record.Namespace == m.namespace {
				m.invalidate(getSlot)
				m.batchLog = nil
				if m.identity.Read {
					m.setSelected(&result)
				}
				return m.scan(false)
			}
			return nil
		}, err
	})
}

func (m *model) confirmDelete(key string) tea.Cmd {
	if !m.canWrite() || !m.requireKey(key) {
		return nil
	}
	namespace := m.namespace
	return m.confirm("Delete key", []string{safeLine(key), "Namespace  " + safeLine(namespace)}, func(m *model) tea.Cmd {
		return m.request(writeSlot, "Delete key", func(ctx context.Context, client console.Client) (func(*model) tea.Cmd, error) {
			deleted, err := client.Delete(ctx, namespace, key)
			return func(m *model) tea.Cmd {
				if deleted {
					m.say("Deleted " + safeLine(key))
				} else {
					m.say("Key not found: " + safeLine(key))
				}
				if namespace == m.namespace {
					m.invalidate(getSlot)
					if m.selected != nil && m.selected.Key == key {
						m.setSelected(nil)
					}
					return m.scan(false)
				}
				return nil
			}, err
		})
	})
}

func (m *model) sendBatch(operations []console.Mutation) tea.Cmd {
	return m.request(writeSlot, "Batch write", func(ctx context.Context, client console.Client) (func(*model) tea.Cmd, error) {
		results, err := client.Batch(ctx, operations)
		return func(m *model) tea.Cmd {
			m.invalidate(getSlot)
			m.setSelected(nil)
			m.batchLog = make([]string, len(operations))
			var applied, rejected int
			for i, op := range operations {
				prefix := fmt.Sprintf("%3d  %s / %s  ·  ", i+1, safeLine(op.Namespace), safeLine(op.Key))
				switch {
				case i >= len(results) || results[i].Error != "":
					m.batchLog[i] = prefix + "Failed"
					rejected++
				case op.Delete && !results[i].Deleted:
					m.batchLog[i] = prefix + "Key not found"
				case op.Delete:
					m.batchLog[i] = prefix + "Deleted"
					applied++
				default:
					m.batchLog[i] = fmt.Sprintf("%sSaved at revision %d", prefix, results[i].Revision)
					applied++
				}
			}
			m.valueTop = 0
			summary := fmt.Sprintf("Batch complete · %d applied · %d failed", applied, rejected)
			if rejected > 0 {
				m.fail(summary)
			} else {
				m.say(summary)
			}
			return m.scan(false)
		}, err
	})
}

func (m *model) loadHead() tea.Cmd {
	return m.request(headSlot, "Cluster status", func(ctx context.Context, client console.Client) (func(*model) tea.Cmd, error) {
		head, err := client.Head(ctx)
		return func(m *model) tea.Cmd {
			m.head = &head
			return nil
		}, err
	})
}

func (m *model) listAccessKeys() tea.Cmd {
	if !m.identity.Manage {
		return nil
	}
	return m.request(keysSlot, "List access keys", func(ctx context.Context, client console.Client) (func(*model) tea.Cmd, error) {
		keys, err := client.ListKeys(ctx)
		slices.SortFunc(keys, func(a, b console.AccessKey) int { return strings.Compare(a.Name, b.Name) })
		return func(m *model) tea.Cmd {
			selected := m.currentAccessKey()
			m.accessKeys = keys
			i := slices.IndexFunc(keys, func(k console.AccessKey) bool { return k.Name == selected })
			if i == -1 {
				i = min(m.accessPick, max(0, len(keys)-1))
			}
			m.accessPick = i
			return nil
		}, err
	})
}

func (m *model) currentAccessKey() string {
	if m.accessPick < len(m.accessKeys) {
		return m.accessKeys[m.accessPick].Name
	}
	return ""
}

func (m *model) createAccessKey(key console.AccessKey) tea.Cmd {
	return m.request(writeSlot, "Create access key", func(ctx context.Context, client console.Client) (func(*model) tea.Cmd, error) {
		secret, err := client.CreateKey(ctx, key)
		return func(m *model) tea.Cmd {
			m.say("Created access key " + safeLine(key.Name))
			m.dialog = &dialog{kind: secretDialog, title: "Access key created", hint: safeLine(key.Name), secret: secret}
			return m.listAccessKeys()
		}, err
	})
}

func (m *model) confirmRevoke() tea.Cmd {
	if !m.canManage() {
		return nil
	}
	name := m.currentAccessKey()
	if name == "" {
		m.fail("Select an access key to revoke.")
		return nil
	}
	lines := []string{safeLine(name), "Revoking a key takes effect immediately."}
	if name == m.identity.Name {
		lines = append(lines, "This is the access key for your current session.")
	}
	return m.confirm("Revoke access key", lines, func(m *model) tea.Cmd {
		return m.request(writeSlot, "Revoke access key", func(ctx context.Context, client console.Client) (func(*model) tea.Cmd, error) {
			err := client.RevokeKey(ctx, name)
			return func(m *model) tea.Cmd {
				m.say("Revoked access key " + safeLine(name))
				return m.listAccessKeys()
			}, err
		})
	})
}

type watchState struct {
	id        uint64
	cancel    context.CancelFunc
	namespace string
	key       string
	starting  bool
	events    []string
	total     uint64
	top       int
}

type watchOpened struct {
	id     uint64
	stream console.WatchStream
	err    error
}

type watchUpdate struct {
	id     uint64
	stream console.WatchStream
	event  console.Event
	err    error
}

func (m *model) startWatch(key string) tea.Cmd {
	if !m.canRead() {
		return nil
	}
	m.stopWatch(false)
	ctx, cancel := context.WithCancel(m.ctx)
	m.watch = watchState{id: m.watch.id + 1, cancel: cancel, namespace: m.namespace, key: key, starting: true}
	m.tab = watchTab
	id, namespace, client := m.watch.id, m.namespace, m.client
	return func() tea.Msg {
		stream, err := client.Watch(ctx, namespace, key)
		return watchOpened{id: id, stream: stream, err: err}
	}
}

func (m *model) stopWatch(announce bool) {
	if m.watch.cancel == nil {
		return
	}
	m.watch.cancel()
	m.watch.cancel = nil
	m.watch.id++
	m.watch.starting = false
	if announce {
		m.say("Watch stopped.")
	}
}

func (m *model) receiveWatch(msg watchOpened) tea.Cmd {
	if msg.id != m.watch.id {
		return nil
	}
	if msg.err != nil {
		m.stopWatch(false)
		m.fail(rpcError("Start watch", msg.err))
		return nil
	}
	m.watch.starting = false
	m.say("Watch is live.")
	return nextEvent(msg.id, msg.stream)
}

func nextEvent(id uint64, stream console.WatchStream) tea.Cmd {
	return func() tea.Msg {
		event, err := stream.Next()
		return watchUpdate{id: id, stream: stream, event: event, err: err}
	}
}

func (m *model) receiveEvent(msg watchUpdate) tea.Cmd {
	if msg.id != m.watch.id {
		return nil
	}
	if msg.err != nil {
		m.stopWatch(false)
		if errors.Is(msg.err, io.EOF) {
			m.say("Watch ended.")
		} else {
			m.fail(rpcError("Watch", msg.err))
		}
		return nil
	}
	if !msg.event.Checkpoint {
		record := msg.event.Record
		verb := "SET"
		if msg.event.Deleted {
			verb = "DELETE"
		}
		line := fmt.Sprintf("%-6s  %8d  %s", verb, record.Revision, safeLine(record.Key))
		if !msg.event.Deleted {
			value := record.Value[:min(len(record.Value), 160)]
			line += "  ·  " + safeLine(string(value))
		}
		const history = 500
		if len(m.watch.events) == history {
			copy(m.watch.events, m.watch.events[1:])
			m.watch.events = m.watch.events[:history-1]
		}
		m.watch.events = append(m.watch.events, line)
		m.watch.total++
		if m.watch.top > 0 {
			m.watch.top = min(len(m.watch.events)-1, m.watch.top+1)
		}
	}
	return nextEvent(msg.id, msg.stream)
}

func (m *model) canRead() bool {
	if !m.identity.Read {
		m.fail("This access key does not have read permission.")
		return false
	}
	return m.requireNamespace()
}

func (m *model) canWrite() bool {
	if !m.identity.Write {
		m.fail("This access key does not have write permission.")
		return false
	}
	return m.canMutate() && m.requireNamespace()
}

func (m *model) canManage() bool {
	if !m.identity.Manage {
		m.fail("This access key does not have management permission.")
		return false
	}
	return m.canMutate()
}

func (m *model) canMutate() bool {
	if m.requests[writeSlot].pending {
		m.fail("A change is already in progress.")
		return false
	}
	return true
}

func (m *model) requireNamespace() bool {
	if m.namespace == "" {
		m.fail("Choose a namespace first. Press N for namespaces.")
		return false
	}
	return true
}

func (m *model) requireKey(key string) bool {
	if key == "" {
		m.fail("Enter or select a key.")
		return false
	}
	return true
}
