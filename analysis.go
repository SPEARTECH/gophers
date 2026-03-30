package gophers

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/atotto/clipboard"
	"github.com/charmbracelet/bubbles/key"
	btable "github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textarea"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/traefik/yaegi/interp"
	"github.com/traefik/yaegi/stdlib"
)

// LaunchAnalysisShell starts the interactive analysis TUI.
// Starts empty; load dataframes from the command input panel.
func LaunchAnalysisShell() error {
	m := newAnalysisModel()
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}

type modalKind int

const (
	modalNone        modalKind = iota
	modalDFConfig              // DF panel: delete df or back
	modalHistoryOpts           // History panel: full command, delete or back
	modalRowDetail             // Table panel: row columns, selectable for full value
	modalColValue              // Full column value view from row detail
	modalSettings              // Global settings: row limit, char limit
)

type analysisKeys struct {
	Submit    key.Binding
	Quit      key.Binding
	Config    key.Binding
	NextPanel key.Binding
	PrevPanel key.Binding
	Back      key.Binding

	Up       key.Binding
	Down     key.Binding
	PageUp   key.Binding
	PageDown key.Binding
}

var defaultAnalysisKeys = analysisKeys{
	Submit:    key.NewBinding(key.WithKeys("ctrl+s"), key.WithHelp("ctrl+s", "submit/action")),
	Quit:      key.NewBinding(key.WithKeys("ctrl+q"), key.WithHelp("ctrl+q", "quit")),
	Config:    key.NewBinding(key.WithKeys("ctrl+c"), key.WithHelp("ctrl+c", "settings")),
	NextPanel: key.NewBinding(key.WithKeys("ctrl+right"), key.WithHelp("ctrl+→", "next panel")),
	PrevPanel: key.NewBinding(key.WithKeys("ctrl+left"), key.WithHelp("ctrl+←", "prev panel")),
	Back:      key.NewBinding(key.WithKeys("esc"), key.WithHelp("esc", "back/close")),
	Up:        key.NewBinding(key.WithKeys("up", "k")),
	Down:      key.NewBinding(key.WithKeys("down", "j")),
	PageUp:    key.NewBinding(key.WithKeys("pgup")),
	PageDown:  key.NewBinding(key.WithKeys("pgdown")),
}

type analysisModel struct {
	dfs   []*DataFrame
	names []string // df1, df2, ...

	selected int // index of active df, -1 if none

	// left column state
	dfListSel    int
	dfListOffset int
	history      []string // newest first
	histSel      int
	histOffset   int

	table    btable.Model
	textarea textarea.Model

	width, height int
	contentHeight int
	dfListHeight  int
	historyHeight int

	// 0 = dataframe list, 1 = history, 2 = table, 3 = input
	focus int

	// modal state
	modal       modalKind // which modal is open (0 = none)
	modalCursor int       // cursor position within modal options
	modalColSel int       // for row-detail modal: which column is expanded (-1 = none)
	modalIdx    int       // index of item the modal was opened for (df index, hist index, table row)
	modalCopied bool      // feedback flag: value was copied to clipboard

	// Go interpreter
	goInterp    *interp.Interpreter
	trackedVars []string // variable names to check for DataFrames
	evalErr     string   // last eval error message

	// settings (session config)
	cfgRowLimit  int // max rows shown in datatable (0 = unlimited)
	cfgCharLimit int // max characters per cell value (0 = unlimited)

	// settings modal input buffers
	cfgRowLimitBuf  string
	cfgCharLimitBuf string

	keys analysisKeys
}

func newAnalysisModel() analysisModel {
	ta := textarea.New()
	ta.Placeholder = `Go code: df := ReadJSON("path"), df.Select("col"), df.Filter(Col("x").Gt(5))`
	ta.ShowLineNumbers = false
	ta.Prompt = "» "
	ta.SetHeight(1)
	ta.Focus()

	// Initialize yaegi Go interpreter
	i := interp.New(interp.Options{})
	_ = i.Use(stdlib.Symbols)
	_ = i.Use(gophersSymbols())

	// Import gophers and create top-level aliases so user doesn't need package prefix
	_, _ = i.Eval(`import gophers "github.com/speartech/gophers"`)
	_, _ = i.Eval(`
		var (
			Dataframe       = gophers.Dataframe
			ReadJSON        = gophers.ReadJSON
			ReadCSV         = gophers.ReadCSV
			ReadNDJSON      = gophers.ReadNDJSON
			ReadYAML        = gophers.ReadYAML
			ReadParquet     = gophers.ReadParquet
			ReadHTML        = gophers.ReadHTML
			ReadHTMLTop     = gophers.ReadHTMLTop
			ReadSqlite      = gophers.ReadSqlite
			GetAPI          = gophers.GetAPI
			SqliteSQL       = gophers.SqliteSQL
			CloneJSON       = gophers.CloneJSON
			Col             = gophers.Col
			Lit             = gophers.Lit
			Concat          = gophers.Concat
			CurrentTimestamp = gophers.CurrentTimestamp
			CurrentDate     = gophers.CurrentDate
			DateDiff        = gophers.DateDiff
			SHA256          = gophers.SHA256
			SHA512          = gophers.SHA512
			UDF             = gophers.UDF
			Compile         = gophers.Compile
			If              = gophers.If
			Or              = gophers.Or
			And             = gophers.And
			Agg             = gophers.Agg
			Sum             = gophers.Sum
			Max             = gophers.Max
			Min             = gophers.Min
			Median          = gophers.Median
			Mean            = gophers.Mean
			Mode            = gophers.Mode
			Unique          = gophers.Unique
			First           = gophers.First
			CollectList     = gophers.CollectList
			CollectSet      = gophers.CollectSet
			CreateReport    = gophers.CreateReport
			ConnectLLM      = gophers.ConnectLLM
			CustomLLM       = gophers.CustomLLM
		)
	`)

	m := analysisModel{
		dfs:           []*DataFrame{},
		names:         []string{},
		selected:      -1,
		dfListSel:     -1,
		dfListOffset:  0,
		history:       []string{},
		histSel:       -1,
		histOffset:    0,
		table:         btable.New(),
		textarea:      ta,
		width:         100,
		height:        30,
		contentHeight: 16,
		dfListHeight:  4,
		historyHeight: 8,
		focus:        3,
		modal:        modalNone,
		modalColSel:  -1,
		modalIdx:     -1,
		goInterp:     i,
		trackedVars:  []string{},
		cfgRowLimit:  0,
		cfgCharLimit: 0,
		keys:         defaultAnalysisKeys,
	}
	m.computeLayout()
	m.rebuildTable()
	return m
}

func (m analysisModel) Init() tea.Cmd { return nil }

func (m analysisModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		// Ctrl+Q quit (always works)
		if key.Matches(msg, m.keys.Quit) || msg.String() == "ctrl+q" {
			return m, tea.Quit
		}

		// Ctrl+C opens settings (works from anywhere except inside a modal)
		if m.modal == modalNone && (key.Matches(msg, m.keys.Config) || msg.String() == "ctrl+c") {
			m.modal = modalSettings
			m.modalCursor = 0 // start on row limit input
			// Populate buffers with current values
			if m.cfgRowLimit > 0 {
				m.cfgRowLimitBuf = strconv.Itoa(m.cfgRowLimit)
			} else {
				m.cfgRowLimitBuf = ""
			}
			if m.cfgCharLimit > 0 {
				m.cfgCharLimitBuf = strconv.Itoa(m.cfgCharLimit)
			} else {
				m.cfgCharLimitBuf = ""
			}
			m.textarea.Blur()
			return m, nil
		}

		// When a modal is open, route all keys to modal handler
		if m.modal != modalNone {
			return m.updateModal(msg)
		}

		// Focus controls with explicit textarea focus/blur so typing works only in input panel.
		applyFocus := func(f int) {
			m.focus = f
			switch m.focus {
			case 0: // dataframe list
				if len(m.dfs) > 0 && m.dfListSel < 0 {
					m.dfListSel = 0
				}
			case 1: // history
				if len(m.history) > 0 && m.histSel < 0 {
					m.histSel = 0
				}
			case 2: // table
				if m.selected < 0 && len(m.dfs) > 0 {
					if m.dfListSel >= 0 && m.dfListSel < len(m.dfs) {
						m.selected = m.dfListSel
					} else {
						m.selected = 0
					}
				}
			}

			if m.focus == 3 {
				m.textarea.Focus()
			} else {
				m.textarea.Blur()
			}

			m.rebuildTable()
		}

		// Ctrl+Right → next panel, Ctrl+Left → prev panel
		if key.Matches(msg, m.keys.NextPanel) || msg.String() == "ctrl+right" {
			applyFocus((m.focus + 1) % 4)
			return m, nil
		}
		if key.Matches(msg, m.keys.PrevPanel) || msg.String() == "ctrl+left" {
			applyFocus((m.focus + 3) % 4)
			return m, nil
		}

		// Ctrl+S: context-sensitive action per panel
		if key.Matches(msg, m.keys.Submit) || msg.String() == "ctrl+s" {
			switch m.focus {
			case 0: // DF list → open config modal
				if m.dfListSel >= 0 && m.dfListSel < len(m.dfs) {
					m.modal = modalDFConfig
					m.modalIdx = m.dfListSel
					m.modalCursor = 0
				}
				return m, nil
			case 1: // History → open options modal
				if m.histSel >= 0 && m.histSel < len(m.history) {
					m.modal = modalHistoryOpts
					m.modalIdx = m.histSel
					m.modalCursor = 0
				}
				return m, nil
			case 2: // Table → open row detail modal
				if m.selected >= 0 && m.selected < len(m.dfs) {
					row := m.table.Cursor()
					df := m.dfs[m.selected]
					if row >= 0 && row < df.Rows {
						m.modal = modalRowDetail
						m.modalIdx = row
						m.modalCursor = 0
						m.modalColSel = -1
					}
				}
				return m, nil
			case 3: // Input → submit command
				input := strings.TrimSpace(m.textarea.Value())
				if input != "" {
					m.history = append(m.history, input)
					m.histSel = len(m.history) - 1
					// Auto-scroll to show newest entry at bottom
					if m.histSel >= m.historyHeight-1 {
						m.histOffset = m.histSel - (m.historyHeight - 2)
					}
					m.evalCommand(input)
				}
				m.textarea.SetValue("")
				m.textarea.SetHeight(1)
				m.computeLayout()
				m.rebuildTable()
				return m, nil
			}
		}

		switch m.focus {
		case 0: // DataFrames panel
			if key.Matches(msg, m.keys.Up) {
				if m.dfListSel > 0 {
					m.dfListSel--
					if m.dfListSel < m.dfListOffset {
						m.dfListOffset = m.dfListSel
					}
					m.selected = m.dfListSel
					m.rebuildTable()
				}
				return m, nil
			}
			if key.Matches(msg, m.keys.Down) {
				if m.dfListSel < len(m.dfs)-1 {
					m.dfListSel++
					if m.dfListSel >= m.dfListOffset+m.dfListHeight-1 {
						m.dfListOffset = m.dfListSel - (m.dfListHeight - 2)
						if m.dfListOffset < 0 {
							m.dfListOffset = 0
						}
					}
					m.selected = m.dfListSel
					m.rebuildTable()
				}
				return m, nil
			}
			if key.Matches(msg, m.keys.PageUp) {
				m.dfListOffset -= m.dfListHeight - 1
				if m.dfListOffset < 0 {
					m.dfListOffset = 0
				}
				return m, nil
			}
			if key.Matches(msg, m.keys.PageDown) {
				m.dfListOffset += m.dfListHeight - 1
				maxOff := max(0, len(m.dfs)-m.dfListHeight+1)
				if m.dfListOffset > maxOff {
					m.dfListOffset = maxOff
				}
				return m, nil
			}
			if msg.String() == "enter" && m.dfListSel >= 0 && m.dfListSel < len(m.dfs) {
				m.selected = m.dfListSel
				m.rebuildTable()
				m.focus = 2
				return m, nil
			}
			return m, nil

		case 1: // History panel
			if key.Matches(msg, m.keys.Up) {
				if m.histSel > 0 {
					m.histSel--
					if m.histSel < m.histOffset {
						m.histOffset = m.histSel
					}
				}
				return m, nil
			}
			if key.Matches(msg, m.keys.Down) {
				if m.histSel < len(m.history)-1 {
					m.histSel++
					if m.histSel >= m.histOffset+m.historyHeight-1 {
						m.histOffset = m.histSel - (m.historyHeight - 2)
					}
				}
				return m, nil
			}
			if key.Matches(msg, m.keys.PageUp) {
				m.histOffset -= m.historyHeight - 1
				if m.histOffset < 0 {
					m.histOffset = 0
				}
				return m, nil
			}
			if key.Matches(msg, m.keys.PageDown) {
				m.histOffset += m.historyHeight - 1
				maxOff := max(0, len(m.history)-m.historyHeight+1)
				if m.histOffset > maxOff {
					m.histOffset = maxOff
				}
				return m, nil
			}
			if msg.String() == "enter" && m.histSel >= 0 && m.histSel < len(m.history) {
				m.textarea.SetValue(m.history[m.histSel])
				m.focus = 3
				return m, nil
			}
			return m, nil

		case 2: // Data table panel
			m.table, cmd = m.table.Update(msg)
			return m, cmd

		case 3: // Input panel
			// Pre-expand textarea height before Enter so the internal viewport
			// doesn't scroll and hide the first line.
			if msg.Type == tea.KeyEnter {
				upcoming := strings.Count(m.textarea.Value(), "\n") + 2
				if upcoming > 8 {
					upcoming = 8
				}
				m.textarea.SetHeight(upcoming)
				m.computeLayout()
			}

			m.textarea, cmd = m.textarea.Update(msg)

			lines := strings.Count(m.textarea.Value(), "\n") + 1
			if lines < 1 {
				lines = 1
			}
			if lines > 8 {
				lines = 8
			}
			if m.textarea.Height() != lines {
				m.textarea.SetHeight(lines)
				m.computeLayout()
				m.rebuildTable()
			}
			return m, cmd
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.computeLayout()
		m.rebuildTable()
		return m, nil
	}

	return m, cmd
}

func (m analysisModel) View() string {
	status := m.renderStatusBar()

	dfListHeight := m.dfListHeight
	if dfListHeight > m.contentHeight-3 {
		dfListHeight = 3
	}
	historyHeight := m.contentHeight - dfListHeight
	if historyHeight < 3 {
		historyHeight = 3
	}

	inactiveBorder := lipgloss.NewStyle().
		Border(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240"))

	activeBorder := lipgloss.NewStyle().
		Border(lipgloss.ThickBorder()).
		BorderForeground(lipgloss.Color("86"))

	dfStyle := inactiveBorder.Width(30).Height(dfListHeight)
	if m.focus == 0 {
		dfStyle = activeBorder.Width(30).Height(dfListHeight)
	}
	dfBox := dfStyle.Render(m.renderDFListPanelLimited(dfListHeight))

	histStyle := inactiveBorder.Width(30).Height(historyHeight)
	if m.focus == 1 {
		histStyle = activeBorder.Width(30).Height(historyHeight)
	}
	histBox := histStyle.Render(m.renderHistoryPanelLimited(historyHeight))

	leftStack := lipgloss.JoinVertical(lipgloss.Left, dfBox, histBox)
	leftH := lipgloss.Height(leftStack)

	// Match center panel height to left stack so tops and bottoms align
	centerInnerH := leftH - 2 // subtract border top+bottom
	if centerInnerH < 4 {
		centerInnerH = 4
	}

	centerStyle := inactiveBorder.
		Padding(1, 1).
		Height(centerInnerH)
	if m.focus == 2 {
		centerStyle = activeBorder.
			Padding(1, 1).
			Height(centerInnerH)
	}
	centerBox := centerStyle.Render(m.table.View())

	mainRow := lipgloss.JoinHorizontal(lipgloss.Top, leftStack, centerBox)

	var b strings.Builder
	b.WriteString(status)
	b.WriteString("\n")
	b.WriteString(mainRow)
	b.WriteString("\n")
	if m.evalErr != "" {
		errStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("196"))
		errMsg := m.evalErr
		if len(errMsg) > m.width-2 {
			errMsg = errMsg[:m.width-5] + "..."
		}
		b.WriteString(errStyle.Render(errMsg))
		b.WriteString("\n")
	}
	inputStyle := lipgloss.NewStyle().
		Border(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		Width(m.width - 2)
	if m.focus == 3 {
		inputStyle = lipgloss.NewStyle().
			Border(lipgloss.ThickBorder()).
			BorderForeground(lipgloss.Color("86")).
			Width(m.width - 2)
	}
	b.WriteString(inputStyle.Render(m.textarea.View()))
	b.WriteString("\n")

	// Overlay modal if active
	if m.modal != modalNone {
		bg := b.String()
		modalContent := m.renderModal()
		if modalContent != "" {
			return lipgloss.Place(
				m.width, m.height,
				lipgloss.Center, lipgloss.Center,
				modalContent,
				lipgloss.WithWhitespaceChars(" "),
			)
		}
		return bg
	}

	return b.String()
}

func (m *analysisModel) computeLayout() {
	lines := strings.Count(m.textarea.Value(), "\n") + 1
	if lines < 1 {
		lines = 1
	}
	if lines > 8 {
		lines = 8
	}

	// legend(1) + \n(1) + df borders(2) + hist borders(2) +
	// \n(1) + error line(0 or 1+\n) + input borders(2) + input lines + \n(1)
	reserved := 10 + lines
	if m.evalErr != "" {
		reserved += 2 // error text + newline
	}
	m.contentHeight = m.height - reserved
	if m.contentHeight < 6 {
		m.contentHeight = 6
	}

	// DF panel is fixed at 4 inner lines; history gets the rest
	m.dfListHeight = 4
	m.historyHeight = m.contentHeight - m.dfListHeight
	if m.historyHeight < 3 {
		m.historyHeight = 3
	}

	taW := m.width - 4 // subtract border (2) + small padding (2)
	if taW < 20 {
		taW = 20
	}
	m.textarea.SetWidth(taW)
}

func (m analysisModel) renderStatusBar() string {
	keyStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("86"))

	descStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("252"))

	sepStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("240"))

	sep := sepStyle.Render(" │ ")

	hotkeys := []struct{ key, desc string }{
		{"Ctrl+S", "Select/Submit"},
		{"Ctrl+←/→", "Switch Panel"},
		{"Ctrl+C", "Config"},
		{"Ctrl+Q", "Quit"},
	}

	var parts []string
	for _, hk := range hotkeys {
		parts = append(parts, keyStyle.Render(hk.key)+" "+descStyle.Render(hk.desc))
	}

	return strings.Join(parts, sep)
}

func (m analysisModel) renderDFListPanelLimited(maxLines int) string {
	title := "DataFrames"
	lines := []string{title}
	if len(m.dfs) == 0 {
		return title + "\n\n<none loaded>"
	}

	start := m.dfListOffset
	if start < 0 {
		start = 0
	}
	end := start + maxLines - 1
	if end > len(m.dfs) {
		end = len(m.dfs)
	}

	arrowStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("33"))
	for i := start; i < end; i++ {
		prefix := "  "
		if m.focus == 0 && i == m.dfListSel {
			prefix = arrowStyle.Render("> ")
		} else if i == m.selected {
			prefix = arrowStyle.Render("▸ ")
		}
		lines = append(lines, fmt.Sprintf("%s[%d] %s (%d×%d)", prefix, i+1, m.names[i], m.dfs[i].Rows, len(m.dfs[i].Cols)))
	}
	if end < len(m.dfs) {
		lines = append(lines, "  …")
	}
	return strings.Join(lines, "\n")
}

func (m analysisModel) renderHistoryPanelLimited(maxLines int) string {
	title := "History"
	lines := []string{title}
	if len(m.history) == 0 {
		return title + "\n\n<no commands yet>"
	}

	start := m.histOffset
	if start < 0 {
		start = 0
	}
	end := start + maxLines - 1
	if end > len(m.history) {
		end = len(m.history)
	}

	arrowStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("33"))
	for i := start; i < end; i++ {
		prefix := "  "
		if m.focus == 1 && i == m.histSel {
			prefix = arrowStyle.Render("> ")
		}
		lines = append(lines, fmt.Sprintf("%s[%d] %s", prefix, i+1, m.history[i]))
	}
	if end < len(m.history) {
		lines = append(lines, "  …")
	}
	return strings.Join(lines, "\n")
}

func (m *analysisModel) rebuildTable() {
	if m.selected < 0 || m.selected >= len(m.dfs) {
		cols := []btable.Column{{Title: "No DataFrame Selected", Width: 40}}
		redNoData := lipgloss.NewStyle().Foreground(lipgloss.Color("131")).Render("<no data>")
		rows := []btable.Row{{redNoData}}
		t := btable.New(
			btable.WithColumns(cols),
			btable.WithRows(rows),
			btable.WithFocused(m.focus == 2),
			btable.WithHeight(m.contentHeight-2),
		)
		m.table = t
		return
	}

	df := m.dfs[m.selected]
	columns, rows := buildTableColumnsRows(df, m.cfgRowLimit, m.cfgCharLimit)

	tableHeight := m.contentHeight - 2
	if tableHeight < 4 {
		tableHeight = 4
	}

	t := btable.New(
		btable.WithColumns(columns),
		btable.WithRows(rows),
		btable.WithFocused(m.focus == 2),
		btable.WithHeight(tableHeight),
	)

	s := btable.DefaultStyles()
	s.Header = s.Header.
		Bold(true).
		Foreground(lipgloss.Color("255")).
		Background(lipgloss.Color("27"))
	if m.focus == 2 {
		s.Selected = s.Selected.
			Foreground(lipgloss.Color("229")).
			Background(lipgloss.Color("33")).
			Bold(false)
	} else {
		// No highlight when table is not focused
		s.Selected = lipgloss.NewStyle()
	}
	t.SetStyles(s)

	m.table = t
}

func buildTableColumnsRows(df *DataFrame, rowLimit, charLimit int) ([]btable.Column, []btable.Row) {
	colWidths := make([]int, len(df.Cols))
	for i, col := range df.Cols {
		colWidths[i] = len(col)
	}

	numRows := df.Rows
	if rowLimit > 0 && numRows > rowLimit {
		numRows = rowLimit
	}

	rowsStr := make([][]string, numRows)
	for r := 0; r < numRows; r++ {
		row := make([]string, len(df.Cols))
		for cIdx, col := range df.Cols {
			val := ""
			if slice, ok := df.Data[col]; ok && r < len(slice) {
				val = fastToString(slice[r])
			}
			if charLimit > 0 && len(val) > charLimit {
				val = val[:charLimit-3] + "..."
				if charLimit < 4 {
					val = val[:charLimit]
				}
			}
			row[cIdx] = val
			if len(val) > colWidths[cIdx] {
				colWidths[cIdx] = len(val)
			}
		}
		rowsStr[r] = row
	}

	columns := make([]btable.Column, len(df.Cols))
	for i, col := range df.Cols {
		width := colWidths[i] + 2
		if width < 6 {
			width = 6
		}
		if width > 80 {
			width = 80
		}
		columns[i] = btable.Column{Title: col, Width: width}
	}

	rows := make([]btable.Row, len(rowsStr))
	for i, r := range rowsStr {
		rows[i] = btable.Row(r)
	}
	return columns, rows
}

// updateModal handles all key events when a modal is open.
func (m analysisModel) updateModal(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// Esc closes modal (or goes back to parent modal)
	if key.Matches(msg, m.keys.Back) || msg.String() == "esc" {
		if m.modal == modalColValue {
			m.modalCopied = false
			m.modal = modalRowDetail
			m.modalCursor = m.modalColSel
			m.modalColSel = -1
			return m, nil
		}
		m.modal = modalNone
		if m.focus == 3 {
			m.textarea.Focus()
		}
		return m, nil
	}

	isConfirm := msg.String() == "enter" || key.Matches(msg, m.keys.Submit) || msg.String() == "ctrl+s"

	switch m.modal {
	case modalDFConfig:
		// Options: 0=Delete, 1=Back
		optCount := 2
		if key.Matches(msg, m.keys.Up) {
			if m.modalCursor > 0 {
				m.modalCursor--
			}
		}
		if key.Matches(msg, m.keys.Down) {
			if m.modalCursor < optCount-1 {
				m.modalCursor++
			}
		}
		if isConfirm {
			switch m.modalCursor {
			case 0: // Delete DataFrame
				idx := m.modalIdx
				if idx >= 0 && idx < len(m.dfs) {
					m.dfs = append(m.dfs[:idx], m.dfs[idx+1:]...)
					m.names = append(m.names[:idx], m.names[idx+1:]...)
					// Adjust selected
					if m.selected == idx {
						m.selected = -1
					} else if m.selected > idx {
						m.selected--
					}
					// Adjust dfListSel
					if m.dfListSel >= len(m.dfs) {
						m.dfListSel = len(m.dfs) - 1
					}
					m.rebuildTable()
				}
				m.modal = modalNone
			case 1: // Back
				m.modal = modalNone
			}
		}
		return m, nil

	case modalHistoryOpts:
		// Options: 0=Copy Command, 1=Copy All History, 2=Delete, 3=Back
		optCount := 4
		if key.Matches(msg, m.keys.Up) {
			if m.modalCursor > 0 {
				m.modalCursor--
				m.modalCopied = false
			}
		}
		if key.Matches(msg, m.keys.Down) {
			if m.modalCursor < optCount-1 {
				m.modalCursor++
				m.modalCopied = false
			}
		}
		if isConfirm {
			switch m.modalCursor {
			case 0: // Copy Command
				idx := m.modalIdx
				if idx >= 0 && idx < len(m.history) {
					_ = clipboard.WriteAll(m.history[idx])
					m.modalCopied = true
				}
			case 1: // Copy All History
				_ = clipboard.WriteAll(strings.Join(m.history, "\n"))
				m.modalCopied = true
			case 2: // Delete
				idx := m.modalIdx
				if idx >= 0 && idx < len(m.history) {
					m.history = append(m.history[:idx], m.history[idx+1:]...)
					if m.histSel >= len(m.history) {
						m.histSel = len(m.history) - 1
					}
				}
				m.modal = modalNone
			case 3: // Back
				m.modal = modalNone
			}
		}
		return m, nil

	case modalRowDetail:
		// Options: one per column + Back at end
		if m.selected < 0 || m.selected >= len(m.dfs) {
			m.modal = modalNone
			return m, nil
		}
		df := m.dfs[m.selected]
		optCount := len(df.Cols) + 1 // columns + Back
		if key.Matches(msg, m.keys.Up) {
			if m.modalCursor > 0 {
				m.modalCursor--
			}
		}
		if key.Matches(msg, m.keys.Down) {
			if m.modalCursor < optCount-1 {
				m.modalCursor++
			}
		}
		if isConfirm {
			if m.modalCursor < len(df.Cols) {
				// Open full value modal for this column
				m.modalColSel = m.modalCursor
				m.modal = modalColValue
				m.modalCursor = 0 // cursor on Back button
			} else {
				// Back
				m.modal = modalNone
			}
		}
		return m, nil

	case modalColValue:
		// Options: 0=Copy, 1=Back
		optCount := 2
		if key.Matches(msg, m.keys.Up) {
			if m.modalCursor > 0 {
				m.modalCursor--
				m.modalCopied = false
			}
		}
		if key.Matches(msg, m.keys.Down) {
			if m.modalCursor < optCount-1 {
				m.modalCursor++
				m.modalCopied = false
			}
		}
		if isConfirm {
			switch m.modalCursor {
			case 0: // Copy to clipboard
				if m.selected >= 0 && m.selected < len(m.dfs) {
					df := m.dfs[m.selected]
					row := m.modalIdx
					colIdx := m.modalColSel
					if colIdx >= 0 && colIdx < len(df.Cols) {
						col := df.Cols[colIdx]
						val := ""
						if slice, ok := df.Data[col]; ok && row < len(slice) {
							val = fastToString(slice[row])
						}
						_ = clipboard.WriteAll(val)
						m.modalCopied = true
					}
				}
			case 1: // Back
				m.modalCopied = false
				m.modal = modalRowDetail
				m.modalCursor = m.modalColSel
				m.modalColSel = -1
			}
		}
		return m, nil

	case modalSettings:
		// 0=Row Limit input, 1=Char Limit input, 2=Commit, 3=Back
		optCount := 4
		if key.Matches(msg, m.keys.Up) {
			if m.modalCursor > 0 {
				m.modalCursor--
			}
		}
		if key.Matches(msg, m.keys.Down) {
			if m.modalCursor < optCount-1 {
				m.modalCursor++
			}
		}
		// When on an input field, handle digit typing and backspace
		if m.modalCursor == 0 || m.modalCursor == 1 {
			s := msg.String()
			if len(s) == 1 && s[0] >= '0' && s[0] <= '9' {
				if m.modalCursor == 0 {
					m.cfgRowLimitBuf += s
				} else {
					m.cfgCharLimitBuf += s
				}
				return m, nil
			}
			if msg.Type == tea.KeyBackspace {
				if m.modalCursor == 0 && len(m.cfgRowLimitBuf) > 0 {
					m.cfgRowLimitBuf = m.cfgRowLimitBuf[:len(m.cfgRowLimitBuf)-1]
				} else if m.modalCursor == 1 && len(m.cfgCharLimitBuf) > 0 {
					m.cfgCharLimitBuf = m.cfgCharLimitBuf[:len(m.cfgCharLimitBuf)-1]
				}
				return m, nil
			}
		}
		if isConfirm {
			switch m.modalCursor {
			case 2: // Commit
				if v, err := strconv.Atoi(m.cfgRowLimitBuf); err == nil && v > 0 {
					m.cfgRowLimit = v
				} else {
					m.cfgRowLimit = 0
				}
				if v, err := strconv.Atoi(m.cfgCharLimitBuf); err == nil && v > 0 {
					m.cfgCharLimit = v
				} else {
					m.cfgCharLimit = 0
				}
				m.rebuildTable()
				m.modal = modalNone
				if m.focus == 3 {
					m.textarea.Focus()
				}
			case 3: // Back (discard)
				m.modal = modalNone
				if m.focus == 3 {
					m.textarea.Focus()
				}
			}
		}
		return m, nil
	}

	return m, nil
}

// renderModal renders the active modal as a centered overlay string.
func (m analysisModel) renderModal() string {
	modalBorder := lipgloss.NewStyle().
		Border(lipgloss.ThickBorder()).
		BorderForeground(lipgloss.Color("86")).
		Padding(1, 2).
		Width(80)

	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("255")).
		Background(lipgloss.Color("27")).
		Padding(0, 1)

	selPrefix := lipgloss.NewStyle().Foreground(lipgloss.Color("33")).Bold(true)
	dimStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("241"))

	renderOption := func(idx, cursor int, label string) string {
		if idx == cursor {
			return selPrefix.Render("> ") + label
		}
		return "  " + dimStyle.Render(label)
	}

	switch m.modal {
	case modalDFConfig:
		idx := m.modalIdx
		if idx < 0 || idx >= len(m.dfs) {
			return ""
		}
		name := m.names[idx]
		df := m.dfs[idx]
		title := titleStyle.Render(fmt.Sprintf(" DataFrame: %s (%d×%d) ", name, df.Rows, len(df.Cols)))

		var lines []string
		lines = append(lines, title)
		lines = append(lines, "")
		lines = append(lines, renderOption(0, m.modalCursor, "Delete DataFrame"))
		lines = append(lines, renderOption(1, m.modalCursor, "Back"))

		return modalBorder.Render(strings.Join(lines, "\n"))

	case modalHistoryOpts:
		idx := m.modalIdx
		if idx < 0 || idx >= len(m.history) {
			return ""
		}
		title := titleStyle.Render(" Command ")

		// Word-wrap the command to fit in the modal
		cmdText := m.history[idx]
		maxW := 72
		var wrapped []string
		for len(cmdText) > maxW {
			wrapped = append(wrapped, cmdText[:maxW])
			cmdText = cmdText[maxW:]
		}
		wrapped = append(wrapped, cmdText)

		var lines []string
		lines = append(lines, title)
		lines = append(lines, "")
		for _, w := range wrapped {
			lines = append(lines, w)
		}
		lines = append(lines, "")
		copyLabel := "Copy Command"
		if m.modalCopied && m.modalCursor == 0 {
			copyLabel = "Copy Command  ✓ Copied!"
		}
		copyAllLabel := "Copy All History"
		if m.modalCopied && m.modalCursor == 1 {
			copyAllLabel = "Copy All History  ✓ Copied!"
		}
		lines = append(lines, renderOption(0, m.modalCursor, copyLabel))
		lines = append(lines, renderOption(1, m.modalCursor, copyAllLabel))
		lines = append(lines, renderOption(2, m.modalCursor, "Delete"))
		lines = append(lines, renderOption(3, m.modalCursor, "Back"))

		return modalBorder.Render(strings.Join(lines, "\n"))

	case modalRowDetail:
		if m.selected < 0 || m.selected >= len(m.dfs) {
			return ""
		}
		df := m.dfs[m.selected]
		row := m.modalIdx
		if row < 0 || row >= df.Rows {
			return ""
		}
		title := titleStyle.Render(fmt.Sprintf(" Row %d ", row+1))

		var lines []string
		lines = append(lines, title)
		lines = append(lines, "")

		for i, col := range df.Cols {
			val := ""
			if slice, ok := df.Data[col]; ok && row < len(slice) {
				val = fastToString(slice[row])
			}

			// Truncated display
			display := val
			if len(display) > 60 {
				display = display[:57] + "..."
			}
			label := fmt.Sprintf("%s: %s", col, display)
			lines = append(lines, renderOption(i, m.modalCursor, label))
		}

		lines = append(lines, "")
		lines = append(lines, renderOption(len(df.Cols), m.modalCursor, "Back"))

		return modalBorder.Render(strings.Join(lines, "\n"))

	case modalColValue:
		if m.selected < 0 || m.selected >= len(m.dfs) {
			return ""
		}
		df := m.dfs[m.selected]
		row := m.modalIdx
		colIdx := m.modalColSel
		if row < 0 || row >= df.Rows || colIdx < 0 || colIdx >= len(df.Cols) {
			return ""
		}
		col := df.Cols[colIdx]
		val := ""
		if slice, ok := df.Data[col]; ok && row < len(slice) {
			val = fastToString(slice[row])
		}

		title := titleStyle.Render(fmt.Sprintf(" %s (Row %d) ", col, row+1))

		var lines []string
		lines = append(lines, title)
		lines = append(lines, "")

		// Word-wrap the full value
		maxW := 72
		fullVal := val
		if fullVal == "" {
			lines = append(lines, dimStyle.Render("<empty>"))
		} else {
			for len(fullVal) > maxW {
				lines = append(lines, fullVal[:maxW])
				fullVal = fullVal[maxW:]
			}
			lines = append(lines, fullVal)
		}

		lines = append(lines, "")
		copyLabel := "Copy to Clipboard"
		if m.modalCopied {
			copyLabel = "Copy to Clipboard  ✓ Copied!"
		}
		lines = append(lines, renderOption(0, m.modalCursor, copyLabel))
		lines = append(lines, renderOption(1, m.modalCursor, "Back"))

		return modalBorder.Render(strings.Join(lines, "\n"))

	case modalSettings:
		title := titleStyle.Render(" Settings ")

		inputStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("255")).
			Background(lipgloss.Color("237")).
			Padding(0, 1).
			Width(20)

		activeInputStyle := inputStyle.
			Background(lipgloss.Color("240")).
			Bold(true)

		labelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("252"))

		renderInput := func(idx int, label, buf string) string {
			cursor := " "
			style := inputStyle
			if idx == m.modalCursor {
				cursor = "▎"
				style = activeInputStyle
			}
			display := buf + cursor
			if buf == "" && idx != m.modalCursor {
				display = dimStyle.Render("unlimited")
			}
			return labelStyle.Render(label) + "\n" + style.Render(display)
		}

		var lines []string
		lines = append(lines, title)
		lines = append(lines, "")
		lines = append(lines, renderInput(0, "Row Limit:", m.cfgRowLimitBuf))
		lines = append(lines, "")
		lines = append(lines, renderInput(1, "Value Character Limit:", m.cfgCharLimitBuf))
		lines = append(lines, "")
		lines = append(lines, renderOption(2, m.modalCursor, "Commit"))
		lines = append(lines, renderOption(3, m.modalCursor, "Back"))

		return modalBorder.Render(strings.Join(lines, "\n"))
	}

	return ""
}

// varAssignRe matches simple Go variable assignments: "x := ..." or "x = ..."
var varAssignRe = regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*)\s*:?=`)

// evalCommand evaluates Go code via the embedded yaegi interpreter,
// then syncs any DataFrame variables into the TUI panels.
func (m *analysisModel) evalCommand(input string) {
	m.evalErr = ""

	// Extract variable name if this is an assignment
	if match := varAssignRe.FindStringSubmatch(strings.TrimSpace(input)); match != nil {
		varName := match[1]
		// Track this variable name (avoid duplicates)
		found := false
		for _, v := range m.trackedVars {
			if v == varName {
				found = true
				break
			}
		}
		if !found {
			m.trackedVars = append(m.trackedVars, varName)
		}
	}

	// Evaluate the input
	_, err := m.goInterp.Eval(input)
	if err != nil {
		m.evalErr = err.Error()
		return
	}

	// Sync DataFrame variables
	m.syncDataFrames()
}

// syncDataFrames scans all tracked variable names in the interpreter,
// checks if they are *DataFrame, and updates the TUI DF list accordingly.
func (m *analysisModel) syncDataFrames() {
	newDfs := make([]*DataFrame, 0)
	newNames := make([]string, 0)

	for _, varName := range m.trackedVars {
		val, err := m.goInterp.Eval(varName)
		if err != nil {
			continue
		}

		// Check if the value is a *DataFrame
		if !val.IsValid() || val.IsZero() {
			continue
		}

		// Handle both *DataFrame and DataFrame
		var df *DataFrame
		v := val.Interface()
		switch t := v.(type) {
		case *DataFrame:
			df = t
		default:
			// Try to get through pointer indirection
			if val.Kind() == reflect.Ptr && !val.IsNil() {
				if d, ok := val.Interface().(*DataFrame); ok {
					df = d
				}
			}
		}

		if df != nil {
			newDfs = append(newDfs, df)
			newNames = append(newNames, varName)
		}
	}

	m.dfs = newDfs
	m.names = newNames

	// Adjust selection
	if len(m.dfs) == 0 {
		m.selected = -1
		m.dfListSel = -1
	} else {
		// Try to keep current selection by name, or select the last one
		if m.selected >= len(m.dfs) {
			m.selected = len(m.dfs) - 1
		}
		if m.dfListSel >= len(m.dfs) {
			m.dfListSel = len(m.dfs) - 1
		}
		// If nothing was selected, select the last (newest) df
		if m.selected < 0 {
			m.selected = len(m.dfs) - 1
			m.dfListSel = m.selected
		}
	}

	m.rebuildTable()
}

func max(a, b int) int {
	if a >= b {
		return a
	}
	return b
}
