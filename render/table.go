package render

import (
	"fmt"
	"io"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
)

// Column describes a table column with its display metadata.
type Column struct {
	Key      string
	Header   string
	WidthMin int
	WidthMax int
	Align    text.Align
}

// Table is a thin wrapper over go-pretty's table.Writer that keys columns
// by name so column sets can be declared once and selected by key at render
// time. Rendering itself is delegated to go-pretty.
type Table[T any] struct {
	cols     []Column
	rows     []T
	value    func(rowIdx int, col string, row T) any
	style    *table.Style
	title    string
	vertical bool
}

func NewTable[T any](value func(rowIdx int, col string, row T) any) *Table[T] {
	return &Table[T]{value: value}
}

func (t *Table[T]) SetStyle(s table.Style) *Table[T] {
	t.style = &s
	return t
}

func (t *Table[T]) SetTitle(title string) *Table[T] {
	t.title = title
	return t
}

// SetVertical toggles MySQL \G-style output: each row is printed as a list
// of right-aligned column-name/value pairs, separated by a banner line.
func (t *Table[T]) SetVertical(v bool) *Table[T] {
	t.vertical = v
	return t
}

func (t *Table[T]) AddColumn(c Column) *Table[T] {
	t.cols = append(t.cols, c)
	return t
}

func (t *Table[T]) AddColumns(cs ...Column) *Table[T] {
	t.cols = append(t.cols, cs...)
	return t
}

func (t *Table[T]) AddRow(r T) *Table[T] {
	t.rows = append(t.rows, r)
	return t
}

func (t *Table[T]) AddRows(rs []T) *Table[T] {
	t.rows = append(t.rows, rs...)
	return t
}

func (t *Table[T]) Render(w io.Writer) {
	if t.vertical {
		t.renderVertical(w)
		return
	}
	t.renderHorizontal(w)
}

func (t *Table[T]) renderHorizontal(w io.Writer) {
	pt := table.NewWriter()
	pt.SetOutputMirror(w)
	if t.title != "" {
		pt.SetTitle(t.title)
	}

	headerRow := make(table.Row, len(t.cols))
	configs := make([]table.ColumnConfig, len(t.cols))
	for i, col := range t.cols {
		headerRow[i] = col.Header
		configs[i] = table.ColumnConfig{
			Number:   i + 1,
			WidthMin: col.WidthMin,
			WidthMax: col.WidthMax,
			Align:    col.Align,
		}
	}
	pt.AppendHeader(headerRow)
	pt.SetColumnConfigs(configs)

	for i, row := range t.rows {
		r := make(table.Row, len(t.cols))
		for j, col := range t.cols {
			r[j] = t.value(i, col.Key, row)
		}
		pt.AppendRow(r)
	}

	if t.style != nil {
		pt.SetStyle(*t.style)
	} else {
		pt.SetStyle(table.StyleLight)
	}
	pt.Render()
}

func (t *Table[T]) renderVertical(w io.Writer) {
	maxHeaderWidth := 0
	for _, col := range t.cols {
		if hw := displayWidth(col.Header); hw > maxHeaderWidth {
			maxHeaderWidth = hw
		}
	}

	const bannerStars = "***************************"
	for i, row := range t.rows {
		fmt.Fprintf(w, "%s %d. row %s\n", bannerStars, i+1, bannerStars)
		for _, col := range t.cols {
			pad := strings.Repeat(" ", maxHeaderWidth-displayWidth(col.Header))
			fmt.Fprintf(w, "%s%s: %v\n", pad, col.Header, t.value(i, col.Key, row))
		}
	}
}

// displayWidth returns the visual cell width of s (CJK-aware),
// using go-pretty's per-rune width helper.
func displayWidth(s string) int {
	w := 0
	for _, r := range s {
		w += text.RuneWidth(r)
	}
	return w
}
