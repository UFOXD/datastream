package oracle

import (
	"fmt"
	"strings"
)

// DmlType DML 操作类型
type DmlType int

const (
	DmlInsert DmlType = iota
	DmlUpdate
	DmlDelete
)

// DmlEntry 一条 DML 解析结果
type DmlEntry struct {
	Type      DmlType
	NewValues map[string]string
	OldValues map[string]string
}

// DmlParser LogMiner SQL_REDO 解析器
type DmlParser interface {
	Parse(sql string) (*DmlEntry, error)
}

type dmlParser struct {
	sql    string
	pos    int
	length int
}

// NewDmlParser 创建 DML 解析器
func NewDmlParser() DmlParser {
	return &dmlParser{}
}

func (p *dmlParser) Parse(sql string) (*DmlEntry, error) {
	p.sql = sql
	p.pos = 0
	p.length = len(sql)

	if p.length == 0 {
		return nil, fmt.Errorf("empty SQL")
	}

	switch p.sql[0] {
	case 'i':
		return p.parseInsert()
	case 'u':
		return p.parseUpdate()
	case 'd':
		return p.parseDelete()
	default:
		return nil, fmt.Errorf("unsupported DML: %q", sql)
	}
}

func (p *dmlParser) parseInsert() (*DmlEntry, error) {
	if err := p.expect("insert into "); err != nil {
		return nil, err
	}
	p.skipTableName()

	cols, err := p.parseColumnList()
	if err != nil {
		return nil, err
	}

	p.skipSpaces()
	if err := p.expect("values "); err != nil {
		return nil, err
	}

	vals, err := p.parseValueList(len(cols))
	if err != nil {
		return nil, err
	}

	newValues := make(map[string]string, len(cols))
	for i, col := range cols {
		if vals[i] == "Unsupported Type" {
			continue
		}
		newValues[col] = vals[i]
	}

	return &DmlEntry{
		Type:      DmlInsert,
		NewValues: newValues,
		OldValues: make(map[string]string),
	}, nil
}

func (p *dmlParser) parseUpdate() (*DmlEntry, error) {
	if err := p.expect("update "); err != nil {
		return nil, err
	}
	p.skipTableName()

	newValues, err := p.parseSetClause()
	if err != nil {
		return nil, err
	}

	oldValues := make(map[string]string)
	p.skipSpaces()
	if p.pos < p.length && p.hasPrefix("where ") {
		oldValues, err = p.parseWhereClause()
		if err != nil {
			return nil, err
		}
	}

	return &DmlEntry{
		Type:      DmlUpdate,
		NewValues: newValues,
		OldValues: oldValues,
	}, nil
}

func (p *dmlParser) parseDelete() (*DmlEntry, error) {
	if err := p.expect("delete from "); err != nil {
		return nil, err
	}
	p.skipTableName()
	p.skipSpaces()

	oldValues, err := p.parseWhereClause()
	if err != nil {
		return nil, err
	}

	return &DmlEntry{
		Type:      DmlDelete,
		NewValues: make(map[string]string),
		OldValues: oldValues,
	}, nil
}

func (p *dmlParser) skipTableName() {
	inQuote := false
	for p.pos < p.length {
		c := p.sql[p.pos]
		if c == '"' {
			inQuote = !inQuote
		} else if !inQuote && (c == '(' || c == ' ') {
			break
		}
		p.pos++
	}
}

func (p *dmlParser) parseColumnList() ([]string, error) {
	p.skipSpaces()
	if p.pos >= p.length || p.sql[p.pos] != '(' {
		return nil, fmt.Errorf("expected '(' at pos %d", p.pos)
	}
	p.pos++ // skip '('

	var cols []string
	for p.pos < p.length {
		p.skipSpaces()
		if p.sql[p.pos] == ')' {
			p.pos++
			break
		}
		if p.sql[p.pos] == ',' {
			p.pos++
			continue
		}
		name := p.readQuotedName()
		if name == "" {
			return nil, fmt.Errorf("expected column name at pos %d", p.pos)
		}
		cols = append(cols, name)
	}
	return cols, nil
}

func (p *dmlParser) parseValueList(numCols int) ([]string, error) {
	p.skipSpaces()
	if p.pos >= p.length || p.sql[p.pos] != '(' {
		return nil, fmt.Errorf("expected '(' at pos %d", p.pos)
	}
	p.pos++ // skip '('

	var vals []string
	for p.pos < p.length {
		p.skipSpaces()
		if p.sql[p.pos] == ')' {
			p.pos++
			break
		}
		if p.sql[p.pos] == ',' {
			p.pos++
			p.skipSpaces()
		}
		val := p.readValue()
		vals = append(vals, val)
	}

	if len(vals) != numCols {
		return nil, fmt.Errorf("column/value count mismatch: %d columns, %d values", numCols, len(vals))
	}
	return vals, nil
}

func (p *dmlParser) parseSetClause() (map[string]string, error) {
	p.skipSpaces()
	if err := p.expect("set "); err != nil {
		return nil, err
	}

	result := make(map[string]string)
	for p.pos < p.length {
		p.skipSpaces()

		if p.hasPrefix("where ") || p.sql[p.pos] == ';' {
			break
		}

		col := p.readQuotedName()
		if col == "" {
			break
		}

		p.skipSpaces()
		if p.pos < p.length && p.sql[p.pos] == '=' {
			p.pos++ // skip '='
		}
		p.skipSpaces()

		val := p.readValue()
		if val == "Unsupported Type" {
			// skip
		} else {
			result[col] = val
		}

		p.skipSpaces()
		if p.pos < p.length && p.sql[p.pos] == ',' {
			p.pos++
		}
	}

	return result, nil
}

func (p *dmlParser) parseWhereClause() (map[string]string, error) {
	if err := p.expect("where "); err != nil {
		return nil, err
	}

	result := make(map[string]string)
	for p.pos < p.length {
		p.skipSpaces()

		if p.sql[p.pos] == ';' {
			break
		}

		col := p.readQuotedName()
		if col == "" {
			break
		}

		p.skipSpaces()

		// IS NULL
		if p.hasPrefix("IS NULL") {
			p.pos += 7
			result[col] = "NULL"
			p.skipSpaces()
			if p.hasPrefix("and ") {
				p.pos += 4
			} else if p.hasPrefix("or ") {
				p.pos += 3
			}
			continue
		}

		if p.pos < p.length && p.sql[p.pos] == '=' {
			p.pos++ // skip '='
		}
		p.skipSpaces()

		val := p.readValue()
		if val == "Unsupported Type" {
			// skip
		} else {
			result[col] = val
		}

		p.skipSpaces()
		if p.hasPrefix("and ") {
			p.pos += 4
		} else if p.hasPrefix("or ") {
			p.pos += 3
		}
	}

	return result, nil
}

func (p *dmlParser) readQuotedName() string {
	p.skipSpaces()
	if p.pos >= p.length || p.sql[p.pos] != '"' {
		return ""
	}
	p.pos++ // skip opening '"'
	start := p.pos
	for p.pos < p.length && p.sql[p.pos] != '"' {
		p.pos++
	}
	name := p.sql[start:p.pos]
	if p.pos < p.length {
		p.pos++ // skip closing '"'
	}
	return name
}

func (p *dmlParser) readValue() string {
	p.skipSpaces()
	if p.pos >= p.length {
		return ""
	}

	// Single-quoted string value
	if p.sql[p.pos] == '\'' {
		return p.readQuotedString()
	}

	// NULL
	if p.hasPrefix("NULL") {
		p.pos += 4
		return "NULL"
	}

	// Unsupported Type
	if p.hasPrefix("Unsupported Type") {
		p.pos += 16
		return "Unsupported Type"
	}

	// Unsupported
	if p.hasPrefix("Unsupported") {
		start := p.pos
		for p.pos < p.length && p.sql[p.pos] != ',' && p.sql[p.pos] != ';' && p.sql[p.pos] != ')' && !p.hasPrefix(" and ") && !p.hasPrefix(" where ") {
			p.pos++
		}
		return strings.TrimSpace(p.sql[start:p.pos])
	}

	// Function call (TO_TIMESTAMP, TO_DATE, TO_TIMESTAMP_TZ, etc.) or unquoted value
	start := p.pos
	nested := 0
	inQuote := false

	for p.pos < p.length {
		c := p.sql[p.pos]

		if inQuote {
			if c == '\'' {
				if p.pos+1 < p.length && p.sql[p.pos+1] == '\'' {
					p.pos += 2
					continue
				}
				inQuote = false
			}
			p.pos++
			continue
		}

		if c == '\'' {
			inQuote = true
			p.pos++
			continue
		}

		if c == '(' {
			nested++
			p.pos++
			continue
		}
		if c == ')' {
			if nested > 0 {
				nested--
				p.pos++
				continue
			}
			break
		}

		if nested == 0 {
			// Check for concatenation operator
			if c == ' ' && p.pos+1 < p.length && p.sql[p.pos+1] == '|' {
				p.pos++
				continue
			}
			if c == '|' && p.pos+1 < p.length && p.sql[p.pos+1] == '|' {
				p.pos += 2
				p.skipSpaces()
				// Read next part of concatenated value
				continue
			}

			if c == ',' || c == ';' {
				break
			}
			if c == ' ' {
				// Check if next token is a keyword
				if p.hasPrefix(" and ") || p.hasPrefix(" where ") || p.hasPrefix(" set ") {
					break
				}
			}
		}

		p.pos++
	}

	return strings.TrimSpace(p.sql[start:p.pos])
}

func (p *dmlParser) readQuotedString() string {
	if p.pos >= p.length || p.sql[p.pos] != '\'' {
		return ""
	}

	start := p.pos
	p.pos++ // skip opening quote

	for p.pos < p.length {
		if p.sql[p.pos] == '\'' {
			if p.pos+1 < p.length && p.sql[p.pos+1] == '\'' {
				// escaped quote
				p.pos += 2
				continue
			}
			// end of string
			p.pos++

			// Check for concatenation
			saved := p.pos
			p.skipSpaces()
			if p.pos < p.length && p.sql[p.pos] == '|' && p.pos+1 < p.length && p.sql[p.pos+1] == '|' {
				p.pos += 2
				p.skipSpaces()
				rest := p.readValue()
				return p.sql[start:saved] + " || " + rest
			}
			p.pos = saved

			return p.sql[start:p.pos]
		}
		p.pos++
	}

	return p.sql[start:p.pos]
}

func (p *dmlParser) skipSpaces() {
	for p.pos < p.length && p.sql[p.pos] == ' ' {
		p.pos++
	}
}

func (p *dmlParser) hasPrefix(prefix string) bool {
	return p.pos+len(prefix) <= p.length &&
		strings.EqualFold(p.sql[p.pos:p.pos+len(prefix)], prefix)
}

func (p *dmlParser) expect(s string) error {
	if !p.hasPrefix(s) {
		end := p.pos + len(s)
		if end > p.length {
			end = p.length
		}
		return fmt.Errorf("expected %q at pos %d, got %q", s, p.pos, p.sql[p.pos:end])
	}
	p.pos += len(s)
	return nil
}
