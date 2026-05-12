// Package filter provides event filtering for DataStream.
package filter

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/UFOXD/datastream/pkg/event"
)

// ExpressionFilter filters events based on complex expressions.
// Supports comparison operators, logical operators, and field access.
type ExpressionFilter struct {
	expression string
	evaluator  func(*event.ChangeEvent) (bool, error)
}

// ExpressionConfig holds configuration for expression filter.
type ExpressionConfig struct {
	// Expression is the filter expression
	// Examples:
	//   - "table == 'users'"
	//   - "database == 'inventory' && operation == 'insert'"
	//   - "after.age > 18"
	//   - "after.status == 'active' || after.status == 'pending'"
	Expression string `json:"expression" toml:"expression"`
}

// NewExpressionFilter creates a new expression-based filter.
func NewExpressionFilter(cfg *ExpressionConfig) (*ExpressionFilter, error) {
	eval, err := parseExpression(cfg.Expression)
	if err != nil {
		return nil, fmt.Errorf("failed to parse expression: %w", err)
	}

	return &ExpressionFilter{
		expression: cfg.Expression,
		evaluator:  eval,
	}, nil
}

// Filter evaluates the expression against the event.
func (ef *ExpressionFilter) Filter(e *event.ChangeEvent) (bool, error) {
	return ef.evaluator(e)
}

// Expression returns the filter expression.
func (ef *ExpressionFilter) Expression() string {
	return ef.expression
}

// parseExpression parses an expression string into an evaluator function.
func parseExpression(expr string) (func(*event.ChangeEvent) (bool, error), error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return func(*event.ChangeEvent) (bool, error) { return true, nil }, nil
	}

	// Handle logical operators
	if strings.Contains(expr, "&&") {
		return parseAndExpression(expr)
	}
	if strings.Contains(expr, "||") {
		return parseOrExpression(expr)
	}

	// Handle comparison operators
	return parseComparison(expr)
}

// parseAndExpression parses && expressions.
func parseAndExpression(expr string) (func(*event.ChangeEvent) (bool, error), error) {
	parts := strings.Split(expr, "&&")
	var evaluators []func(*event.ChangeEvent) (bool, error)

	for _, part := range parts {
		eval, err := parseExpression(strings.TrimSpace(part))
		if err != nil {
			return nil, err
		}
		evaluators = append(evaluators, eval)
	}

	return func(e *event.ChangeEvent) (bool, error) {
		for _, eval := range evaluators {
			result, err := eval(e)
			if err != nil {
				return false, err
			}
			if !result {
				return false, nil
			}
		}
		return true, nil
	}, nil
}

// parseOrExpression parses || expressions.
func parseOrExpression(expr string) (func(*event.ChangeEvent) (bool, error), error) {
	parts := strings.Split(expr, "||")
	var evaluators []func(*event.ChangeEvent) (bool, error)

	for _, part := range parts {
		eval, err := parseExpression(strings.TrimSpace(part))
		if err != nil {
			return nil, err
		}
		evaluators = append(evaluators, eval)
	}

	return func(e *event.ChangeEvent) (bool, error) {
		for _, eval := range evaluators {
			result, err := eval(e)
			if err != nil {
				return false, err
			}
			if result {
				return true, nil
			}
		}
		return false, nil
	}, nil
}

// parseComparison parses a single comparison expression.
func parseComparison(expr string) (func(*event.ChangeEvent) (bool, error), error) {
	// Define supported operators
	operators := []string{"==", "!=", ">=", "<=", ">", "<", "=~", "!~"}

	for _, op := range operators {
		if strings.Contains(expr, op) {
			parts := strings.SplitN(expr, op, 2)
			if len(parts) != 2 {
				continue
			}

			left := strings.TrimSpace(parts[0])
			right := strings.TrimSpace(parts[1])

			return createComparisonEvaluator(left, op, right)
		}
	}

	return nil, fmt.Errorf("no valid operator found in expression: %s", expr)
}

// createComparisonEvaluator creates an evaluator for a comparison.
func createComparisonEvaluator(left, op, right string) (func(*event.ChangeEvent) (bool, error), error) {
	// Parse right value (can be string literal, number, or field reference)
	rightValue, err := parseValue(right)
	if err != nil {
		return nil, err
	}

	return func(e *event.ChangeEvent) (bool, error) {
		leftVal, err := getFieldValue(e, left)
		if err != nil {
			return false, err
		}

		return compareValues(leftVal, op, rightValue, right)
	}, nil
}

// parseValue parses a value from the expression.
func parseValue(s string) (interface{}, error) {
	s = strings.TrimSpace(s)

	// String literal (quoted)
	if len(s) >= 2 && (s[0] == '\'' || s[0] == '"') {
		return s[1 : len(s)-1], nil
	}

	// Boolean
	if s == "true" {
		return true, nil
	}
	if s == "false" {
		return false, nil
	}

	// Null
	if s == "null" || s == "nil" {
		return nil, nil
	}

	// Number (int or float)
	if strings.Contains(s, ".") {
		var f float64
		_, err := fmt.Sscanf(s, "%f", &f)
		return f, err
	}

	var i int64
	_, err := fmt.Sscanf(s, "%d", &i)
	if err == nil {
		return i, nil
	}

	// Otherwise, it's a field reference - return as-is
	return s, nil
}

// getFieldValue retrieves a field value from an event.
// Supports: table, database, schema, operation, before.*, after.*
func getFieldValue(e *event.ChangeEvent, field string) (interface{}, error) {
	parts := strings.Split(field, ".")

	switch parts[0] {
	case "table":
		return e.Table.Table, nil
	case "database":
		return e.Table.Database, nil
	case "schema":
		return e.Table.Schema, nil
	case "operation", "op":
		return string(e.Type), nil
	case "before":
		if len(parts) < 2 {
			return nil, fmt.Errorf("before.* requires a field name")
		}
		if e.Before.IsEmpty() {
			return nil, nil
		}
		f, ok := e.Before.Fields[parts[1]]
		if !ok {
			return nil, nil
		}
		return f.Value, nil
	case "after":
		if len(parts) < 2 {
			return nil, fmt.Errorf("after.* requires a field name")
		}
		if e.After.IsEmpty() {
			return nil, nil
		}
		f, ok := e.After.Fields[parts[1]]
		if !ok {
			return nil, nil
		}
		return f.Value, nil
	default:
		// Try to get from after fields first, then before
		if !e.After.IsEmpty() {
			if v, ok := e.After.Fields[parts[0]]; ok {
				return v.Value, nil
			}
		}
		if !e.Before.IsEmpty() {
			if v, ok := e.Before.Fields[parts[0]]; ok {
				return v.Value, nil
			}
		}
		return nil, fmt.Errorf("unknown field: %s", field)
	}
}

// compareValues compares two values with the given operator.
// For regex operators, rightValue should be the pattern string.
func compareValues(left interface{}, op string, right interface{}, rightStr string) (bool, error) {
	switch op {
	case "==":
		return reflect.DeepEqual(left, right), nil
	case "!=":
		return !reflect.DeepEqual(left, right), nil
	case ">=", "<=", ">", "<":
		return compareNumbers(left, op, right)
	case "=~":
		// Regex match - use rightValue (parsed, without quotes) as pattern
		pattern, ok := right.(string)
		if !ok {
			return false, fmt.Errorf("regex pattern must be string, got %T", right)
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return false, fmt.Errorf("invalid regex: %w", err)
		}
		leftStr := fmt.Sprintf("%v", left)
		return re.MatchString(leftStr), nil
	case "!~":
		// Regex not match - use rightValue (parsed, without quotes) as pattern
		pattern, ok := right.(string)
		if !ok {
			return false, fmt.Errorf("regex pattern must be string, got %T", right)
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return false, fmt.Errorf("invalid regex: %w", err)
		}
		leftStr := fmt.Sprintf("%v", left)
		return !re.MatchString(leftStr), nil
	default:
		return false, fmt.Errorf("unknown operator: %s", op)
	}
}

// compareNumbers compares two numeric values.
func compareNumbers(left interface{}, op string, right interface{}) (bool, error) {
	leftNum, err := toFloat64(left)
	if err != nil {
		return false, err
	}

	rightNum, err := toFloat64(right)
	if err != nil {
		return false, err
	}

	switch op {
	case ">=":
		return leftNum >= rightNum, nil
	case "<=":
		return leftNum <= rightNum, nil
	case ">":
		return leftNum > rightNum, nil
	case "<":
		return leftNum < rightNum, nil
	default:
		return false, fmt.Errorf("unknown operator: %s", op)
	}
}

// toFloat64 converts a value to float64.
func toFloat64(v interface{}) (float64, error) {
	switch val := v.(type) {
	case int:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case float32:
		return float64(val), nil
	case float64:
		return val, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}
