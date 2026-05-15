package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatchPattern_ExactMatch(t *testing.T) {
	tests := []struct {
		pattern string
		s       string
		want    bool
	}{
		{"table1", "table1", true},
		{"table1", "table2", false},
		{"", "", true},
		{"abc", "abc", true},
		{"abc", "abcd", false},
	}

	for _, tt := range tests {
		got := matchPattern(tt.pattern, tt.s)
		require.Equal(t, tt.want, got, "matchPattern(%q, %q)", tt.pattern, tt.s)
	}
}

func TestMatchPattern_WildcardStar(t *testing.T) {
	tests := []struct {
		pattern string
		s       string
		want    bool
	}{
		// Single * matches everything
		{"*", "", true},
		{"*", "anything", true},
		{"*", "table_name_123", true},

		// * at end
		{"table*", "table", true},
		{"table*", "table1", true},
		{"table*", "table_name", true},
		{"table*", "tab", false},

		// * at beginning
		{"*_suffix", "abc_suffix", true},
		{"*_suffix", "xyz_suffix", true},
		{"*_suffix", "_suffix", true}, // * matches empty, then _suffix matches
		{"*_suffix", "suffix", false}, // _suffix != suffix (underscore required)
		{"*_suffix", "abc_other", false},

		// * in middle
		{"db*_table", "db1_table", true},
		{"db*_table", "db123_table", true},
		{"db*_table", "db_table", true}, // * matches empty
		{"db*_table", "db_other", false},

		// Multiple *
		{"*_*", "a_b", true},
		{"*_*", "abc_xyz", true},
		{"*_*", "ab", false},
		{"*_*_*", "a_b_c", true},
	}

	for _, tt := range tests {
		got := matchPattern(tt.pattern, tt.s)
		require.Equal(t, tt.want, got, "matchPattern(%q, %q)", tt.pattern, tt.s)
	}
}

func TestMatchPattern_WildcardQuestion(t *testing.T) {
	tests := []struct {
		pattern string
		s       string
		want    bool
	}{
		// Single ?
		{"?", "a", true},
		{"?", "", false},
		{"?", "ab", false},

		// ? in pattern
		{"table?", "table1", true},
		{"table?", "tableA", true},
		{"table?", "table", false},
		{"table?", "table12", false},

		// Multiple ?
		{"??", "ab", true},
		{"???", "abc", true},
		{"???", "ab", false},

		// Mix of * and ?
		{"*?", "a", true},
		{"*?", "abc", true},
		{"*?", "", false},
		{"?*", "a", true},
		{"?*", "abc", true},
		{"?*", "", false},
	}

	for _, tt := range tests {
		got := matchPattern(tt.pattern, tt.s)
		require.Equal(t, tt.want, got, "matchPattern(%q, %q)", tt.pattern, tt.s)
	}
}

func TestMatchPattern_ComplexPatterns(t *testing.T) {
	tests := []struct {
		pattern string
		s       string
		want    bool
	}{
		// Database.Table patterns
		{"mydb.*", "mydb.table1", true},
		{"mydb.*", "mydb.", true}, // * matches empty
		{"mydb.*", "mydb", false},

		// Wildcard database
		{"*_db.users", "prod_db.users", true},
		{"*_db.users", "test_db.users", true},
		{"*_db.users", "prod_db.orders", false},

		// Table patterns with prefix
		{"t_*_2024", "t_users_2024", true},
		{"t_*_2024", "t_orders_2024", true},
		{"t_*_2024", "t_users_2023", false},

		// Complex multi-wildcard
		{"*_*_*", "a_b_c", true},
		{"*_*_*", "prod_db_table1", true},

		// Edge cases
		{"a*b*c", "abc", true},    // * matches empty in both places
		{"a*b*c", "aXbYc", true},  // * matches X and Y
		{"a*b*c", "aXbYcd", false}, // d doesn't match
	}

	for _, tt := range tests {
		got := matchPattern(tt.pattern, tt.s)
		require.Equal(t, tt.want, got, "matchPattern(%q, %q)", tt.pattern, tt.s)
	}
}

func TestWildcardMatch_Performance(t *testing.T) {
	// Test with longer strings to ensure algorithm is efficient
	pattern := "prefix_*_suffix"
	s := "prefix_middle_content_suffix"

	for i := 0; i < 100; i++ {
		got := wildcardMatch(pattern, s)
		require.True(t, got)
	}
}
