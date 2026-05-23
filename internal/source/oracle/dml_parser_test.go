package oracle

import (
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
)

func mustParse(t *testing.T, sql string) *DmlEntry {
	t.Helper()
	parser := NewDmlParser()
	entry, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse(%q) returned error: %v", sql, err)
	}
	return entry
}

func assertNewValue(t *testing.T, entry *DmlEntry, col, expected string) {
	t.Helper()
	val, ok := entry.NewValues[col]
	if !ok {
		t.Errorf("NewValues missing column %q, have %v", col, entry.NewValues)
		return
	}
	if val != expected {
		t.Errorf("NewValues[%q] = %q, want %q", col, val, expected)
	}
}

func assertOldValue(t *testing.T, entry *DmlEntry, col, expected string) {
	t.Helper()
	val, ok := entry.OldValues[col]
	if !ok {
		t.Errorf("OldValues missing column %q, have %v", col, entry.OldValues)
		return
	}
	if val != expected {
		t.Errorf("OldValues[%q] = %q, want %q", col, val, expected)
	}
}

func assertNewValueAbsent(t *testing.T, entry *DmlEntry, col string) {
	t.Helper()
	if val, ok := entry.NewValues[col]; ok {
		t.Errorf("NewValues[%q] should be absent, got %q", col, val)
	}
}

// T01: 基本 INSERT 多列
func TestParseInsertMultiColumn(t *testing.T) {
	sql := `insert into "HR"."EMP"("ID","NAME","SALARY") values (1,'John',5000);`
	entry := mustParse(t, sql)

	if entry.Type != DmlInsert {
		t.Fatalf("Type = %v, want DmlInsert", entry.Type)
	}
	if len(entry.NewValues) != 3 {
		t.Fatalf("NewValues has %d columns, want 3", len(entry.NewValues))
	}
	assertNewValue(t, entry, "ID", "1")
	assertNewValue(t, entry, "NAME", "'John'")
	assertNewValue(t, entry, "SALARY", "5000")
}

// T02: 基本 INSERT 单列
func TestParseInsertSingleColumn(t *testing.T) {
	sql := `insert into "HR"."T"("ID") values (42);`
	entry := mustParse(t, sql)

	if entry.Type != DmlInsert {
		t.Fatalf("Type = %v, want DmlInsert", entry.Type)
	}
	assertNewValue(t, entry, "ID", "42")
}

// T03: 基本 UPDATE（SET + WHERE）
func TestParseUpdateBasic(t *testing.T) {
	sql := `update "HR"."EMP" set "NAME" = 'Jane' where "ID" = 1 and "NAME" = 'John';`
	entry := mustParse(t, sql)

	if entry.Type != DmlUpdate {
		t.Fatalf("Type = %v, want DmlUpdate", entry.Type)
	}
	assertNewValue(t, entry, "NAME", "'Jane'")
	assertOldValue(t, entry, "ID", "1")
	assertOldValue(t, entry, "NAME", "'John'")
}

// T04: 基本 DELETE
func TestParseDeleteBasic(t *testing.T) {
	sql := `delete from "HR"."EMP" where "ID" = 1 and "NAME" = 'John';`
	entry := mustParse(t, sql)

	if entry.Type != DmlDelete {
		t.Fatalf("Type = %v, want DmlDelete", entry.Type)
	}
	assertOldValue(t, entry, "ID", "1")
	assertOldValue(t, entry, "NAME", "'John'")
}

// T05: 单引号转义
func TestParseInsertEscapedQuote(t *testing.T) {
	sql := `insert into "S"."T"("C") values ('O''Brien');`
	entry := mustParse(t, sql)

	assertNewValue(t, entry, "C", "'O''Brien'")
}

// T06: 值内逗号
func TestParseInsertCommaInValue(t *testing.T) {
	sql := `insert into "S"."T"("C") values ('hello, world');`
	entry := mustParse(t, sql)

	assertNewValue(t, entry, "C", "'hello, world'")
}

// T07: TO_TIMESTAMP 函数
func TestParseUpdateToTimestamp(t *testing.T) {
	sql := `update "S"."T" set "D" = TO_TIMESTAMP('2020-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS') where "ID" = 1;`
	entry := mustParse(t, sql)

	assertNewValue(t, entry, "D", "TO_TIMESTAMP('2020-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')")
	assertOldValue(t, entry, "ID", "1")
}

// T08: TO_DATE 嵌套
func TestParseInsertToDate(t *testing.T) {
	sql := `insert into "S"."T"("ID","D") values (1,TO_DATE('2020-01-01','YYYY-MM-DD'));`
	entry := mustParse(t, sql)

	assertNewValue(t, entry, "ID", "1")
	assertNewValue(t, entry, "D", "TO_DATE('2020-01-01','YYYY-MM-DD')")
}

// T09: NULL 值 INSERT
func TestParseInsertNull(t *testing.T) {
	sql := `insert into "S"."T"("A","B") values (1,NULL);`
	entry := mustParse(t, sql)

	assertNewValue(t, entry, "A", "1")
	assertNewValue(t, entry, "B", "NULL")
}

// T10: IS NULL in WHERE
func TestParseDeleteIsNull(t *testing.T) {
	sql := `delete from "S"."T" where "A" = 1 and "B" IS NULL;`
	entry := mustParse(t, sql)

	assertOldValue(t, entry, "A", "1")
	assertOldValue(t, entry, "B", "NULL")
}

// T11: Unsupported Type
func TestParseInsertUnsupportedType(t *testing.T) {
	sql := `insert into "S"."T"("A","B") values (1,Unsupported Type);`
	entry := mustParse(t, sql)

	assertNewValue(t, entry, "A", "1")
	assertNewValueAbsent(t, entry, "B")
}

// T12: 字符串拼接
func TestParseUpdateConcatenation(t *testing.T) {
	sql := `update "S"."T" set "C" = 'abc' || 'def' where "ID" = 1;`
	entry := mustParse(t, sql)

	assertNewValue(t, entry, "C", "'abc' || 'def'")
	assertOldValue(t, entry, "ID", "1")
}

// T13: 空 WHERE
func TestParseUpdateNoWhere(t *testing.T) {
	sql := `update "S"."T" set "C" = 'v';`
	entry := mustParse(t, sql)

	if entry.Type != DmlUpdate {
		t.Fatalf("Type = %v, want DmlUpdate", entry.Type)
	}
	assertNewValue(t, entry, "C", "'v'")
	if len(entry.OldValues) != 0 {
		t.Errorf("OldValues should be empty, got %v", entry.OldValues)
	}
}

// T14: UPDATE 多列 SET
func TestParseUpdateMultiColumnSet(t *testing.T) {
	sql := `update "S"."T" set "A" = 1, "B" = 'x', "C" = NULL where "A" = 0 and "B" = 'y' and "C" = 'z';`
	entry := mustParse(t, sql)

	assertNewValue(t, entry, "A", "1")
	assertNewValue(t, entry, "B", "'x'")
	assertNewValue(t, entry, "C", "NULL")
	assertOldValue(t, entry, "A", "0")
	assertOldValue(t, entry, "B", "'y'")
	assertOldValue(t, entry, "C", "'z'")
}

// T15: 不支持的 DML 类型
func TestParseUnsupportedDml(t *testing.T) {
	parser := NewDmlParser()
	_, err := parser.Parse(`select * from "T"`)
	if err == nil {
		t.Fatal("expected error for unsupported DML, got nil")
	}
}

// T15b: 大写 DML
func TestParseUppercaseInsert(t *testing.T) {
	sql := `INSERT INTO "S"."T"("ID","NAME") VALUES (1,'John');`
	entry := mustParse(t, sql)

	if entry.Type != DmlInsert {
		t.Fatalf("Type = %v, want DmlInsert", entry.Type)
	}
	assertNewValue(t, entry, "ID", "1")
	assertNewValue(t, entry, "NAME", "'John'")
}

// T15c: 前导空白
func TestParseLeadingWhitespace(t *testing.T) {
	sql := "\n  insert into \"S\".\"T\"(\"ID\") values (1);"
	entry := mustParse(t, sql)

	if entry.Type != DmlInsert {
		t.Fatalf("Type = %v, want DmlInsert", entry.Type)
	}
	assertNewValue(t, entry, "ID", "1")
}

// T16: TO_TIMESTAMP_TZ
func TestParseUpdateToTimestampTz(t *testing.T) {
	sql := `update "S"."T" set "D" = TO_TIMESTAMP_TZ('2024-02-14 10:58:02.202590 +01:00') where "ID" = 1;`
	entry := mustParse(t, sql)

	assertNewValue(t, entry, "D", "TO_TIMESTAMP_TZ('2024-02-14 10:58:02.202590 +01:00')")
	assertOldValue(t, entry, "ID", "1")
}

// T17: TO_DATE in WHERE
func TestParseDeleteToDateInWhere(t *testing.T) {
	sql := `delete from "S"."T" where "ID" = 1 and "D" = TO_DATE('15-MAY-21', 'DD-MON-RR');`
	entry := mustParse(t, sql)

	assertOldValue(t, entry, "ID", "1")
	assertOldValue(t, entry, "D", "TO_DATE('15-MAY-21', 'DD-MON-RR')")
}

// T18: 多时间列 UPDATE
func TestParseUpdateMultipleTimestampColumns(t *testing.T) {
	sql := `update "S"."T" set "TS" = TO_TIMESTAMP('2020-02-02 00:00:00.'), "TZ" = TO_TIMESTAMP_TZ('2020-02-02 00:00:00.000000 +08:00') where "TS" = TO_TIMESTAMP('2020-02-01 00:00:00.') and "TZ" = TO_TIMESTAMP_TZ('2020-02-01 00:00:00.000000 +08:00');`
	entry := mustParse(t, sql)

	assertNewValue(t, entry, "TS", "TO_TIMESTAMP('2020-02-02 00:00:00.')")
	assertNewValue(t, entry, "TZ", "TO_TIMESTAMP_TZ('2020-02-02 00:00:00.000000 +08:00')")
	assertOldValue(t, entry, "TS", "TO_TIMESTAMP('2020-02-01 00:00:00.')")
	assertOldValue(t, entry, "TZ", "TO_TIMESTAMP_TZ('2020-02-01 00:00:00.000000 +08:00')")
}

// ---- Integration tests ----

// I01: UPDATE mergeUpdateValues 合并逻辑
func TestMergeUpdateValues(t *testing.T) {
	entry := &DmlEntry{
		Type:      DmlUpdate,
		NewValues: map[string]string{"NAME": "'Jane'"},
		OldValues: map[string]string{"ID": "1", "NAME": "'John'", "SALARY": "5000"},
	}
	mergeUpdateValues(entry)

	if entry.NewValues["ID"] != "1" {
		t.Errorf("NewValues[ID] = %q, want %q", entry.NewValues["ID"], "1")
	}
	if entry.NewValues["NAME"] != "'Jane'" {
		t.Errorf("NewValues[NAME] should stay as SET value, got %q", entry.NewValues["NAME"])
	}
	if entry.NewValues["SALARY"] != "5000" {
		t.Errorf("NewValues[SALARY] = %q, want %q (copied from OldValues)", entry.NewValues["SALARY"], "5000")
	}
}

// I02: entryToRowData 类型转换
func TestEntryToRowData(t *testing.T) {
	vals := map[string]string{
		"A": "NULL",
		"B": "'hello'",
		"C": "42",
		"D": "TO_TIMESTAMP('2020-01-01 00:00:00.')",
	}
	rd := entryToRowData(vals)

	fieldA, _ := rd.GetField("A")
	if fieldA.Value != nil {
		t.Errorf("A should be nil for NULL, got %v", fieldA.Value)
	}

	fieldB, _ := rd.GetField("B")
	if fieldB.Value != "hello" {
		t.Errorf("B = %v, want %q", fieldB.Value, "hello")
	}

	fieldC, _ := rd.GetField("C")
	if fieldC.Value != int64(42) {
		t.Errorf("C = %v (%T), want int64(42)", fieldC.Value, fieldC.Value)
	}

	fieldD, _ := rd.GetField("D")
	if fieldD.Value != "TO_TIMESTAMP('2020-01-01 00:00:00.')" {
		t.Errorf("D = %v, want raw function string", fieldD.Value)
	}
}

// I03: UPDATE ChangeEvent 同时有 After 和 Before
func TestUpdateChangeEventHasBothBeforeAndAfter(t *testing.T) {
	sql := `update "HR"."EMP" set "NAME" = 'Jane' where "ID" = 1 and "NAME" = 'John' and "SALARY" = 5000;`
	parser := NewDmlParser()
	entry, err := parser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	mergeUpdateValues(entry)

	after := entryToRowData(entry.NewValues)
	before := entryToRowData(entry.OldValues)

	// After should have all columns (NAME from SET, ID+SALARY merged from WHERE)
	checkField := func(rd event.RowData, name string) {
		if _, ok := rd.GetField(name); !ok {
			t.Errorf("RowData missing field %q", name)
		}
	}
	checkField(after, "ID")
	checkField(after, "NAME")
	checkField(after, "SALARY")
	checkField(before, "ID")
	checkField(before, "NAME")
	checkField(before, "SALARY")

	// NAME should differ between before and after
	afterName, _ := after.GetField("NAME")
	beforeName, _ := before.GetField("NAME")
	if afterName.Value == beforeName.Value {
		t.Errorf("After.NAME should differ from Before.NAME, both are %v", afterName.Value)
	}
}
