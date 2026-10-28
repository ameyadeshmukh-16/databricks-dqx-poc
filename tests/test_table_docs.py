"""Tests for the table/column comment catalogue and its idempotent application."""

from types import SimpleNamespace

from src.table_docs import (
    STREAMING_TABLES, TABLE_DOCS, apply_table_docs, sql_literal, table_statements,
)


class FakeSpark:
    """Minimal Spark stand-in: which tables/columns exist, and a log of the SQL it was asked to run."""

    def __init__(self, tables, schema_exists=True, fail_on=None):
        self.tables = tables                      # {"name": [columns]}
        self.schema_exists = schema_exists
        self.fail_on = fail_on
        self.statements = []
        self.catalog = SimpleNamespace(
            databaseExists=lambda name: self.schema_exists,
            tableExists=lambda fqn: fqn.rsplit(".", 1)[-1] in self.tables,
        )

    def table(self, fqn):
        return SimpleNamespace(columns=self.tables[fqn.rsplit(".", 1)[-1]])

    def sql(self, statement):
        if self.fail_on and self.fail_on in statement:
            raise RuntimeError("boom\nstack")
        self.statements.append(statement)


def test_literals_escape_quotes_and_backslashes():
    assert sql_literal("it's") == "'it\\'s'"
    assert sql_literal("a\\b") == "'a\\\\b'"


def test_statements_cover_the_table_and_only_columns_that_exist():
    doc = {"comment": "A table", "columns": {"a": "col a", "b": "col b", "gone": "not there"}}
    stmts = table_statements("c.s.t", doc, ["a", "B"])
    assert [label for label, _ in stmts] == ["c.s.t", "c.s.t.a", "c.s.t.B"]       # case-insensitive match, real name used
    assert stmts[0][1] == "COMMENT ON TABLE c.s.t IS 'A table'"
    assert stmts[1][1] == "ALTER TABLE c.s.t ALTER COLUMN `a` COMMENT 'col a'"


def test_a_comment_with_a_quote_cannot_break_the_statement():
    stmts = table_statements("c.s.t", {"comment": "it's \\ fine", "columns": {}}, [])
    assert stmts[0][1] == "COMMENT ON TABLE c.s.t IS 'it\\'s \\\\ fine'"


def test_applying_documents_existing_tables_and_reports_the_missing_ones():
    spark = FakeSpark({"stream_batch_metrics": ["batch_id", "clean_count", "extra"]})
    result = apply_table_docs(spark, "c", "s", {k: TABLE_DOCS[k] for k in ("stream_batch_metrics", "dq_alert_log")})
    assert result["documented"] == ["stream_batch_metrics"] and result["missing"] == ["dq_alert_log"]
    joined = "\n".join(spark.statements)
    assert "COMMENT ON SCHEMA c.s" in joined
    assert "ALTER COLUMN `batch_id`" in joined and "ALTER COLUMN `clean_count`" in joined
    assert "`extra`" not in joined                                                # undocumented column left alone


def test_applying_twice_is_harmless():
    spark = FakeSpark({"stream_batch_metrics": ["batch_id"]})
    docs = {"stream_batch_metrics": TABLE_DOCS["stream_batch_metrics"]}
    apply_table_docs(spark, "c", "s", docs)
    first = list(spark.statements)
    apply_table_docs(spark, "c", "s", docs)
    assert spark.statements == first + first


def test_a_missing_schema_means_everything_is_still_to_do():
    result = apply_table_docs(FakeSpark({}, schema_exists=False), "c", "s", {"a": {"comment": "x", "columns": {}}})
    assert result == {"documented": [], "missing": ["a"], "failed": []}


def test_one_failing_column_does_not_stop_the_rest():
    spark = FakeSpark({"stream_batch_metrics": ["batch_id", "clean_count"]}, fail_on="`batch_id`")
    result = apply_table_docs(spark, "c", "s", {"stream_batch_metrics": TABLE_DOCS["stream_batch_metrics"]})
    assert result["failed"] == [("c.s.stream_batch_metrics.batch_id", "boom")]
    assert any("`clean_count`" in s for s in spark.statements)
    assert result["documented"] == []                                              # not claimed done while something failed


def test_the_catalogue_covers_every_table_the_streaming_pipeline_creates():
    assert all(name in TABLE_DOCS for name in STREAMING_TABLES)
    assert all(doc["comment"].strip() for doc in TABLE_DOCS.values())


def test_every_documented_column_has_text():
    assert all(text.strip() for doc in TABLE_DOCS.values() for text in doc["columns"].values())
