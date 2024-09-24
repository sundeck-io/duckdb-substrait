import pandas as pd
import duckdb

def test_roundtrip_substrait(require):
    connection = require('substrait')

    connection.execute('CREATE TABLE integers (i integer)')
    connection.execute('INSERT INTO integers VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(NULL)')
    res = connection.get_substrait("select * from integers limit 5")
    proto_bytes = res.fetchone()[0]

    query_result = connection.from_substrait(proto_bytes)

    expected = pd.Series(range(5), name="i", dtype="int32")

    pd.testing.assert_series_equal(query_result.df()["i"], expected)


def test_roundtrip_proto_select_virtual_table(require):
    connection = require('substrait')

    res = connection.get_substrait("SELECT * FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9))")
    proto_bytes = res.fetchone()[0]

    query_result = connection.from_substrait(proto_bytes)

    expected = pd.Series(range(10), name="col0", dtype="int32")

    pd.testing.assert_series_equal(query_result.df()["col0"], expected)


def test_roundtrip_json_select_virtual_table(require):
    connection = require('substrait')

    res = connection.get_substrait_json("SELECT * FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9))")
    proto_bytes = res.fetchone()[0]

    query_result = connection.from_substrait_json(proto_bytes)

    expected = pd.Series(range(10), name="col0", dtype="int32")

    pd.testing.assert_series_equal(query_result.df()["col0"], expected)