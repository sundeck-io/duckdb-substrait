#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "substrait_extension.hpp"


using namespace duckdb;
using namespace std;

int main(int argc, char* argv[]) {
    // Call Catch2's session to run tests
    return Catch::Session().run(argc, argv);
}

TEST_CASE("Test C Get and To Substrait API", "[substrait-api]") {
  DuckDB db(nullptr);
  SubstraitExtension substrait_extension;
  substrait_extension.Load(db);
  Connection con(db);
  con.EnableQueryVerification();
  // create the database
  REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
  REQUIRE_NO_FAIL(
      con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

  auto proto = con.GetSubstrait("select * from integers limit 2");
  auto result = con.FromSubstrait(proto);

  REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));

  REQUIRE_THROWS(con.GetSubstrait("select * from p"));

  REQUIRE_THROWS(con.FromSubstrait("this is not valid"));
}

TEST_CASE("Test C Get and To Json-Substrait API", "[substrait-api]") {
  DuckDB db(nullptr);
  SubstraitExtension substrait_extension;
  substrait_extension.Load(db);
  Connection con(db);
  con.EnableQueryVerification();
  // create the database
  REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
  REQUIRE_NO_FAIL(
      con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

  auto json = con.GetSubstraitJSON("select * from integers limit 2");
  auto result = con.FromSubstraitJSON(json);

  REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));

  REQUIRE_THROWS(con.GetSubstraitJSON("select * from p"));

  REQUIRE_THROWS(con.FromSubstraitJSON("this is not valid"));
}

TEST_CASE("Test C Get and To Substrait API for Insert from select", "[substrait-api]") {
    DuckDB db(nullptr);
    SubstraitExtension substrait_extension;
    substrait_extension.Load(db);
    Connection con(db);
    con.EnableQueryVerification();
    // create second table
    // create first table and populate data
    REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(i INTEGER)"));
    REQUIRE_NO_FAIL(
    con.Query("INSERT INTO t1 VALUES (1), (2), (3), (NULL)"));

    REQUIRE_NO_FAIL(con.Query("CREATE TABLE t2(i INTEGER)"));

    // Issue substrait query for insert as select
    auto proto = con.GetSubstrait("insert into t2 from (select * from t1)");
    auto result = con.FromSubstrait(proto);

    // number of rows inserted are expected as result of insert
    REQUIRE(CHECK_COLUMN(result, 0, {4}));
}

TEST_CASE("Test C Get and To Json-Substrait API for Insert from select", "[substrait-api]") {
    DuckDB db(nullptr);
    SubstraitExtension substrait_extension;
    substrait_extension.Load(db);
    Connection con(db);
    con.EnableQueryVerification();
    // create second table
    // create first table and populate data
    REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(i INTEGER)"));
    REQUIRE_NO_FAIL(
    con.Query("INSERT INTO t1 VALUES (1), (2), (3), (NULL)"));

    REQUIRE_NO_FAIL(con.Query("CREATE TABLE t2(i INTEGER)"));
    // Issue substrait query for insert as select
    auto json = con.GetSubstraitJSON("insert into t2 from (select * from t1)");
    // round trip
    auto result = con.FromSubstraitJSON(json);

    // number of rows inserted are expected as result of insert
    REQUIRE(CHECK_COLUMN(result, 0, {4}));
}

TEST_CASE("Test C Get and To Substrait API for Insert from virtual table", "[substrait-api]") {
    DuckDB db(nullptr);
    SubstraitExtension substrait_extension;
    substrait_extension.Load(db);
    Connection con(db);
    con.EnableQueryVerification();

    REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(i INTEGER)"));

    auto proto = con.GetSubstrait("INSERT INTO t1 VALUES (1), (2), (3), (NULL)");
    auto result = con.FromSubstrait(proto);
    // number of rows inserted are expected as result of insert
    REQUIRE(CHECK_COLUMN(result, 0, {4}));
}

TEST_CASE("Test C Get and To JSON-Substrait API for Insert from virtual table", "[substrait-api]") {
    DuckDB db(nullptr);
    SubstraitExtension substrait_extension;
    substrait_extension.Load(db);
    Connection con(db);
    con.EnableQueryVerification();

    REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(i INTEGER)"));

    auto json = con.GetSubstraitJSON("INSERT INTO t1 VALUES (1), (2), (3), (NULL)");
    auto result = con.FromSubstraitJSON(json);
    // number of rows inserted are expected as result of insert
    REQUIRE(CHECK_COLUMN(result, 0, {4}));
}

TEST_CASE("Test C Get and To Substrait API for Select from virtual table", "[substrait-api]") {
    DuckDB db(nullptr);
    SubstraitExtension substrait_extension;
    substrait_extension.Load(db);
    Connection con(db);
    con.EnableQueryVerification();


    auto json = con.GetSubstrait("SELECT * FROM (VALUES (1, 2), (3, 4))");
    auto result = con.FromSubstrait(json);
    // number of rows selected are expected as result of insert
    REQUIRE(CHECK_COLUMN(result, 0, {1, 3}));
    REQUIRE(CHECK_COLUMN(result, 1, {2, 4}));
}

TEST_CASE("Test C Get and To JSON-Substrait API for Select from virtual table", "[substrait-api]") {
    DuckDB db(nullptr);
    SubstraitExtension substrait_extension;
    substrait_extension.Load(db);
    Connection con(db);
    con.EnableQueryVerification();


    auto json = con.GetSubstraitJSON("SELECT * FROM (VALUES (1, 2), (3, 4))");
    auto result = con.FromSubstraitJSON(json);
    // number of rows selected are expected as result of insert
    REQUIRE(CHECK_COLUMN(result, 0, {1, 3}));
    REQUIRE(CHECK_COLUMN(result, 1, {2, 4}));
}