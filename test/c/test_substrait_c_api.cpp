#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/connection_manager.hpp"

#include <chrono>
#include <thread>

using namespace duckdb;
using namespace std;

TEST_CASE("Test C Get and To Substrait API", "[substrait-api]") {
  DuckDB db(nullptr);
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

TEST_CASE("Test C SelectFromValues Get and To Substrait API", "[substrait-api]") {
  DuckDB db(nullptr);
  Connection con(db);
  con.EnableQueryVerification();

  auto json = con.GetSubstraitJSON("SELECT 13 as id, 17 as age");

  auto result = con.FromSubstraitJSON(json);

  REQUIRE(CHECK_COLUMN(result, 0, {13}));
  REQUIRE(CHECK_COLUMN(result, 1, {17}));
}

TEST_CASE("Test C CTAS Get and To Substrait API", "[substrait-api]") {
  DuckDB db(nullptr);
  Connection con(db);

  auto proto = con.GetSubstrait("create table t1 as SELECT * FROM (VALUES ('john', 25), ('jane', 21))");
  auto result = con.FromSubstrait(proto);

  REQUIRE_NO_FAIL(con.Query("SELECT * from t1"));

  auto json_plan = con.GetSubstraitJSON("create table t2 as SELECT * FROM t1");
  result = con.FromSubstraitJSON(json_plan);

  proto = con.GetSubstrait("select * from t2 limit 2");
  result = con.FromSubstrait(proto);

  REQUIRE(CHECK_COLUMN(result, 0, {"john", "jane"}));
  REQUIRE(CHECK_COLUMN(result, 1, {25, 21}));
}