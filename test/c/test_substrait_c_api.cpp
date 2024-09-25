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

TEST_CASE("Test C SelectFromValues with Substrait API", "[substrait-api]") {
  DuckDB db(nullptr);
  Connection con(db);
  con.EnableQueryVerification();

  auto json = con.GetSubstraitJSON("SELECT 13 as id, 17 as age");

  auto result = con.FromSubstraitJSON(json);

  REQUIRE(CHECK_COLUMN(result, 0, {13}));
  REQUIRE(CHECK_COLUMN(result, 1, {17}));
}

TEST_CASE("Test C CTAS Basic with Substrait API", "[substrait-api]") {
  DuckDB db(nullptr);
  Connection con(db);

  auto proto = con.GetSubstrait("create table t1 as SELECT * FROM (VALUES ('john', 25), ('jane', 21)) AS t(name, age)");
  auto result = con.FromSubstrait(proto);

  REQUIRE_NO_FAIL(con.Query("SELECT * from t1"));

  auto json_plan = con.GetSubstraitJSON("create table t2 as SELECT * FROM t1");
  result = con.FromSubstraitJSON(json_plan);

  proto = con.GetSubstrait("select * from t2 limit 2");
  result = con.FromSubstrait(proto);

  REQUIRE(CHECK_COLUMN(result, 0, {"john", "jane"}));
  REQUIRE(CHECK_COLUMN(result, 1, {25, 21}));
}


void CreateEmployeeTable(Connection& con) {
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE employees ("
					  "employee_id INTEGER PRIMARY KEY, "
					  "name VARCHAR(100), "
					  "department_id INTEGER, "
					  "salary DECIMAL(10, 2))"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO employees VALUES "
						  "(1, 'John Doe', 1, 120000), "
						  "(2, 'Jane Smith', 2, 80000), "
						  "(3, 'Alice Johnson', 1, 50000), "
						  "(4, 'Bob Brown', 3, 95000), "
						  "(5, 'Charlie Black', 2, 60000)"));
}

void CreatePartTimeEmployeeTable(Connection& con) {
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE part_time_employees ("
					  "id INTEGER PRIMARY KEY, "
					  "name VARCHAR(100), "
					  "department_id INTEGER, "
					  "hourly_rate DECIMAL(10, 2))"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO part_time_employees VALUES "
						  "(6, 'David White', 1, 30000), "
						  "(7, 'Eve Green', 2, 40000)"));
}

void CreateDepartmentsTable(Connection& con) {
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE departments (department_id INTEGER PRIMARY KEY, department_name VARCHAR(100))"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO departments VALUES "
						  "(1, 'HR'), "
						  "(2, 'Engineering'), "
						  "(3, 'Finance')"));
}

TEST_CASE("Test C CTAS Select columns with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	auto proto = con.GetSubstrait("CREATE TABLE employee_salaries AS "
		"SELECT name, salary FROM employees"
	);
	auto result = con.FromSubstrait(proto);

	REQUIRE_NO_FAIL(con.Query("SELECT * from employee_salaries"));
}

TEST_CASE("Test C CTAS Filter with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	auto proto = con.GetSubstrait("CREATE TABLE filtered_employees AS "
		"SELECT * FROM employees "
		"WHERE salary > 80000;"
	);
	auto result = con.FromSubstrait(proto);

	REQUIRE_NO_FAIL(con.Query("SELECT * from filtered_employees"));
}

TEST_CASE("Test C CTAS Case_When with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	auto proto = con.GetSubstrait("CREATE TABLE categorized_employees AS "
		"SELECT name, "
		"CASE "
			"WHEN salary > 100000 THEN 'High' "
			"WHEN salary BETWEEN 50000 AND 100000 THEN 'Medium' "
			"ELSE 'Low' "
		"END AS salary_category "
		"FROM employees"
	);
	auto result = con.FromSubstrait(proto);

	REQUIRE_NO_FAIL(con.Query("SELECT * from categorized_employees"));
}

TEST_CASE("Test C CTAS OrderBy with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	auto proto = con.GetSubstrait("CREATE TABLE ordered_employees AS "
		"SELECT * FROM employees "
		"ORDER BY salary DESC"
	);
	auto result = con.FromSubstrait(proto);

	REQUIRE_NO_FAIL(con.Query("SELECT * from ordered_employees"));
}

TEST_CASE("Test C CTAS SubQuery with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	auto proto = con.GetSubstrait("CREATE TABLE high_salary_employees AS "
		"SELECT * "
		"FROM ( "
			"SELECT employee_id, name, salary "
			"FROM employees "
			"WHERE salary > 100000)"
	);
	auto result = con.FromSubstrait(proto);

	REQUIRE_NO_FAIL(con.Query("SELECT * from high_salary_employees"));
}

TEST_CASE("Test C CTAS Distinct with Substrait API", "[substrait-api]") {
	SKIP_TEST("unsupported child type in Distinct operator");  // TODO fix TransformDistinct
	return;

	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);
	auto proto = con.GetSubstrait("CREATE TABLE unique_departments AS "
		"SELECT DISTINCT department_id FROM employees"
	);
	auto result = con.FromSubstrait(proto);

	REQUIRE_NO_FAIL(con.Query("SELECT * from unique_departments"));
}

TEST_CASE("Test C CTAS Aggregation with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	auto proto = con.GetSubstrait("CREATE TABLE department_summary AS "
		"SELECT department_id, COUNT(*) AS employee_count "
		"FROM employees "
		"GROUP BY department_id"
	);
	auto result = con.FromSubstrait(proto);

	REQUIRE_NO_FAIL(con.Query("SELECT * from department_summary"));
}

TEST_CASE("Test C CTAS Join with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);
	CreateDepartmentsTable(con);

	auto proto = con.GetSubstrait("CREATE TABLE employee_departments AS "
		"SELECT e.employee_id, e.name, d.department_name "
		"FROM employees e "
		"JOIN departments d "
		"ON e.department_id = d.department_id"
	);
	auto result = con.FromSubstrait(proto);

	REQUIRE_NO_FAIL(con.Query("SELECT * from employee_departments"));
}

TEST_CASE("Test C CTAS Union with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);
	CreatePartTimeEmployeeTable(con);

	auto proto = con.GetSubstrait("CREATE TABLE all_employees AS "
		"SELECT employee_id, name, department_id, salary "
		"FROM employees "
		"UNION "
		"SELECT id, name, department_id, hourly_rate * 2000 AS salary "
		"FROM part_time_employees"
	);
	auto result = con.FromSubstrait(proto);

	REQUIRE_NO_FAIL(con.Query("SELECT * from all_employees"));
}