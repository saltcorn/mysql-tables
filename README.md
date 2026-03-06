# mysql-tables
Table provider for remote MySQL and MariaDB tables

This module contains a Saltcorn table provider for remote MySQL and MariaDB tables. Use this to access a table on a different database as if it were a normal Saltcorn table.

There are two ways of setting up a remote table:

* The standard way is to create a table and select the "MySQL remote table" as the table provider. In the configuration, you will on the first screen select the database connection parameters (host URL, port, username, password, database name, and table name). On the second page you can adjust the fields that have been guessed from connecting to the database. This way works fine if you are only importing a single table.

* If you are importing multiple tables and there are relations between them, it is easier to run the "MySQL Database Explorer" view, which is only available to the administrator and will be in your list of views. Here you also enter the database connection parameters, but not the table name. When you have entered the other connection parameters, press "Look up tables" and a list of the tables will appear. Here, select all of the tables you would like to import and then click "Import tables". This means you don't have to enter the connection parameters multiple times, and it will also correctly set up any relations as foreign key fields between the imported tables.

If you have added fields to the remote table after doing an import, simply use the "MySQL Database Explorer" to import these tables again. The list of fields will be updated.

If you would prefer not to have your database connection password stored in the database you can set this up as an environment variable, as indicated on the sublabel for the password connection parameter. You should set the `SC_EXTMYSQL_PASS_{database name}` environment variable. For instance, if your database is called `testdb1` then set the `SC_EXTMYSQL_PASS_testdb1` environment variable.

## Query function

This module also provides the `extMySqlQuery` function for use in code actions. It takes a connection object, an SQL query string, and optionally an array of parameters. Queries are executed in a read-only transaction for safety. Example usage:

```javascript
const result = await extMySqlQuery(
  { host: "db.example.com", port: 3306, user: "reader", password: "secret", database: "mydb" },
  "SELECT * FROM orders WHERE status = ?",
  ["pending"]
);
// result.rows contains the query results
```
