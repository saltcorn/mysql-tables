const { getConnection } = require("./connections");

module.exports = {
  sc_plugin_api_version: 1,
  table_providers: require("./table-provider"),
  viewtemplates: [require("./database-browser")],
  functions: {
    extMySqlQuery: {
      run: async (connection, query, parameters) => {
        const sql_log = (...args) => {
          console.log(...args);
        };
        const pool = await getConnection(connection);
        const conn = await pool.getConnection();
        try {
          sql_log("START TRANSACTION READ ONLY;");
          await conn.query("START TRANSACTION READ ONLY;");

          sql_log(query, parameters || []);
          const [rows, fields] = await conn.query(query, parameters || []);

          sql_log("ROLLBACK;");
          await conn.query("ROLLBACK");
          return { rows, fields };
        } finally {
          conn.release();
        }
      },
      isAsync: true,
      description:
        "Run a read-only SQL query on an external MySQL/MariaDB database",
      arguments: [
        {
          name: "connection",
          type: "JSON",
          tstype:
            "{host: string, port: number, user: string, password: string, database: string}",
          required: true,
        },
        { name: "sql_query", type: "String", required: true },
        { name: "parameters", type: "JSON", tstype: "any[]" },
      ],
    },
  },
};
