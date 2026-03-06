const mysql = require("mysql2/promise");

const pools = {};

const getConnection = async (connObj) => {
  if (!connObj) return null;
  const key = `${connObj.host}:${connObj.port || 3306}:${connObj.database}:${connObj.user}`;
  if (!pools[key]) {
    const password =
      connObj.password || process.env[`SC_EXTMYSQL_PASS_${connObj.database}`];
    pools[key] = mysql.createPool({
      host: connObj.host,
      port: connObj.port || 3306,
      user: connObj.user,
      password,
      database: connObj.database,
      waitForConnections: true,
      connectionLimit: 10,
    });
  }
  return pools[key];
};

module.exports = { getConnection };
