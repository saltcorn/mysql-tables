const findType = (dataType, columnType) => {
  const dt = dataType.toLowerCase();

  if (
    dt === "tinyint" &&
    columnType &&
    columnType.toLowerCase() === "tinyint(1)"
  ) {
    return "Bool";
  }

  if (
    [
      "int",
      "integer",
      "smallint",
      "mediumint",
      "bigint",
      "tinyint",
    ].includes(dt)
  ) {
    return "Integer";
  }

  if (["float", "double", "decimal", "numeric", "real"].includes(dt)) {
    return "Float";
  }

  if (
    [
      "varchar",
      "char",
      "text",
      "tinytext",
      "mediumtext",
      "longtext",
      "enum",
      "set",
    ].includes(dt)
  ) {
    return "String";
  }

  if (["date", "datetime", "timestamp"].includes(dt)) {
    return "Date";
  }

  if (dt === "json") {
    return "String";
  }

  return null;
};

const discoverable_tables = async (database, pool) => {
  const [rows] = await pool.query(
    `SELECT TABLE_NAME as table_name
     FROM INFORMATION_SCHEMA.TABLES
     WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
     ORDER BY TABLE_NAME`,
    [database],
  );
  return rows;
};

const discover_tables = async (tableNames, database, pool) => {
  const packTables = [];

  for (const tnm of tableNames) {
    const [columns] = await pool.query(
      `SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_TYPE, EXTRA
       FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
       ORDER BY ORDINAL_POSITION`,
      [database, tnm],
    );

    const [primaryKeys] = await pool.query(
      `SELECT COLUMN_NAME
       FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'`,
      [database, tnm],
    );
    const pkColumns = primaryKeys.map((r) => r.COLUMN_NAME);

    const [foreignKeys] = await pool.query(
      `SELECT COLUMN_NAME, REFERENCED_TABLE_NAME
       FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
         AND REFERENCED_TABLE_SCHEMA IS NOT NULL`,
      [database, tnm],
    );
    const fkMap = {};
    foreignKeys.forEach((fk) => {
      fkMap[fk.COLUMN_NAME] = fk.REFERENCED_TABLE_NAME;
    });

    const fields = columns
      .map((c) => {
        const isAutoIncrement =
          c.EXTRA && c.EXTRA.includes("auto_increment");
        const fkTable = fkMap[c.COLUMN_NAME];

        const field = {
          name: c.COLUMN_NAME,
          label: c.COLUMN_NAME,
          required: c.IS_NULLABLE === "NO" && !isAutoIncrement,
        };

        if (pkColumns.includes(c.COLUMN_NAME)) {
          field.primary_key = true;
        }

        if (fkTable) {
          field.type = "Key";
          field.reftable_name = fkTable;
        } else {
          const type = findType(c.DATA_TYPE, c.COLUMN_TYPE);
          if (!type) return null;
          field.type = type;
        }

        return field;
      })
      .filter((f) => f !== null);

    packTables.push({
      name: tnm,
      fields,
      min_role_read: 1,
      min_role_write: 1,
    });
  }

  return { tables: packTables };
};

module.exports = { discover_tables, discoverable_tables, findType };
