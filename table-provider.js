const db = require("@saltcorn/data/db");
const Workflow = require("@saltcorn/data/models/workflow");
const Form = require("@saltcorn/data/models/form");
const FieldRepeat = require("@saltcorn/data/models/fieldrepeat");
const Field = require("@saltcorn/data/models/field");
const Table = require("@saltcorn/data/models/table");
const {
  aggregation_query_fields,
  joinfield_renamer,
} = require("@saltcorn/data/models/internal/query");
const { getState } = require("@saltcorn/data/db/state");
const {
  sqlsanitize,
  mkWhere,
  mkSelectOptions,
} = require("@saltcorn/db-common/internal");

const { getConnection } = require("./connections");
const { discover_tables } = require("./discovery");

const pgToMysql = (sql, values) => {
  if (!sql) return { sql: "", values: values || [] };

  const newValues = [];
  let converted = sql;

  if (/\$\d+/.test(sql) && values && values.length > 0) {
    converted = converted.replace(/\$(\d+)/g, (_, num) => {
      newValues.push(values[parseInt(num) - 1]);
      return "?";
    });
  } else if (values) {
    newValues.push(...values);
  }

  converted = converted
    .replace(/"/g, "`")
    .replace(/\bILIKE\b/gi, "LIKE")
    .replace(/::([\w]+(\[\])?)/g, "")
    .replace(/\bNULLS\s+(FIRST|LAST)\b/gi, "")
    .replace(/\bLIMIT\s+ALL\b/gi, "");

  return { sql: converted, values: newValues };
};

const configuration_workflow = (req) =>
  new Workflow({
    onDone: (ctx) => {
      (ctx.fields || []).forEach((f) => {
        if (f.summary_field) {
          if (!f.attributes) f.attributes = {};
          f.attributes.summary_field = f.summary_field;
        }
      });
      return ctx;
    },
    steps: [
      {
        name: "table",
        form: async () => {
          return new Form({
            fields: [
              {
                name: "host",
                label: "Database host",
                type: "String",
                required: true,
                exclude_from_mobile: true,
              },
              {
                name: "port",
                label: "Port",
                type: "Integer",
                required: true,
                default: 3306,
                exclude_from_mobile: true,
              },
              {
                name: "user",
                label: "User",
                type: "String",
                required: true,
                exclude_from_mobile: true,
              },
              {
                name: "password",
                label: "Password",
                type: "String",
                fieldview: "password",
                required: true,
                sublabel:
                  "If blank, use environment variable <code>SC_EXTMYSQL_PASS_{database name}</code>",
                exclude_from_mobile: true,
              },
              {
                name: "database",
                label: "Database",
                type: "String",
                required: true,
                exclude_from_mobile: true,
              },
              {
                name: "table_name",
                label: "Table name",
                type: "String",
                required: true,
                exclude_from_mobile: true,
              },
              {
                name: "read_only",
                label: "Read-only",
                type: "Bool",
              },
            ],
          });
        },
      },
      {
        name: "fields",
        form: async (ctx) => {
          const pool = await getConnection(ctx);
          const pack = await discover_tables(
            [ctx.table_name],
            ctx.database,
            pool,
          );
          const tables = await Table.find({});

          const real_fkey_opts = tables.map((t) => `Key to ${t.name}`);
          const fkey_opts = ["File", ...real_fkey_opts];

          const form = new Form({
            fields: [
              {
                input_type: "section_header",
                label: "Column types",
              },
              new FieldRepeat({
                name: "fields",
                fields: [
                  {
                    name: "name",
                    label: "Name",
                    type: "String",
                    required: true,
                  },
                  {
                    name: "label",
                    label: "Label",
                    type: "String",
                    required: true,
                  },
                  {
                    name: "type",
                    label: "Type",
                    type: "String",
                    required: true,
                    attributes: {
                      options: getState().type_names.concat(fkey_opts || []),
                    },
                  },
                  {
                    name: "primary_key",
                    label: "Primary key",
                    type: "Bool",
                  },
                  {
                    name: "summary_field",
                    label: "Summary field",
                    sublabel:
                      "The field name, on the target table, which will be used to pick values for this key",
                    type: "String",
                    showIf: { type: real_fkey_opts },
                  },
                ],
              }),
            ],
          });
          if (!ctx.fields || !ctx.fields.length) {
            if (!form.values) form.values = {};
            form.values.fields = pack.tables[0].fields;
          } else {
            (ctx.fields || []).forEach((f) => {
              if (f.type === "Key" && f.reftable_name)
                f.type = `Key to ${f.reftable_name}`;
              if (f.attributes?.summary_field)
                f.summary_field = f.attributes?.summary_field;
              const reftable_name =
                f.reftable_name || typeof f.type === "string"
                  ? f.type.replace("Key to ", "")
                  : null;
              const reftable = reftable_name && Table.findOne(reftable_name);
              const repeater = form.fields.find((ff) => ff.isRepeat);
              const sum_form_field = repeater.fields.find(
                (ff) => ff.name === "summary_field",
              );
              if (reftable && sum_form_field) {
                sum_form_field.showIf.type = sum_form_field.showIf.type.filter(
                  (t) => t !== f.type,
                );

                repeater.fields.push(
                  new Field({
                    name: "summary_field",
                    label: "Summary field for " + f.name,
                    sublabel: `The field name, on the ${reftable_name} table, which will be used to pick values for this key`,
                    type: "String",
                    showIf: { type: f.type },
                    attributes: {
                      options: reftable.fields.map((f) => f.name),
                    },
                  }),
                );
              }
            });
          }

          return form;
        },
      },
    ],
  });

const getPkName = (cfg) => {
  const pkField = (cfg.fields || []).find((f) => f.primary_key);
  return pkField ? pkField.name : "id";
};

module.exports = {
  "MySQL remote table": {
    configuration_workflow,
    fields: (cfg) => {
      return cfg?.fields || [];
    },
    get_table: (cfg) => {
      const pkName = getPkName(cfg);
      return {
        disableFiltering: true,
        ...(cfg?.read_only
          ? {}
          : {
              deleteRows: async (where, user) => {
                const pool = await getConnection(cfg);
                const { where: whereClause, values: whereVals } = mkWhere(
                  where || {},
                );
                const { sql, values } = pgToMysql(
                  `DELETE FROM "${sqlsanitize(cfg.table_name)}" ${whereClause}`,
                  whereVals,
                );
                await pool.query(sql, values);
              },
              updateRow: async (updRow, id, user) => {
                const pool = await getConnection(cfg);
                const kvs = Object.entries(updRow);
                const assigns = kvs
                  .map(([k]) => `\`${sqlsanitize(k)}\` = ?`)
                  .join(", ");
                const values = [...kvs.map(([, v]) => v), id];
                const sql = `UPDATE \`${sqlsanitize(cfg.table_name)}\` SET ${assigns} WHERE \`${sqlsanitize(pkName)}\` = ?`;
                await pool.query(sql, values);
              },
              insertRow: async (rec, user) => {
                const pool = await getConnection(cfg);
                const kvs = Object.entries(rec);
                const fnameList = kvs
                  .map(([k]) => `\`${sqlsanitize(k)}\``)
                  .join(", ");
                const valPlaceholders = kvs.map(() => "?").join(", ");
                const values = kvs.map(([, v]) => v);
                const sql = `INSERT INTO \`${sqlsanitize(cfg.table_name)}\` (${fnameList}) VALUES (${valPlaceholders})`;
                const [result] = await pool.query(sql, values);
                return result.insertId;
              },
            }),
        countRows: async (where, opts) => {
          const pool = await getConnection(cfg);
          const { where: whereClause, values: whereVals } = mkWhere(
            where || {},
          );
          const { sql, values } = pgToMysql(
            `SELECT COUNT(*) as count FROM "${sqlsanitize(cfg.table_name)}" ${whereClause}`,
            whereVals,
          );
          const [rows] = await pool.query(sql, values);
          return parseInt(rows[0].count);
        },
        aggregationQuery: async (aggregations, options) => {
          const pool = await getConnection(cfg);
          const {
            sql: pgSql,
            values: pgValues,
            groupBy,
          } = aggregation_query_fields(cfg.table_name, aggregations, {
            ...options,
            schema: cfg.database,
          });
          const { sql, values } = pgToMysql(pgSql, pgValues);

          const [rows] = await pool.query(sql, values);
          if (groupBy) return rows;
          return rows[0];
        },
        distinctValues: async (fieldnm, whereObj) => {
          const pool = await getConnection(cfg);
          if (whereObj) {
            const { where, values: whereVals } = mkWhere(whereObj);
            const { sql, values } = pgToMysql(
              `SELECT DISTINCT "${sqlsanitize(fieldnm)}" FROM "${sqlsanitize(cfg.table_name)}" ${where} ORDER BY "${sqlsanitize(fieldnm)}"`,
              whereVals,
            );
            const [rows] = await pool.query(sql, values);
            return rows.map((r) => r[fieldnm]);
          } else {
            const sql = `SELECT DISTINCT \`${sqlsanitize(fieldnm)}\` FROM \`${sqlsanitize(cfg.table_name)}\` ORDER BY \`${sqlsanitize(fieldnm)}\``;
            const [rows] = await pool.query(sql);
            return rows.map((r) => r[fieldnm]);
          }
        },
        getRows: async (where, opts) => {
          const pool = await getConnection(cfg);
          const { where: whereClause, values: whereVals } = mkWhere(
            where || {},
          );
          const selectOpts = mkSelectOptions(opts || {});
          const { sql, values } = pgToMysql(
            `SELECT * FROM "${sqlsanitize(cfg.table_name)}" ${whereClause} ${selectOpts}`,
            whereVals,
          );
          const [rows] = await pool.query(sql, values);
          return rows;
        },
        getJoinedRows: async (opts) => {
          const pool = await getConnection(cfg);
          const pseudoTable = new Table({
            name: cfg.table_name,
            fields: cfg.fields,
          });
          const {
            sql: pgSql,
            values: pgValues,
            joinFields,
            aggregations,
          } = await pseudoTable.getJoinedQuery({
            schema: cfg.database,
            ...opts,
            ignoreExternal: true,
          });
          const { sql, values } = pgToMysql(pgSql, pgValues);
          if (db.get_sql_logging?.()) console.log(sql, values);
          const [rows] = await pool.query(sql, values);
          let result = joinfield_renamer
            ? joinfield_renamer(joinFields, aggregations)(rows)
            : rows;
          for (const k of Object.keys(joinFields || {})) {
            if (!joinFields?.[k].lookupFunction) continue;
            for (const row of result) {
              row[k] = await joinFields[k].lookupFunction(row);
            }
          }
          return result;
        },
      };
    },
  },
};
