var backend = require("../index");

/**
 * The Node.js ReadySet client library.
 */
class Client {
  /**
   * Create a Client.
   * If called with no argument, all default configuration options are used.
   *
   * @example
   * const client = new rs.Client();
   *
   * @example
   * config = {
   *   deployment: "customapp",
   *   zookeeperAddress: "127.0.0.1:3000",
   *   sanitize: false,
   * };
   * const client = new rs.Client(config);
   *
   * @param {{
   *  deployment: string,
   *  zookeeperAddress: string,
   *  mySQLAddress: string,
   *  sanitize: boolean,
   *  staticResponses: boolean,
   *  slowLog: boolean,
   *  permissive: boolean,
   *  readYourWrite: boolean
   * }} config Connection configuration options.
   * Any option can be left out of the configuration object to use the
   * default for that specific option.
   * @prop {string} config.deployment defaults to "myapp"
   * @prop {string} config.zookeeperAddress defaults to "127.0.0.1:2181"
   * @prop {string} config.mySQLAddress defaults to ""
   * @prop {boolean} config.sanitize defaults to true
   * @prop {boolean} config.staticResponses defaults to true
   * @prop {boolean} config.slowLog defaults to false
   * @prop {boolean} config.permissive defaults to false
   * @prop {boolean} config.readYourWrite defaults to false
   */
  constructor(config) {
    if (config == null) {
      config = {};
    }
    this.conn = backend.connect(config);
    this.prep_cache = new Map();
  }

  /**
   * Prepares a parameterized query if necessary and then executes
   * the query with the specified parameters.
   * Prepared queries are cached.
   *
   * @example
   * const client = new rs.Client();
   * const r = await client.execute("SELECT * FROM mytable WHERE id = ?", [10001]);
   * console.log(r.data);
   *
   * @example
   * const client = new rs.Client();
   * const r = await client.execute("UPDATE mytable SET first_name = ? where id = ?", ["Clifford", 10001]);
   * console.log(r.numRowsUpdated);
   *
   * @param {string} q The SQL query to be queried. Parameters are denoted with a question mark.
   * @param {(?string|?number|?boolean|?Date)[]} params The values to be inserted as parameters in
   * the parameterized query during execution.
   * @returns {Promise} Promise object resolves to the result of the query.
   */
  async execute(q, params) {
    if (!this.prep_cache.has(q)) {
      const prep_res = await new Promise((resolve, reject) => {
        backend.asyncPrepare(this.conn, q, (err, res) => {
          if (err) return reject(err);
          resolve(res);
        });
      });
      this.prep_cache.set(q, prep_res.statementId);
    }
    const statementId = this.prep_cache.get(q);
    return new Promise((resolve, reject) => {
      backend.asyncExecute(this.conn, statementId, params, (err, res) => {
        if (err) return reject(err);
        resolve(res);
      });
    });
  }

  /**
   * Executes a query in an ad-hoc manner (without preparing).
   *
   * @example
   * const client = new rs.Client();
   * const r = await client.query("SELECT * FROM mytable WHERE id = 10001");
   * console.log(r.data);
   *
   * @example
   * const client = new rs.Client();
   * const r = await client.query("UPDATE mytable SET first_name = "Clifford" where id = 10001");
   * console.log(r.numRowsUpdated);
   *
   * @param {string} q The SQL query to be queried.
   * @returns {Promise} Promise object resolves to the result of the query.
   */
  async query(q) {
    return new Promise((resolve, reject) =>
      backend.asyncQuery(this.conn, q, (err, res) => {
        if (err) return reject(err);
        resolve(res);
      })
    );
  }
}

module.exports = {
  Client,
};
