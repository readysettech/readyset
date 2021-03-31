var backend = require("../index");

class Client {
  constructor(config) {
    if (config == null) {
      config = {};
    }
    this.conn = backend.connect(config);
    this.prep_cache = new Map();
  }

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
