var backend = require('../index');

class Client {
    constructor(config) {
        if (config === undefined) {
            config = {};
        }
        this.conn = backend.connect(config);
    }

    async prepare(q) {
        return new Promise((resolve, reject) => backend.asyncPrepare(this.conn, q, (err, res) => {
            if (err) return reject(err);
            resolve(res);
        }));
    }

    // TODO: implement execute once ParamParser is changed

    async query(q) {
        return new Promise((resolve, reject) => backend.asyncQuery(this.conn, q, (err, res) => {
            if (err) return reject(err);
            resolve(res);
        }));
    }
}

module.exports = {
    Client
}