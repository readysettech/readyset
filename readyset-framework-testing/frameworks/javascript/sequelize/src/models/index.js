'use strict';

const Sequelize = require('sequelize');
let config = require(__dirname + '/../config/config.js');
const db = {};

let sequelize;
if (config.use_env_variable) {
  sequelize = new Sequelize(process.env[config.use_env_variable], config);
} else {
  const env = process.env.NODE_ENV || 'development';
  config = config[env];
  sequelize = new Sequelize(config.database, config.username, config.password, config);
}

const models = [
  require("./commitment.js")
]

models.forEach(m => {
  const model = m(sequelize, Sequelize.DataTypes);
  console.log(model)
  db[model.name] = model;
});

Object.keys(db).forEach(modelName => {
  if (db[modelName].associate) {
    db[modelName].associate(db);
  }
});

db.sequelize = sequelize;
db.Sequelize = Sequelize;

module.exports = db;
