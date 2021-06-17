'use strict';
const {
  Model
} = require('sequelize');
module.exports = (sequelize, DataTypes) => {
  class Commitment extends Model {
    /**
     * Helper method for defining associations.
     * This method is not a part of Sequelize lifecycle.
     * The `models/index` file will call this method automatically.
     */
    static associate(models) {
      // define association here
    }
  };
  Commitment.init({
    user: DataTypes.STRING,
    issue: DataTypes.STRING,
    committedOn: DataTypes.DATE,
    finishedOn: DataTypes.DATE
  }, {
    sequelize,
    modelName: 'Commitment',
  });
  return Commitment;
};
