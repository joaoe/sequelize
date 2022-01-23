'use strict';

const _ = require('lodash');

const Utils = require('../../utils');
const { QueryTypes } = require('../../query-types');
const { Op } = require('../../operators');
const { QueryInterface } = require('../abstract/query-interface');

/**
 * The interface that Sequelize uses to talk with MSSQL database
 */
class MSSqlQueryInterface extends QueryInterface {
  /**
  * A wrapper that fixes MSSQL's inability to cleanly remove columns from existing tables if they have a default constraint.
  *
  * @override
  */
  async removeColumn(tableName, attributeName, options) {
    options = { raw: true, ...options };

    const findConstraintSql = this.queryGenerator.getDefaultConstraintQuery(tableName, attributeName);
    const [results0] = await this.sequelize.query(findConstraintSql, options);
    if (results0.length > 0) {
      // No default constraint found -- we can cleanly remove the column
      const dropConstraintSql = this.queryGenerator.dropConstraintQuery(tableName, results0[0].name);
      await this.sequelize.query(dropConstraintSql, options);
    }

    const findForeignKeySql = this.queryGenerator.getForeignKeyQuery(tableName, attributeName);
    const [results] = await this.sequelize.query(findForeignKeySql, options);
    if (results.length > 0) {
      // No foreign key constraints found, so we can remove the column
      const dropForeignKeySql = this.queryGenerator.dropForeignKeyQuery(tableName, results[0].constraint_name);
      await this.sequelize.query(dropForeignKeySql, options);
    }

    // Check if the current column is a primaryKey
    const primaryKeyConstraintSql = this.queryGenerator.getPrimaryKeyConstraintQuery(tableName, attributeName);
    const [result] = await this.sequelize.query(primaryKeyConstraintSql, options);
    if (result.length > 0) {
      const dropConstraintSql = this.queryGenerator.dropConstraintQuery(tableName, result[0].constraintName);
      await this.sequelize.query(dropConstraintSql, options);
    }

    const removeSql = this.queryGenerator.removeColumnQuery(tableName, attributeName);

    return this.sequelize.query(removeSql, options);
  }

  /**
   * @override
   */
  async bulkInsert(tableName, records, options, attributes) {
    if (options.ignoreDuplicates || options.updateOnDuplicate) {
      const updateFields = options.updateOnDuplicate || [];
      const uniqueKeyColumns = this._columnsOfUniqueKeys(options.model);

      const sql = records.map(record => {
        const where = this._populateWhereForUpsert(uniqueKeyColumns, record, {});
        const updateValues = _.pick(record, updateFields);

        return this.queryGenerator.upsertQuery(tableName, record, updateValues, where, options);
      }).join('');

      options = { ...options, type: QueryTypes.UPSERT, raw: true };

      return await this.sequelize.query(sql, options);
    }

    return super.bulkInsert(tableName, records, options, attributes);
  }

  /**
   * @override
   */
  async upsert(tableName, insertValues, updateValues, where, options) {
    options = { ...options, type: QueryTypes.UPSERT, raw: true, returning: true };
    const uniqueKeyColumns = this._columnsOfUniqueKeys(options.model, options.conflictFields);
    where = this._populateWhereForUpsert(uniqueKeyColumns, insertValues, where);
    const sql = this.queryGenerator.upsertQuery(tableName, insertValues, updateValues, where, options);

    return await this.sequelize.query(sql, options);
  }

  _populateWhereForUpsert(uniqueKeyColumns, record, where) {
    const wheres = [];
    let colsInWhere = [];

    if (!Utils.isWhereEmpty(where)) {
      wheres.push(where);
      colsInWhere = Object.keys(where);
    }

    const attributes = Object.keys(record);
    for (const index of uniqueKeyColumns) {
      if (_.intersection(index, colsInWhere).length !== index.length
          && _.intersection(attributes, index).length === index.length) {
        where = {};
        for (const field of index) {
          where[field] = record[field];
        }

        wheres.push(where);
      }
    }

    return { [Op.or]: wheres };
  }

  _columnsOfUniqueKeys(model, extraColumns) {
    function getFieldName(field) {
      return typeof field === 'string' ? field : field.name || field.field || field;
    }

    return [
      ...(_.isEmpty(extraColumns) ? [] : [extraColumns]),
      ...Object.entries(model.rawAttributes).filter(keyAttr => keyAttr[1].primaryKey).map(keyAttr => [getFieldName(keyAttr[1]) || keyAttr[0]]),
      ...Object.values(model.uniqueKeys).map(item => item.fields.map(getFieldName)),
      ...Object.values(model._indexes).filter(item => item.unique).map(item => item.fields.map(getFieldName)),
    ];
  }
}

exports.MSSqlQueryInterface = MSSqlQueryInterface;
