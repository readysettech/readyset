/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

const {unstable_getCacheForType} = require('react');
const {createPool} = require('mysql2');
const {promisify} = require('util');

const Pending = 0;
const Resolved = 1;
const Rejected = 2;

function createRecordFromThenable(thenable) {
  const record = {
    status: Pending,
    value: thenable,
  };
  thenable.then(
    (value) => {
      if (record.status === Pending) {
        const resolvedRecord = record;
        resolvedRecord.status = Resolved;
        resolvedRecord.value = value;
      }
    },
    (err) => {
      if (record.status === Pending) {
        const rejectedRecord = record;
        rejectedRecord.status = Rejected;
        rejectedRecord.value = err;
      }
    }
  );
  return record;
}

function readRecordValue(record) {
  if (record.status === Resolved) {
    return record.value;
  } else {
    throw record.value;
  }
}

function Pool(options) {
  this.pool = createPool(options);

  // Unique function per instance because it's used for cache identity.
  this.createRecordMap = function() {
    return new Map();
  };
}

Pool.prototype.query = function(query, values) {
  const pool = this.pool;
  const outerMap = unstable_getCacheForType(this.createRecordMap);

  let innerMap = outerMap;
  let key = query;
  if (values != null) {
    // If we have parameters, each becomes as a nesting layer for Maps.
    // We want to find (or create as needed) the innermost Map, and return that.
    for (let i = 0; i < values.length; i++) {
      let nextMap = innerMap.get(key);
      if (nextMap === undefined) {
        nextMap = new Map();
        innerMap.set(key, nextMap);
      } else if (!(nextMap instanceof Map)) {
        throw new Error(
          'This query has received more parameters than the last time ' +
            'the same query was used. Always pass the exact number of ' +
            'parameters that the query needs.'
        );
      }
      innerMap = nextMap;
      key = values[i];
    }
  }

  let record = innerMap.get(key);
  if (!record) {
    const thenable = promisify(pool.execute).call(pool, query, values);
    record = createRecordFromThenable(thenable);
    innerMap.set(key, record);
  } else if (record instanceof Map) {
    invariant(
      false,
      'This query has received fewer parameters than the last time ' +
        'the same query was used. Always pass the exact number of ' +
        'parameters that the query needs.'
    );
  }
  const result = readRecordValue(record);
  return result;
};

module.exports.Pool = Pool;
