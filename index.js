'use strict';

const debug = require('debug')('CommonDao');
const createCommonDao = require('common-dao');
const TYPE_COLUMN = 1;
const TYPE_PK = 2;
const TYPE_INDEX = 3;
function loadMetadata(client, table){
  var pos = table.indexOf('.');
  var schema = null;
  if ( pos >= 0 ) {
    schema = table.substring(0, pos);
    table = table.substring(pos+1);
  }
  if ( !schema ) schema = client.keyspace;
  return function loadTableMetadata(callback){
    if ( !schema ) return callback(new Error('Empty keyspace'));
    client.connect((err)=>{
      if ( err ) return callback(err);
      client.metadata.getTable(schema, table, function(err, result){
        if ( err ) return callback(err);
        var columns = {};
        var columnsString = '';
        result.columns.forEach((col)=>{columns[col.name] = TYPE_COLUMN;})
        columnsString = result.columns.map((col)=>{return col.name}).join(',');
        result.partitionKeys.forEach((col)=>{
          if ( columns[col.name] === TYPE_COLUMN ) {
            columns[col.name] = TYPE_PK;
          }
        });
        result.indexes.forEach((col)=>{
          if ( columns[col.target] === TYPE_COLUMN ) {
            columns[col.target] = TYPE_INDEX;
          }
        });
        callback(null,{
          columns: columns,
          columnsString: columnsString
        })
      });
    });
  }
}


class CassandraDaoAdapter {
  constructor(table, getConnection, opts){
    this.table = table;
    this.getConnection = getConnection;
  }

  *ensureLoad(){
    if ( !this.tableInfo ) {
      this.tableInfo = yield loadMetadata(this.getConnection(), this.table);
    }
  }

  *refresh(){
    delete this.tableInfo;
  }

  isPrimaryKey(col){
    return this.tableInfo.columns[col] === TYPE_PK;
  }

  isIndex(col) {
    return this.tableInfo.columns[col] === TYPE_INDEX;
  }

  exists(col){
    return this.tableInfo.columns[col] != null;
  }

  selectColumns(columns){
		return columns ? columns.join(',') : this.tableInfo.columnsString;
	}

  columnsPair(columns){
		return columns.map((col)=>{
      return (col +'=:'+col);
    }).join(',');
	}

  wherePair(field, value){
    if ( value instanceof Array ){
      return field+' in :'+field;
    }
    else {
      return field+'=:'+field;
    }
  }

  execute(cql, params, options) {
    var client = this.getConnection();
    return function cassandraExecute(callback){
      client.execute(cql, params, options, function(err, result){
        if ( err ) {
          debug('CassandraDaoAdapter.execute', err);
          callback(err);
          return;
        }
        callback(err, result.rows, result);
      });
    }
	}
}

module.exports = function createDao(table, getConnection, options){
  return createCommonDao(table, {
    adapter: new CassandraDaoAdapter(table, getConnection, options)
  });
}
