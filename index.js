//obj that saved table&field meta-data 
var tbDefine = [];
var dbname = "TEST";
var Q = require('q');
var fs = require('fs');
var db2;
var log = require('log4node');

/**
 * get definition of table by tablename
 */
var getTbDefine = function(name){
  for (var i = 0; i < tbDefine.length; i++) {
    var item = tbDefine[i];
    if (item.TABLE_NAME==name.toUpperCase()){
      return item;
    }
  }
  return null;
}

/**
 * get field definition of table by tablename&fldname
 */
var getFldDefine = function(tbname,fldname){
  var tb = getTbDefine(tbname);
  for (var i = 0; i < tb.FIELD_DEFINITION.length; i++) {
    var item = tb.FIELD_DEFINITION[i];    
    if (item.COLUMN_NAME==fldname.toUpperCase())
    return item;
  };
  return null;
}

/**
 * execute sql and return result
 */
function exeQuery(sql) {
  var deferred = Q.defer();
  log.debug("sql=" + sql);
  db2.query(sql, function (error, data) {
    if (error) {
      log.error("db error:" + JSON.stringify(error));
      deferred.reject(error);
    } else {
      log.debug("db data=" + JSON.stringify(data));
      deferred.resolve(data);
    }
  });
  return deferred.promise;
}

/**
 * get value of sequence 
 * sequence name rule is : tablename + "_seq"
 */
var getSeq = function(name){
  var deferred = Q.defer();
  exeQuery('select NEXT VALUE FOR ' + name + '_seq value from sysibm.sysdummy1').then(data => deferred.resolve(data[0].VALUE));
  return deferred.promise;
}

var insert = function(name,dataObj){
  var deferred = Q.defer();
  if (dataObj.constructor == Array){
    var promises = [];
    for (var i = 0; i < dataObj.length; i++) {
      var item = dataObj[i];
      promises.push(_insert(name,dataObj));
    }
    Q.all(promises).then(function(result){ 
      deferred.resolve(result);
    });
  } else {
    deferred.resolve(_insert(name,dataObj));
  }
  return deferred.promise;
}

/**
 * mapping fieldname with JSON field and insert into table named "name" parameter 
 */
var _insert = function(name,dataObj){
  var deferred = Q.defer();
  getSeq(name).then(function(seq){
    var tb = getTbDefine(name);
    dataObj[tb.PK_FIELD] = seq;
    var sql = "insert into " + tb.TABLE_NAME + "([fldstr]) values([valstr])";
    var fldstr = "",valstr="",valarr = [];
    for (var i = 0; i < tb.FIELD_DEFINITION.length; i++) {
      var item = tb.FIELD_DEFINITION[i];
      var fldname = item.COLUMN_NAME;
      if (tb.PK_FIELD == fldname.toUpperCase()){
        fldstr = fldstr + fldname + ",";
        valstr = valstr + seq + ",";
      } else {
        if (dataObj[fldname] != null && dataObj[fldname] != undefined){
          fldstr = fldstr + fldname + ",";
          valstr = valstr + "?,"
          valarr.push(dataObj[fldname]);
        }
      }
    };
    fldstr = fldstr.substring(0,fldstr.length-1);
    valstr = valstr.substring(0,valstr.length-1);
    sql = sql.replace("[fldstr]",fldstr).replace("[valstr]",valstr);
    log.debug("sql=" + sql);
    log.debug("valarr=" + valarr);
    db2.prepare(sql,function (err, stmt) {
      if (err) {
        console.log(err);
        return;
      }
      // Bind and Execute the statment asynchronously
      stmt.execute(valarr, function (err, ret) {
        if( err ) {
          log.error(err);  
          deferred.reject(err);
        }
        else {
          deferred.resolve(dataObj);
        }
      });
    });
  });
  return deferred.promise;
}

/**
 * get records that field equal with corresponding JSON field
 */
var get = function(name,dataObj){
  var deferred = Q.defer();
  var tb = getTbDefine(name);
  var sql = "select * from " + tb.TABLE_NAME + " where [fldstr]";
  var fldstr = "",valarr = [];
  for (var i = 0; i < tb.FIELD_DEFINITION.length; i++) {
    var item = tb.FIELD_DEFINITION[i];
    var fldname = item.COLUMN_NAME;
    if (dataObj[fldname] != null && dataObj[fldname] != undefined){
      fldstr = fldstr + fldname + " = ? and ";
      valarr.push(dataObj[fldname]);
    }
  }
  fldstr = fldstr.substring(0,fldstr.length-5);
  sql = sql.replace("[fldstr]",fldstr);
  log.debug("sql=" + sql);
  log.debug("valarr=" + valarr);
  db2.prepare(sql,function (err, stmt) {
    if (err) {
      console.log(err);
      return;
    }
    // Bind and Execute the statment asynchronously
    stmt.execute(valarr, function (err, ret) {
      if( err ) {
        log.error(err);  
        deferred.reject(err)
      }
      else {
        var data = ret.fetchAllSync();
        log.debug("data = " + JSON.stringify(data));
        deferred.resolve(data);
      }
    });
  });
  return deferred.promise;
}

/**
 * get record by PK
 */
var getById = function (name, id) {
  var tb = getTbDefine(name);
  var dataObj = {};
  dataObj[tb.PK_FIELD] = id;
  return get(name, dataObj);
};

/**
 * update records that field equal with corresponding JSON field
 */
var update = function(name,dataObj){
  var deferred = Q.defer();
  var tb = getTbDefine(name);
  var sql = "update " + tb.TABLE_NAME + " set [fldstr]" + " where [pkstr]";
  var fldstr = "",valarr = [],pkstr="";
  for (var i = 0; i < tb.FIELD_DEFINITION.length; i++) {
    var item = tb.FIELD_DEFINITION[i];
    var fldname = item.COLUMN_NAME;
    if (dataObj[fldname] != null && dataObj[fldname] != undefined){
      fldstr = fldstr + fldname + " = ? , ";
      valarr.push(dataObj[fldname]);
    }
  }
  fldstr = fldstr.substring(0,fldstr.length-3);
  sql = sql.replace("[fldstr]",fldstr).replace("[pkstr]",tb.PK_FIELD + "=" + dataObj[tb.PK_FIELD]);
  log.debug("sql=" + sql);
  log.debug("valarr=" + valarr);
  db2.prepare(sql,function (err, stmt) {
    if (err) {
      console.log(err);
      return;
    }
    // Bind and Execute the statment asynchronously
    stmt.executeNonQuery(valarr, function (err, ret) {
      if( err ) {
        log.error(err);  
        deferred.reject(err);
      }
      else {
        log.debug("Affected rows = " + ret);
        deferred.resolve(ret);
      }
    });
  });
  return deferred.promise;
}

/**
 * delete records that field equal with corresponding JSON field
 */
var remove = function(name,dataObj){
  var deferred = Q.defer();
  var tb = getTbDefine(name);
  var sql = "delete from " + tb.TABLE_NAME + " where [fldstr]";
  var fldstr = "",valarr = [];
  for (var i = 0; i < tb.FIELD_DEFINITION.length; i++) {
    var item = tb.FIELD_DEFINITION[i];
    var fldname = item.COLUMN_NAME;
    if (dataObj[fldname] != null && dataObj[fldname] != undefined){
      fldstr = fldstr + fldname + " = ? and ";
      valarr.push(dataObj[fldname]);
    }
  }
  fldstr = fldstr.substring(0,fldstr.length-5);
  sql = sql.replace("[fldstr]",fldstr);
  log.debug("sql=" + sql);
  log.debug("valarr=" + valarr);
  db2.prepare(sql,function (err, stmt) {
    if (err) {
      log.error(err);
      deferred.resolve(err);
    }
    // Bind and Execute the statment asynchronously
    stmt.executeNonQuery(valarr, function (err, ret) {
      if( err ) {
        log.error(err);  
        deferred.reject(err)
      }
      else {
        log.debug("Affected rows = " + ret);
        deferred.resolve(ret);
      }
    });
  });
  return deferred.promise;
}

/**
 * delete record by PK
 */
var removeById = function (name, id) {
  var tb = getTbDefine(name);
  var dataObj = {};
  dataObj[tb.PK_FIELD] = id;
  return remove(name, dataObj);
};

/**
 * get tablelist of current schema and fields definition of these tables
 */

var init = function(db){
  log.setLogLevel('info');
  db2 = db;
  var deferred = Q.defer();
  db2.describe({
    database : dbname
  }, function (err, data) {
    var promises = [];
    tbDefine = data;
    for (var i = 0; i < tbDefine.length; i++) {
      var item = tbDefine[i];
      promises.push(getTbInfo(item.TABLE_NAME));
    };
    Q.all(promises).then(function(result){ 
      for (var i = 0; i < result.length; i++) {
        tbDefine[i].FIELD_DEFINITION = result[i];
        if (i==result.length-1){
          log.info("got table&field metadata!");
          setPK().then(function(){
            // console.log(JSON.stringify(tbDefine));
            log.info("set PK of table!");
            deferred.resolve(null);
          })
        }
      }
    });
  });
  return deferred.promise;
}

/**
 * get tablelist of current schema
 */
var getTbInfo = function(name){
  var deferred = Q.defer();
  db2.describe({
    database : dbname,
    table : name
  }, function (err, data) {
    if (err){
      deferred.reject(err);
    }
    deferred.resolve(data);
  });
  return deferred.promise;
}

/**
 * set pk field for each item of table list
 */
var setPK = function(){
  var deferred = Q.defer();
  var sql =  "SELECT tabschema, tabname, colname FROM syscat.columns WHERE keyseq IS NOT NULL AND keyseq > 0  ORDER BY tabschema, tabname, keyseq"; 
  exeQuery(sql).then(function(data){
      for (var i = 0; i < data.length; i++) {
        for (var j = 0; j < tbDefine.length; j++) {
          if (data[i].TABNAME == tbDefine[j].TABLE_NAME){
            tbDefine[j].PK_FIELD = data[i].COLNAME;
          }
        }
        if (i==data.length-1) {
          deferred.resolve(tbDefine);
        }
      }
    });
  return deferred.promise;
};

module.exports = {
    get: get,
    getById: getById,
    insert: insert,
    remove: remove,
    removeById: removeById,
    update: update,
    getTbDefine: getTbDefine,
    init: init
};