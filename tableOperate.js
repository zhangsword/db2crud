//obj that saved table&field meta-data 
var tbDefine = require('./tableDefine').tableSet;
var ibmdb = require("ibm_db")
, cn = 'DATABASE=TEST;HOSTNAME=dpev037.innovate.ibm.com;PORT=50000;PROTOCOL=TCPIP;UID=admin;PWD=dont4get!;';
var dbname = "TEST";
var Q = require('q');
var fs = require('fs');

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
  console.log("sql=" + sql);
  db2.query(sql, function (error, data) {
    if (error) {
      console.log("db error:" + JSON.stringify(error));
      deferred.reject(error);
    } else {
      console.log("db data=" + JSON.stringify(data));
      deferred.resolve(data);
    }
  });
  return deferred.promise;
}
/**
 * execute sql and return result
 */
var getSeq = function(name){
  var deferred = Q.defer();
  exeQuery('select NEXT VALUE FOR ' + name + '_seq value from sysibm.sysdummy1').then(data => deferred.resolve(data[0].VALUE));
  return deferred.promise;
}
var insert = function(name,dataObj){
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
    //console.log("sql=" + sql);
    //console.log("valarr=" + valarr);
    db2.prepare(sql,function (err, stmt) {
      if (err) {
        console.log(err);
        return;
      }
      //Bind and Execute the statment asynchronously
      stmt.execute(valarr, function (err, ret) {
        if( err ) {
          console.log(err);  
          deferred.resolve(err);
        }
        else {
          deferred.resolve(dataObj);
        }
      });
    });
  });
  return deferred.promise;
}

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
  console.log("sql=" + sql);
  console.log("valarr=" + valarr);
  db2.prepare(sql,function (err, stmt) {
    if (err) {
      console.log(err);
      return;
    }
    //Bind and Execute the statment asynchronously
    stmt.execute(valarr, function (err, ret) {
      if( err ) {
        console.log(err);  
        deferred.resolve(err)
      }
      else {
        var data = ret.fetchAllSync();
        deferred.resolve(data);
        console.log("data = " + JSON.stringify(data));
      }
    });
  });
  return deferred.promise;
}

var getById = function (name, id) {
  var tb = getTbDefine(name);
  var dataObj = {};
  dataObj[tb.PK_FIELD] = id;
  return get(name, dataObj);
};

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
  console.log("sql=" + sql);
  console.log("valarr=" + valarr);
  db2.prepare(sql,function (err, stmt) {
    if (err) {
      console.log(err);
      return;
    }
    //Bind and Execute the statment asynchronously
    stmt.executeNonQuery(valarr, function (err, ret) {
      if( err ) {
        console.log(err);  
        deferred.resolve(err);
      }
      else {
        console.log("Affected rows = " + ret);
        deferred.resolve(ret);
      }
    });
  });
  return deferred.promise;
}


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
  console.log("sql=" + sql);
  console.log("valarr=" + valarr);
  db2.prepare(sql,function (err, stmt) {
    if (err) {
      console.log(err);
      deferred.resolve(err);
    }
    //Bind and Execute the statment asynchronously
    stmt.executeNonQuery(valarr, function (err, ret) {
      if( err ) {
        console.log(err);  
        deferred.resolve(err)
      }
      else {
        console.log("Affected rows = " + ret);
        deferred.resolve(ret);
      }
    });
  });
  return deferred.promise;
}

var removeById = function (name, id) {
  var tb = getTbDefine(name);
  var dataObj = {};
  dataObj[tb.PK_FIELD] = id;
  return remove(name, dataObj);
};


var init = function(){
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
          console.log("got table&field metadata!");
          setPK().then(function(){
            //console.log(JSON.stringify(tbDefine));
            console.log("set PK of table!");
            deferred.resolve(null);
          })
        }
      }
    });
  });
  return deferred.promise;
}
var getTbInfo = function(name){
  var deferred = Q.defer();
  db2.describe({
    database : dbname,
    table : name
  }, function (err, data) {
    deferred.resolve(data);
  });
  return deferred.promise;
}

var setPK = function(){
  var deferred = Q.defer();
  fs.readFile('./getPK.sql', 'utf8', function(err, contents) {
    exeQuery(contents).then(function(data){
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