/*!
 * db2crud
 * Copyright(c) 2017 Jian Zhang
 * sword_zhang@163.com
 * MIT Licensed
 */

var tbDefine = [];
var dbname = "TEST";
var Q = require('q');
var fs = require('fs');
var moment = require('moment');
var db2;
var log = require('log4node');
var TIMESTAMP_FORMAT = 'YYYY-MM-DD HH:mm:ss';

/**
 * set database with param "name"
 *
 * @param String name:database name :sample 'TEST'
 * @return 
 **/
var setDbname = function(name){
  dbname = name;
}

/**
 * set time format for timestamp field
 *
 * @param String tf :time format of db2:like 'YYYY-MM-DD HH:mm:ss'
 * @return 
 * @reference refer following url about date format 
 * https://momentjs.com/docs/#/use-it/    
 **/
var setTimeFormat = function(tf){
  TIMESTAMP_FORMAT = tf;
}
var intType = 4;
var varcharType = 12;
var timestampType = 93;

/**
 * check field value on type,length,format
 *
 * @param String name :table name of db2 need operate on
 * @param JSON tbObjArr :like following obj
    [  
      {  
        NUM:"2",
        STR:"2",
        DATEFIELD:"2017-09-03 05:25:00"
      },
      {  
        NUM:3,
        STR:3,
        DATEFIELD:"2017-09-03 05:25:00"
      }
    ]
 * @return promise object included checkResult:
 * 【success result sample】: []
 * 【error result sample】
    [  
      [  
        {  
          "field":"STR",
          "value":3,
          "errMsg":"STR[3] is not varchar"
        },
        {  
          "field":"DATEFIELD",
          "value":"abc",
          "errMsg":"format of DATEFIELD[abc] is invalid "
        }
      ],
      [  
        {  
          "field":"STR",
          "value":"123456",
          "errMsg":"length ofSTR[123456] must be less than 5"
        }
      ]
    ]
 * @reference refer following url about date format 
 * https://momentjs.com/docs/#/use-it/    
 **/
var checkArrValid = function(name,tbObjArr) {
  var deferred = Q.defer();
  var promises = [];
  for (var i = 0; i < tbObjArr.length; i++) {
    var item = tbObjArr[i];
    promises.push(checkValid(name,item)); 
  }
  Q.all(promises).then(function(result){ 
    var errFlag = false;
    for (var i = 0; i < result.length; i++) {
      if (result[i].length > 0){
        errFlag = true;
      }
    }
    if (errFlag) {
      deferred.resolve(result);
    } else {
      deferred.resolve([]);
    }
  });
  return deferred.promise;
}

/**
 * check field value on type,length,format
 *
 * @param String name :table name of db2 need operate on
 * @param JSON tbObj :like following obj 
    {  
      NUM:"2",
      STR:"2",
      DATEFIELD:"2017-09-03 05:25:00"
    }
 * @return promise object included checkResult:
 * 【success result sample】: []
 * 【error result sample】  
    [  
      {  
        "field":"DATEFIELD",
        "value":"abc",
        "errMsg":"format of DATEFIELD[abc] is invalid "
      },
      {  
        "field":"STR",
        "value":"123456",
        "errMsg":"length of STR[123456] must be less than 5"
      }
    ]
 * @reference refer following url about date format 
 * https://momentjs.com/docs/#/use-it/   
 *  
 **/
var checkValid = function(name,tbObj){
  var deferred = Q.defer();
  log.debug("check field start************************************");
  var resultArr = [];
  var tb = getTbDefine(name);
  for (var i = 0; i < tb.FIELD_DEFINITION.length; i++) {
    var item = tb.FIELD_DEFINITION[i];
    var data = tbObj[item.COLUMN_NAME];
    log.debug("Checking field:" + item.COLUMN_NAME + " value=" + data + "________");
    if ((typeof data == 'string') || (data instanceof String)){
      log.debug("data='" + data + "'");      
    }else{
      log.debug("data=" + data);
    }
    if (!item.NULLABLE && item.COLUMN_NAME!=tb.PK_FIELD && (data==null || data==undefined)){
      resultArr.push(item.COLUMN_NAME + " must be not null");
    } else {
      if (item.COLUMN_NAME!=tb.PK_FIELD){
        if (item.NULLABLE && (data==null || data==undefined)){
          continue;
        } else {
          switch(item.DATA_TYPE) {
          case 4:
            log.debug("data != parseInt(data, 10):" + (data != parseInt(data, 10)));
            if(data != parseInt(data, 10) || (typeof data == 'string') || (data instanceof String)){
              log.debug("data is invalid");
              resultArr.push({field:item.COLUMN_NAME,value:data,errMsg:item.COLUMN_NAME + "[" + data + "]" + " is not integer"});
            };
            break;
          case 12:
            log.debug("typeof data != 'string' && !data instanceof String:" + (typeof data != 'string' && !data instanceof String));
            if ((typeof data != 'string') && !(data instanceof String)){
              log.debug("data type invalid");
              resultArr.push({field:item.COLUMN_NAME,value:data,errMsg:item.COLUMN_NAME + "[" + data + "]" + " is not varchar"});
            } else {
              if (data.length > item.COLUMN_SIZE) {
                log.debug("data length invalid");
                resultArr.push({field:item.COLUMN_NAME,value:data,errMsg:"length of" + item.COLUMN_NAME + "[" + data + "] must be less than " + item.COLUMN_SIZE});
              }
            }
            break;
          case 93:
            log.debug("!moment(data, TIMESTAMP_FORMAT):" + (!moment(data, TIMESTAMP_FORMAT).isValid()));
            if (!moment(data, TIMESTAMP_FORMAT).isValid()){
              log.debug("date format invalid");
              resultArr.push({field:item.COLUMN_NAME,value:data,errMsg:"format of " + item.COLUMN_NAME + "[" + data + "]" + " is invalid "});
            };
            break;
          }
        }
      }
    }
  }
  deferred.resolve(resultArr);
  return deferred.promise;
}

/**
 * get definition of table by tablename
 *
 * @param String name:name of table
 * @return JSON object just like as display following
   {  
    "TABLE_CAT":null,
    "TABLE_SCHEM":"ADMIN",
    "TABLE_NAME":"DB2CRUDTEST",
    "TABLE_TYPE":"TABLE",
    "REMARKS":null,
    "PK_FIELD":"ID",
    "FIELD_DEFINITION":[  
      {  
        "TABLE_CAT":null,
        "TABLE_SCHEM":"ADMIN",
        "TABLE_NAME":"DB2CRUDTEST",
        "COLUMN_NAME":"ID",
        "DATA_TYPE":4,
        "TYPE_NAME":"INTEGER",
        "COLUMN_SIZE":10,
        "BUFFER_LENGTH":4,
        "DECIMAL_DIGITS":0,
        "NUM_PREC_RADIX":10,
        "NULLABLE":0,
        "REMARKS":null,
        "COLUMN_DEF":null,
        "SQL_DATA_TYPE":4,
        "SQL_DATETIME_SUB":null,
        "CHAR_OCTET_LENGTH":null,
        "ORDINAL_POSITION":1,
        "IS_NULLABLE":"NO"
      }
    }
 *
 **/
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
 * get field definition of field by tablename&fldname
 *
 * @param String tbname:name of table
 * @param String fldname:field name
 * @return JSON object just like as display following
      {  
        "TABLE_CAT":null,
        "TABLE_SCHEM":"ADMIN",
        "TABLE_NAME":"DB2CRUDTEST",
        "COLUMN_NAME":"ID",
        "DATA_TYPE":4,
        "TYPE_NAME":"INTEGER",
        "COLUMN_SIZE":10,
        "BUFFER_LENGTH":4,
        "DECIMAL_DIGITS":0,
        "NUM_PREC_RADIX":10,
        "NULLABLE":0,
        "REMARKS":null,
        "COLUMN_DEF":null,
        "SQL_DATA_TYPE":4,
        "SQL_DATETIME_SUB":null,
        "CHAR_OCTET_LENGTH":null,
        "ORDINAL_POSITION":1,
        "IS_NULLABLE":"NO"
      }
 *
 **/
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
 *
 * @param String sql:sql need be executed
 * @return promise object included all of records meet condition of sql
    [  
      {  
        "ID":3,
        "STR":"str3",
        "DATEFIELD":"2017-09-03 05:25:00.518"
      },
      {  
        "ID":4,
        "STR":"str4"
      }
    ]
 *   
 **/
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
 * get next value of sequence
 *
 * @param String name:name of sequence want get
 * @return promise object included next value of sequence, type of result is integer.
 * 
 **/
var getSeq = function(name){
  var deferred = Q.defer();
  exeQuery('select NEXT VALUE FOR ' + name + '_seq value from sysibm.sysdummy1').then(data => deferred.resolve(data[0].VALUE));
  return deferred.promise;
}

/**
 * insert one record or multiple records into some table
 *
 * @param String name:name of table
 * @param String dataObj:JSON object included record need to be inserted
 * attention: PK field need not be set, db2crud will automatically get next value of sequence as PK field 
 * sequence name rule: [tablename] + "_SEQ" 
    {  
      "STR":"str3",
      "DATEFIELD":"2017-09-03 05:25:00.518"
    }
    OR
   [  
      {  
        "STR":"str3",
        "DATEFIELD":"2017-09-03 05:25:00.518"
      },
      {  
        "STR":"str4"
      }
    ]
 * @return promise object included all of field values inserted into table.
   {  
      "ID" :1,
      "STR":"str3",
      "DATEFIELD":"2017-09-03 05:25:00.518"
    }
    OR
   [  
      {  
        "ID" :1,      
        "STR":"str3",
        "DATEFIELD":"2017-09-03 05:25:00.518"
      },
      {  
        "ID" :2,       
        "STR":"str4"
      }
    ]
 * 
 **/
var insert = function(name,dataObj){
  var deferred = Q.defer();
  var rArr = [];
  if (dataObj.constructor == Array){
    checkArrValid(name,dataObj).then(function(result){
      if (result.length > 0) {
        deferred.reject(result);
      } else {
        var promises = [];
        for (var i = 0; i < dataObj.length; i++) {
          var item = dataObj[i];
          promises.push(_insert(name,item));
        }
        Q.all(promises).then(function(result){ 
          deferred.resolve(result);
        });        
      }
    })
  } else {
    checkValid(name,dataObj).then(function(result){
      if (result.length > 0) {
        deferred.reject(result);
      } else {
        deferred.resolve(_insert(name,dataObj));
      }
    })
  }
  return deferred.promise;
}

/**
 * mapping fieldname with JSON field and insert into table named "name" parameter
 *
 * @param String name:name of table
 * @param String dataObj:JSON object included record need to be inserted
 * attention: PK field need not be set, db2crud will automatically get next value of sequence as PK field 
 * sequence name rule: [tablename] + "_SEQ" 
    {  
      "STR":"str3",
      "DATEFIELD":"2017-09-03 05:25:00.518"
    }
 * @return promise object included all of field values inserted into table.
   {
      "ID" : 1,
      "STR":"str3",
      "DATEFIELD":"2017-09-03 05:25:00.518"
    }
 * 
 **/
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
 *
 * @param String name:name of table
 * @param String dataObj:JSON object included search criterion
    {  
      "STR":"str3",
      "DATEFIELD":"2017-09-03 05:25:00.518"
    }
 * @return promise object included all of field values met criterion.
   {
      "ID" : 1,
      "STR":"str3",
      "DATEFIELD":"2017-09-03 05:25:00.518"
    }
    OR
    [  
      {  
        "ID" :1,      
        "STR":"str3",
        "DATEFIELD":"2017-09-03 05:25:00.518"
      },
      {  
        "ID" :2,       
        "STR":"str4",
        "DATEFIELD": null
      }
    ]
 * 
 **/
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
 * get records with PK id
 *
 * @param String name:name of table
 * @param Integer id: value of pk field
 * @return promise object included all of field values met criterion.
   {
      "ID" : 1,
      "STR":"str3",
      "DATEFIELD":"2017-09-03 05:25:00.518"
    }
 * 
 **/
var getById = function (name, id) {
  var tb = getTbDefine(name);
  var dataObj = {};
  dataObj[tb.PK_FIELD] = id;
  return get(name, dataObj);
};

/**
 * update records that field equal with corresponding JSON field
 *
 * @param String name:name of table
 * @param String dataObj:JSON object included update criterion
 * @return promise object included Affected rows.
 * 
 **/
var update = function(name,dataObj){
  var deferred = Q.defer();
  var resultArr = checkValid(name,dataObj);
  if (resultArr.length > 0) {
    log.debug("data are invalid!__________________");
    deferred.reject(resultArr);
  }  
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
 *
 * @param String name:name of table
 * @param String dataObj:JSON object included update criterion
 * @return promise object included Affected rows.
 * 
 **/
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
 *
 * @param String name:name of table
 * @param Integer id:value of pk field
 * @return promise object included Affected rows.
 * 
 **/
var removeById = function (name, id) {
  var tb = getTbDefine(name);
  var dataObj = {};
  dataObj[tb.PK_FIELD] = id;
  return remove(name, dataObj);
};

/**
 * get tablelist of current schema and fields definition of these tables
 *
 * @param Connection option.db:connection that ibmdb.open returned 
     ibmdb.open(common.connectionString, function(err,conn){});
 * @param String option.dbname:databse name     
 * @return promise included null value
 * 
 **/
var init = function(options){
  log.setLogLevel('info');
  db2 = options.db;
  if (options.dbname !=null && options.dbname !=undefined){
    dbname = options.dbname;
  }
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
 *
 * @param String name:name of current schema    
 * @return promise object included table list
 * 
 **/
var getTbInfo = function(name){
  var deferred = Q.defer();
  db2.describe({
    database : dbname,
    table : name
  }, function (err, data) {
    if (err){
      deferred.reject(err);
    };
    deferred.resolve(data);
  });
  return deferred.promise;
}

/**
 * set pk field for each item of table list
 *  
 * @return promise object included pk field name list
 * 
 **/
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
    init: init,
    setTimeFormat:setTimeFormat,
    setDbname:setDbname
};