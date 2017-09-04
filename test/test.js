var common = require("./common");
var ibmdb = require("ibm_db");
var Q = require('q');
var assert = require('assert');
var db2crud = require('../index');
var db2;
function createTable(db2) {
  db2.querySync("create sequence db2crudtest_seq");
  db2.query('create table db2crudtest (id integer not null,num integer,str varchar(5),datefield timestamp, constraint "P_Identifier_db2crud" primary key (id))', function (err) {
    if (err) {
      console.log(err);
    }
    //return insertData();
    return;
  });
}

function dropTable(db2) {
    try { 
        db2.querySync("drop table db2crudtest");
    }catch(e){
      console.log(e);
    // do nothing if the table doesn't exist
    }
    try { 
      db2.querySync("drop sequence db2crudtest_seq");
    }catch(e){
      console.log(e);
    // do nothing if the table doesn't exist
    }
}

describe('db2crudtest', function() { 

  this.timeout(60000);
  before(function(done) {
    // runs before all tests in this block
    ibmdb.open(common.connectionString, function(err,conn){ 
      db2 = conn;
      if (err) {
        console.log(err);
        process.exit(1);
      } 
      dropTable(db2);
      createTable(db2);
      db2crud.init({db:db2,dbname:'TEST'}).then(function(rdata){
        return done();        
      });
    });
  });
  it('insert one record', (done) => {
    var tbObj = {NUM:1,STR:"12345",DATEFIELD:"2017-09-03 05:25:00"};
    var shouldObj = {NUM:1,STR:"12345",DATEFIELD:"2017-09-03 05:25:00","ID":1};
    commTest(tbObj,shouldObj,done);
  });
  it('insert multi records', (done) => {
    var tbObj = [{ID:2,STR:"str2",DATEFIELD:"2017-09-03 05:25:00.518"},{ID:3,STR:"str3"}];
    var shouldObj = [{ID:2,STR:"str2",DATEFIELD:"2017-09-03 05:25:00.518"},{ID:3,STR:"str3"}];
    commTest(tbObj,shouldObj,done);
  });
  
  it('#exception test(data type is integer but value type is varchar)', (done) => {
    var tbObj = {NUM:"2",STR:"2",DATEFIELD:"2017-09-03 05:25:00"};
    var shouldObj = [{"field":"NUM","value":"2","errMsg":"NUM[2] is not integer"}];
    exceptTest(tbObj,shouldObj,done);
  });
  it('#exception:data type is varchar but value type is integer', (done) => {
    var tbObj = {NUM:3,STR:3,DATEFIELD:"2017-09-03 05:25:00"};
    var shouldObj = [{"field":"STR","value":3,"errMsg":"STR[3] is not varchar"}];
    exceptTest(tbObj,shouldObj,done);
  });
  it('#exception:timestamp format is not right', (done) => {
    var tbObj = {NUM:3,STR:3,DATEFIELD:"abc"}
    var shouldObj = [{"field":"STR","value":3,"errMsg":"STR[3] is not varchar"},{"field":"DATEFIELD","value":"abc","errMsg":"format of DATEFIELD[abc] is invalid "}];
    exceptTest(tbObj,shouldObj,done);
  });
  it('#exception:varchar length is more than max length', (done) => {
    var tbObj = {NUM:3,STR:"123456",DATEFIELD:"2017-09-03 05:25:00"};
    var shouldObj = [{"field":"STR","value":"123456","errMsg":"length ofSTR[123456] must be less than 5"}];
    exceptTest(tbObj,shouldObj,done);
  });
});

var exceptTest = function(tbObj,shouldObj,done){
  db2crud.insert("db2crudtest",tbObj).then(function(result){},function(result){
    result = JSON.stringify(result);
    var shouldstr = JSON.stringify(shouldObj);
    try{
      assert.equal(result,shouldstr);
      done();
    }catch(e){
      done(e);
    }
  });
}

var commTest = function(tbObj,shouldObj,done){
  db2crud.insert("db2crudtest",tbObj).then(function(result){
    result = JSON.stringify(result);
    var shouldstr = JSON.stringify(shouldObj);
    try{
      assert.equal(result,shouldstr);
      done();
    }catch(e){
      done(e);
    }
  });
}