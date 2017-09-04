var common = require("./common");
var ibmdb = require("ibm_db");
var dbutils = require('./index');
var db2;
var Q = require('q');

ibmdb.open(common.connectionString, function(err,conn){ 
  db2 = conn;
	if (err) {
		console.log(err);
		process.exit(1);
	}
	dropTable();	
	console.log("drop table done!");
	createTable();
	console.log("create table done!");
	test({db:db2,dbname:common.DATABASE});
});

function createTable() {
  db2.querySync("create sequence db2crudtest_seq");
	db2.query('create table db2crudtest (id integer not null,num integer,str varchar(5),datefield timestamp, constraint "P_Identifier_db2crud" primary key (id))', function (err) {
		if (err) {
			console.log(err);
			return finish();
		}
		//return insertData();
		return;
	});
}

function dropTable() {
    try { 
        db2.querySync("drop table db2crudtest");
        db2.querySync("drop sequence db2crudtest_seq");
    }catch(e){
    //    console.log(e);
    // do nothing if the table doesn't exist
    }
}

function test(db){
  dbutils.init(db).then(function(){
    insertData().then(function(){
      getData().then(function(){
        updateData().then(function(){
          delData().then(function(){
            console.log("test finished!");
          })
        });
      });
    })
  });
}

function insertData() {
  var deferred = Q.defer();
  var tbObj = {NUM:1,STR:"12345",DATEFIELD:"2017-09-03 05:25:00"};
  dbutils.insert("db2crudtest",tbObj).then(function(rdata){
    console.log("retdata=" + JSON.stringify(rdata));
    tbObj = {ID:2,STR:"str2"};
    dbutils.insert("db2crudtest",tbObj).then(function(rdata){
      tbObj = [{ID:3,STR:"str3",DATEFIELD:"2017-09-03 05:25:00.518"},{ID:4,STR:"str4"}];
      dbutils.insert("db2crudtest",tbObj).then(function(rdata){
        console.log("retdata=" + JSON.stringify(rdata));
        tbObj = [{NUM:"2",STR:"2",DATEFIELD:"2017-09-03 05:25:00"}
        ,{NUM:3,STR:3,DATEFIELD:"2017-09-03 05:25:00"}
        ,{NUM:3,STR:3,DATEFIELD:"abc"}
        ,{NUM:3,STR:"123456",DATEFIELD:"2017-09-03 05:25:00"}];
        dbutils.insert("db2crudtest",tbObj).then(function(rdata){
        },function(error){
          console.log("error=" + JSON.stringify(error));
          deferred.resolve(null);          
        });
      });
    });
  },function(error){
    console.log("error=" + JSON.stringify(error));
  })
  return deferred.promise;
}
function getData() {
  var deferred = Q.defer();
  var tbObj = {};
  dbutils.get("db2crudtest",tbObj).then(function(rdata){
    console.log("retdata=" + JSON.stringify(rdata));
    tbObj = {ID:2,STR:"str2"};
    dbutils.get("db2crudtest",tbObj).then(function(rdata){
      console.log("retdata=" + JSON.stringify(rdata));
      deferred.resolve(null);
    });
  });
  return deferred.promise;
}
function updateData() {
  var deferred = Q.defer();
  var tbObj = {ID:2,STR:"str3"};
  dbutils.get("db2crudtest",tbObj).then(function(rdata){
    console.log("retdata=" + JSON.stringify(rdata));
    deferred.resolve(null);
  });
  return deferred.promise;
}

function delData() {
  var deferred = Q.defer();
  var tbObj = {ID:2,STR:"str3"};
  dbutils.remove("db2crudtest",tbObj).then(function(rdata){
    console.log("retdata=" + JSON.stringify(rdata));
    deferred.resolve(null);
  });
  return deferred.promise;
}

function finish() {
	db2.close(function () {
		console.log("connection closed");
	});
}
