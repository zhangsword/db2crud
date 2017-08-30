var common = require("./common");
var ibmdb = require("ibm_db");
var dbutils = require('./tableOperate');
var Q = require('q');

ibmdb.open(common.connectionString, function(err,conn){ 
  db2 = conn;
	if (err) {
		console.log(err);
		process.exit(1);
	}
	
	dropTable();	
	createTable();
	test();
});

function createTable() {
  db2.querySync("create sequence db2crudtest_seq");
	db2.query('create table db2crudtest (id integer not null,str varchar(50), constraint "P_Identifier_db2crud" primary key (id))', function (err) {
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

function test(){
  dbutils.init().then(function(){
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
  var tbObj = {STR:"str1"};
  dbutils.insert("db2crudtest",tbObj).then(function(rdata){
    console.log("retdata=" + JSON.stringify(rdata));
    tbObj = {ID:2,STR:"str2"};
    dbutils.insert("db2crudtest",tbObj).then(function(rdata){
      console.log("retdata=" + JSON.stringify(rdata));
      deferred.resolve(null);
    });
  });
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
