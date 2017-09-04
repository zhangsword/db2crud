DB2CRUD
=========

tools of node.js for crud on db2

## Installation

  `npm install db2crud`

## Usage
  
  Firstly, you need init db2 connection before usage.
  
    var db2crud = require('db2crud');
    db2crud.init({db:db2,'TEST'});
  
    // select * from db2crudtest where ID=2 and STR='str3';
    var tbObj = {
      ID : 2,
      STR : "str3"
    };
    db2crud.get("db2crudtest", tbObj).then(function(rdata) {
      console.log("retdata=" + JSON.stringify(rdata));
    })

    // delete from db2crudtest where ID=2 and STR='str3'
    var tbObj = {
      ID : 2,
      STR : "str3"
    };
    db2crud.remove("db2crudtest", tbObj).then(function(rdata) {
      console.log("retdata=" + JSON.stringify(rdata));
    })

    // update db2crudtest set STR='str3' where ID=2
    var tbObj = {
      ID : 2,
      STR : "str3"
    };
    db2crud.update("db2crudtest", tbObj).then(function(rdata) {
      console.log("retdata=" + JSON.stringify(rdata));
    })

    // insert db2crudtest(ID,STR) VALUES (2,"str2")
    tbObj = {
      ID : 2,
      STR : "str2"
    };
    db2crud.insert("db2crudtest", tbObj).then(function(rdata) {
      console.log("retdata=" + JSON.stringify(rdata));
    })
    
    // insert db2crudtest(ID,STR) VALUES (3,"str3")
    // insert db2crudtest(ID,STR) VALUES (4,"str4")
    tbObj = [{ID:3,STR:"str3"},{ID:4,STR:"str4"}];
    dbutils.insert("db2crudtest",tbObj).then(function(rdata){
      console.log("retdata=" + JSON.stringify(rdata));
    });
    
Error Check

		//retdata=    [  
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
    tbObj = [{NUM:"2",STR:"2",DATEFIELD:"2017-09-03 05:25:00"}
           ,{NUM:3,STR:3,DATEFIELD:"2017-09-03 05:25:00"}
           ,{NUM:3,STR:3,DATEFIELD:"abc"}
           ,{NUM:3,STR:"123456",DATEFIELD:"2017-09-03 05:25:00"}];
    dbutils.insert("db2crudtest",tbObj).then(function(rdata){
          console.log("retdata=" + JSON.stringify(rdata));
    });
    

You can refer test.js for further detail!

## Tests

  node test

## Contributing

  Email:sword_zhang@163.com
  
  QQ:974566030

##License
  
  Copyright(c) 2017 Jian Zhang

  MIT Licensed