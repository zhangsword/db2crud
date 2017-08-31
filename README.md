DB2CRUD
=========

tools of node.js for crud on db2

## Installation

  `npm install db2crud`

## Usage
  
    // select * from db2crudtest where ID=2 and STR='str3';
    
    var tbObj = {
      ID : 2,
      STR : "str3"
    };
    dbutils.get("db2crudtest", tbObj).then(function(rdata) {
      console.log("retdata=" + JSON.stringify(rdata));
    })

    // delete from db2crudtest where ID=2 and STR='str3'
    var tbObj = {
      ID : 2,
      STR : "str3"
    };
    dbutils.remove("db2crudtest", tbObj).then(function(rdata) {
      console.log("retdata=" + JSON.stringify(rdata));
    })

    // update db2crudtest set STR='str3' where ID=2
    var tbObj = {
      ID : 2,
      STR : "str3"
    };
    dbutils.get("db2crudtest", tbObj).then(function(rdata) {
      console.log("retdata=" + JSON.stringify(rdata));
    })

    // insert db2crudtest(ID,STR) VALUES (2,"str2")
    tbObj = {
      ID : 2,
      STR : "str2"
    };
    dbutils.insert("db2crudtest", tbObj).then(function(rdata) {
      console.log("retdata=" + JSON.stringify(rdata));
    })

You can refer test.js for further detail!

## Tests

  node test

## Contributing

