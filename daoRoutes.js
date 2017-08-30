var express = require('express');
var router = express.Router();
var dbutils = require('./tableOperate');

/**
 * Common routes for simple CRUD operation.
 * see also http://www.ruanyifeng.com/blog/2014/05/restful_api.html
 */

var OK = function (res, statusCode) {
  return function (results) {
    res.status(statusCode || 200);

    // note that if status code = 204 below line will be ignored
    res.json({op: 'success', data: results});
  };
};

var ERROR = function (res) {
  return function (err) {
    res.status(400);
    res.json({error: JSON.stringify(err)});
  };
};

// list
router.get('/:tbl', function (req, res, next) {
  var tbl = req.params.tbl;
  var data = JSON.parse(req.body.data || '{}');
  dbutils.get(tbl, data).then(OK(res), ERROR(res));
});

// create
router.post('/:tbl', function (req, res, next) {
  var tbl = req.params.tbl;
  var data = JSON.parse(req.body.data);
  dbutils.insert(tbl, data).then(OK(res, 201), ERROR(res));
});

// update
router.put('/:tbl/:id', function (req, res, next) {
  var tbl = req.params.tbl;
  var id = req.params.id;

  var data = JSON.parse(req.body.data);
  dbutils.update(tbl, data).then(OK(res, 201), ERROR(res));
});

// delete
router.delete('/:tbl/:id', function (req, res, next) {
  var tbl = req.params.tbl;
  var id = req.params.id;

  dbutils.removeById(tbl, id).then(OK(res, 204), ERROR(res));
});

// detail
router.get('/:tbl/:id', function (req, res, next) {
  var tbl = req.params.tbl;
  var id = req.params.id;

  dbutils.getById(tbl, id).then(OK(res), ERROR(res));
});

module.exports = router;
