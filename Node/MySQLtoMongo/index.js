var mysql = require('mysql');
var mongo = require('mongodb').MongoClient
  , assert = require('assert');

//MySQL connection SOURCE
var connection = mysql.createConnection({
  host     : 'localhost',
  user     : 'root',
  password : 'root03',
  database : 'isv2'
});
connection.connect();
console.log('Conected to MySQL DataBase');

//MONGO connection TARGET
var url = 'mongodb://localhost:27017/megaman';
mongo.connect(url, function(err, db) {
  assert.equal(null, err);
  console.log('Conected to MongoDB');
  var collection = db.collection('isv2');
  var offset = 0;
  connection.query(' select count(*) as `count` ' +
                    'from `isv_semanas_tb` as `t`'
                    ,
  function(err, count, fields) {
    count = parseInt(count[0].count);
    qMySQL(offset, count, collection);
  });
});

function qMySQL(offset, count, collection){
  connection.query(' select t.*, g.*, p.* ' +
                    'from `isv_semanas_tb` as `t`' +
                    'left join `isv_combi_geo_tb` as `g` on `g`.`com_geo_agrup_0` = `t`.`com_geo_agrup_0`' +
                    'left join `isv_combi_prod_tb` as `p` on `p`.`com_prod_agrup_0` = `t`.`com_prod_agrup_0`' +
                    'limit 100000 ' +
                    'offset ' + offset
                    ,
  function(err, rows, fields) {
    console.log(parseInt(offset/count*100) + "%")
    if (err) throw err;
    if ( rows.length == 0 ){
      db.close();
      process.exit();
    }
    mongoInsert(collection, rows, function(err, msg){
      offset += 100000
      qMySQL(offset, count, collection)
    });
  });
}

function mongoInsert(collection, rows, cb){
  if( rows.length == 0 )
    cb(null, "done");
  else{
    row = rows.pop();
    row["com_geo_agrup"] = {};
    row["com_geo_agrup"]["com_geo_agrup_0"] = row["com_geo_agrup_0"];
    row["com_geo_agrup"]["com_geo_agrup_1"] = row["com_geo_agrup_1"];
    row["com_geo_agrup"]["com_geo_agrup_2"] = row["com_geo_agrup_2"];
    row["com_geo_agrup"]["com_geo_agrup_3"] = row["com_geo_agrup_3"];
    row["com_geo_agrup"]["com_geo_agrup_4"] = row["com_geo_agrup_4"];
    row["com_geo_agrup"]["com_geo_agrup_5"] = row["com_geo_agrup_5"];
    row["com_geo_agrup"]["com_geo_agrup_6"] = row["com_geo_agrup_6"];
    delete row["com_geo_agrup_0"];
    delete row["com_geo_agrup_1"];
    delete row["com_geo_agrup_2"];
    delete row["com_geo_agrup_3"];
    delete row["com_geo_agrup_4"];
    delete row["com_geo_agrup_5"];
    delete row["com_geo_agrup_6"];
    row["com_prod_agrup"] = {};
    row["com_prod_agrup"]["com_prod_agrup_0"] = row["com_prod_agrup_0"];
    row["com_prod_agrup"]["com_prod_agrup_1"] = row["com_prod_agrup_1"];
    row["com_prod_agrup"]["com_prod_agrup_2"] = row["com_prod_agrup_2"];
    row["com_prod_agrup"]["com_prod_agrup_3"] = row["com_prod_agrup_3"];
    row["com_prod_agrup"]["com_prod_agrup_4"] = row["com_prod_agrup_4"];
    row["com_prod_agrup"]["com_prod_agrup_5"] = row["com_prod_agrup_5"];
    row["com_prod_agrup"]["com_prod_agrup_6"] = row["com_prod_agrup_6"];
    delete row["com_prod_agrup_0"];
    delete row["com_prod_agrup_1"];
    delete row["com_prod_agrup_2"];
    delete row["com_prod_agrup_3"];
    delete row["com_prod_agrup_4"];
    delete row["com_prod_agrup_5"];
    delete row["com_prod_agrup_6"];
    collection.insert(row, function(err, result) {
      assert.equal(err, null);
      mongoInsert(collection, rows, cb);
    });
  }
}
