
// Constants
var DAMAGE_TYPES = ['ACCI', 'AGRE', 'AGZB', 'ENCF', 'ENST', 
                    'ENZB', 'INFI', 'RECH', 'REST', 'RPLA', 
                    'USUR'];

// Starting express and socket
var fs = require("fs");
var express = require("express");
var app = express();
var server = require("http").Server(app);
var io = require("socket.io")(server);
var csv = require("fast-csv");
var mongo = require("mongodb");

// Configurations
var dbConfig = JSON.parse(fs.readFileSync("dbConfig.json"));
var serverHost = "127.0.0.1";
var serverPort = 3000;

// Delivering public files
app.use(express.static(__dirname + "/public"));

app.get("/", function(req, res){
	res.sendFile(__dirname + "/index.html");
});

// db object
var db;
var MongoClient = mongo.MongoClient;
var mongoURI = "mongodb://"+dbConfig.dbUser+":"+dbConfig.dbPassword+dbConfig.dbAddress;

// connects with the database
MongoClient.connect(mongoURI, function(err, database) {
  if(err) throw err;

  // global reference to the database
  db = database;

  // define socket.io connections
  io.on("connection", function(socket){
  	console.log("A user connected");
  	
  	socket.on("disconnect", function(){
  		console.log("user disconnected");
  	}); //socket.on(...)

    // request to get unique values from the database
    socket.on("getUnique", function(uniqueColumns){
      // example of uniqueColumns: ['brand','exam_year']
      uniqueFilterItems(uniqueColumns, function(uniqueDict) {
        // console.log(uniqueDict);
        io.emit('uniqueDict', uniqueDict);
      });
    }); //socket.on(...)

    // Deals with damage family request
    socket.on("extractDfamData", function(params){
      console.log('params: ', params.length);
      var n = params.length; // to check if assynchronous counting is over
      var msgs = [];
      params.forEach(function(param){
        extractDatabase(param, function(data){
          var msg = {
            data : data,
            color : param.color
          };
          msgs.push(msg);
          if (msgs.length == n) { //if all extractions are done
            console.log(msgs[0]);
            console.log(msgs[1]);
            io.emit('updateDfamGraph', msgs);
          }; // if
        }); // extractDatabase(...)
      }); // params.forEach


    }); //socket.on(...)

  }); //io.on(...)
}); // MongoClient.connect(...)

// start the server
server.listen(serverPort, serverHost, function(){
  var host = server.address().address;
  var port = server.address().port;
  console.log('Listening at http://%s:%s', serverHost, serverPort);
});


//-------------------------------------------------------
// extract the database
//-------------------------------------------------------
// filterParameters = 
// {
//   brand: 'RENAULT',
//   year: '2010'
// }
// 
// Collection = {
//  exam_month: '2',
//  exam_year: '2000',
//  performance: '228799',
//  repairs: '3',
//  customer_name: 'VIACAO URBANA LTDA',
//  city: 'Fortaleza',
//  state: 'CE',
//  brand: 'RENAULT',
//  fabrication_week: '12',
//  fabrication_year: '1997',
//  product_age: '2.855',
//  damage_type: 'RECH',
//  damage_code: '' }
//
function queryMongo(filterParameters, callback) {


  var df = []; //empty array to store the countings

  db.collection("data-simp", function(error,collection){
    console.log("We have the mongo collection");

    DAMAGE_TYPES.forEach(function(damage_type){

      collection.find(
        {
          "brand": filterParameters.brand,
          "exam_year": filterParameters.year,
          "damage_type": damage_type
        }, function(error,cursor) {
          cursor.toArray(function(error,items){
            // add the data to the output array
            df.push({dfam: damage_type, counts: items.length});
            // trick to check if we completed all the assynchronous operations
            if (df.length == DAMAGE_TYPES.length) {
              console.log("Sucessfully extracted! brand: " + filterParameters.brand +
                          ", exam_year: " + filterParameters.year);
              callback(df);
            };
          });
        });  // collection.find(...)
      }); // DAMAGE_TYPES.forEach(...)
  }); //db.collection(...)
}

function extractDatabase(params, callback) {
	// params: [year, brand, color]

  queryMongo(params,function(d){
    if (d) { // d == false if no data is found
      
      // Array that will contain the extracted objects
      var df = [];

      // count and convert to d3 interface format
      var total = 0;
      
      // d is unordered because extraction is assynchronous
      DAMAGE_TYPES.forEach( function(damage_type) {
        d.forEach(function(d_item){
          if (d_item.dfam == damage_type) {
            // console.log("matched ", d_item);
            df.push(d_item);
            total += d_item.counts;
          }
        }); //d.forEach(...)  
      }); //DAMAGE_TYPES.forEach(...)

      if (total>0) {
        df.forEach(function(df_item, index){
          df[index].counts = df[index].counts / total;
          df[index].counts = df[index].counts.toFixed(3);
        }); //d.forEach(...)  
      }

      // return the data
      callback(df); 

    } else { //an error occured
      console.log("Error extracting the database!");
      console.log("Returning zeros...");
      callback(false);
    }
  
  });

};

function uniqueFilterItems(filterColumns, callback) {
// input = array listing the columns to check
// output = uniquePerColumn:
//    keys: columns in filterColumns
//    values: array with the unique values on each column
  db.collection("data-simp", function(error,collection){
    if (error) { 
      throw error;
    } else {
      console.log("We have the mongo collection");
    };

    var uniquePerColumn = {};
    var columnsProcessed = 0;

    filterColumns.forEach(function(column){
      
      // Distinct values of the column
      collection.distinct( column , function(error, unique) {
        // console.log("Unique " + column + ": ", unique);
        uniquePerColumn[column] = unique;
        columnsProcessed++;
        if (columnsProcessed >= filterColumns.length) {
          callback(uniquePerColumn);
        }
      }); // collection.distinct(...)
      
    }); //filterColumns.forEach(
  }); //db.collection(...)
}

