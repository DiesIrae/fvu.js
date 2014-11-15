//--------------------------------------------------------------
// connect.js
//--------------------------------------------------------------
// 
// Fill mongo with the real FVU database
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

var fs = require("fs");
var csv = require("fast-csv");
var mongo = require("mongodb");
var dbUser = "tiago";
var dbPassword = "alceuamoroso65";

var db_path = __dirname + "/public/B_FVU_2000_2013_simp.csv";
// var stream = fs.createReadStream(db_path);

var db;
var MongoClient = mongo.MongoClient;

var mongoURI = "mongodb://"+dbUser+":"+dbPassword+"@ds051740.mongolab.com:51740/fvu";

MongoClient.connect(mongoURI, function(err, database) {
	if(err) throw err;
	console.log("Connected to Mongo!");
  	// reference to the database
  	db = database;

	db.collection("data-simp", function(err,collection){
		if(err) throw err;
		console.log("We have the collection");
		var stream = fs.createReadStream(db_path);
		stream.setEncoding('utf8');
		var n = 0;

		var csvStream = csv({headers : true})
			.on("data", function(d){
				n++;
				// console.log("reading line: ", d); // debug, show the data
				// csvStream.pause(); // debug, read only one line
				collection.insert(d, function(){
					console.log("Entry #%d inserted sucessfully!", n);
				});
			}) //csvStream.on(...)
			.on("end", function(d){
				console.log("finished reading the .csv!");
			}); //csvStream.on(...)

		//pipe method manages the reading flow in order to avoid overloading
		stream.pipe(csvStream);

	}); //db.collection(...)
}); //MongoClient.connect(...)


