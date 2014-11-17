//--------------------------------------------------------------
// query.js
//--------------------------------------------------------------
// 
// Query the fvu-mongo database using input filters and extract 
//   the proportions of each damage type.
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

var DAMAGE_TYPES = ['ACCI', 'AGRE', 'AGZB', 'ENCF', 'ENST', 
										'ENZB', 'INFI', 'RECH', 'REST', 'RPLA', 
										'USUR'];

var fs = require("fs");
var mongo = require("mongodb");
var dbHost = "127.0.0.1";
var dbPort = mongo.Connection.DEFAULT_PORT;

// start the mongo object
var db = new mongo.Db("fvu-simp",
	new mongo.Server(dbHost,dbPort,{}),
	{safe:false});

// parameters for the extraction
var filterParameters = {
	brand: 'RENAULT',
	exam_year: '2010'
};

db.open(function(error){
	console.log("We are connected to mongo! " + dbHost + ":" + dbPort);


	db.collection("user", function(error,collection){
		console.log("We have the mongo connection");

		// group by and count unique damage_codes
		collection.group(['damage_code'], {
				"damage_code": { "$ne" : '' }, //excludes empty
				"brand": filterParameters.brand,
				"exam_year": filterParameters.exam_year
			}, {"count":0}, "function (obj, prev) { prev.count++; }",
		function(error, res) {
			// Function to compare array of objects by count
			function compareDescending(a, b) {
				if (a.count < b.count) { return 1; };
				if (a.count > b.count) { return -1; };
				return 0;
			}
			res.sort(compareDescending);
			console.log(res);
		});

		// collection.distinct( 'damage_code' , {
		// 	"brand": filterParameters.brand,
		// 	"exam_year": filterParameters.exam_year
		// }, function(error, unique) {
		// 	console.log(unique);
		// });

		// var counts = 0;
		// collection.find({
		// 	"brand": filterParameters.brand,
		// 	"exam_year": filterParameters.exam_year
		// }, function(error, cursor) {
		// 	cursor.count(function(error, nbDocs) {
  //   			console.log(nbDocs);
		// 	});
		// 	cursor.toArray(function(error,items){
		// 		counts = items.length;
		// 		console.log(counts);
		// 	});
		// });
		

	}); // db.collection(...)
}); // db.open(...)





		// DAMAGE_TYPES.forEach(function(damage_type){

		// 	collection.find(
		// 		{
		// 			"brand": filterParameters.brand,
		// 			"exam_year": filterParameters.exam_year,
		// 			"damage_type": damage_type
		// 		}, function(error,cursor) {
		// 			cursor.toArray(function(error,items){
		// 				// add the data to the output array
		// 				df.push({dfam: damage_type, counts: items.length});
		// 				// trick to check if we completed all the assynchronous operations
		// 				if (df.length == DAMAGE_TYPES.length) {
		// 					console.log(df);
		// 				};
		// 			});
		// 		});  // collection.find(...)
		// 	}); // DAMAGE_TYPES.forEach(...)















