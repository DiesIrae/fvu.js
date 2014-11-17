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
var filterParameters1 = {
	brand: 'RENAULT',
	exam_year: '2010'
};

// parameters for the extraction
var filterParameters2 = {
	brand: 'TATA',
	exam_year: '2010'
};

var collection;

db.open(function(error){
	console.log("We are connected to mongo! " + dbHost + ":" + dbPort);


	db.collection("user", function(error,col){
		console.log("We have the mongo connection");

		collection = col;

		var brand1 = filterParameters1.brand;
		var exam_year1 = filterParameters1.exam_year;
		var brand2 = filterParameters2.brand;
		var exam_year2 = filterParameters2.exam_year;

		countDamageCodes2([brand1, brand2], [exam_year1, exam_year2], function(dCounts){
			var n = Math.min(dCounts.length, 20);
			var dmCounts = [];
			for ( i = 0; i<n; i++){
				var dmCode = dCounts[i].damage_code;
				var totCounts = dCounts[i].count;
				var dmCount = {
					"damage_code": dmCode,
					"total_counts": totCounts
				};

				countDamageCode(brand1, exam_year1, dmCount, function(cts1, dmCount){
					dmCount['f1'] = cts1;
					countDamageCode(brand2, exam_year2, dmCount, function(cts2, dmCount){
						dmCount['f2'] = cts2;
						dmCounts.push(dmCount);
						if (dmCounts.length == n) {
							// enters if all extraction is complete

							// function to sort
							function compareDescending(a, b) {
								if (a.total_counts < b.total_counts) { return 1; };
								if (a.total_counts > b.total_counts) { return -1; };
								return 0;
							}
							dmCounts.sort(compareDescending);

							// results
							console.log(dmCounts);
						}
					});
				});
			} // for
		}); //countDamageCodes2(...)
	}); // db.collection(...)
}); // db.open(...)


function countDamageCode(brand, exam_year, dmCount, callback) {
	collection.count(
        {
          "brand": brand,
          "exam_year": exam_year,
          "damage_code": dmCount.damage_code
        },
        function (err, n) {
        	callback(n, dmCount);
        }
    );	
}


function countDamageCodes(brand, exam_year, callback) {
	// group by and count unique damage_codes
	collection.group(['damage_code'], {
			"damage_code": { "$ne" : '' }, //excludes empty
			"brand": brand,
			"exam_year": exam_year
		}, {"count":0}, "function (obj, prev) { prev.count++; }",
	function(error, res) {
		// Function to compare array of objects by count
		function compareDescending(a, b) {
			if (a.count < b.count) { return 1; };
			if (a.count > b.count) { return -1; };
			return 0;
		}
		res.sort(compareDescending);
		callback(res);
	});
}


function countDamageCodes2(brandArray, examYearArray, callback) {
	// group by and count unique damage_codes
	collection.group(['damage_code'], {
			"damage_code": { "$ne" : '' }, //excludes empty
			"brand": {"$in" : brandArray},
			"exam_year": {"$in" : examYearArray}
		}, {"count":0}, "function (obj, prev) { prev.count++; }",
	function(error, res) {
		// Function to compare array of objects by count
		function compareDescending(a, b) {
			if (a.count < b.count) { return 1; };
			if (a.count > b.count) { return -1; };
			return 0;
		}
		res.sort(compareDescending);
		callback(res);
	});
}












