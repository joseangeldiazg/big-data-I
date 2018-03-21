var mapCode = function Map() {
		emit(
   	   		this.cuisine,
        	{
            	"data":
            	[
                	{
                    "name": this.name,
                    "lat":  this.address.coord[0],
                    "lon":  this.address.coord[1],
										"address": this.address.street,
                }
            ]
        	}
    	);
	}
	var reduceCode = function Reduce(key, values) {
	  var reduced = {
	        "data": []
	    };
	  for (var i in values) {
	    var inter = values[i];
	    for (var j in inter.data) {
	      reduced.data.push(inter.data[j]);
	    }
	  }
	  return reduced;
	}


	var finalizeCode =  function Finalize(key, reduced) {
		if (reduced.data.length == 1) {
			return {
	            "message" : "Este tipo de cocina solo contiene un restaurante"
	        };
		}
		var min_dist = 999999999999;
		var restaurante1 = { "name": "" , "address": ""};
		var restaurante2 = { "name": "" , "address": ""};
		var r1;
		var r2;
		var d;
	  var contador=0;
		for (var i in reduced.data) {
			for (var j in reduced.data) {
				if (i >= j) {
	                continue;
	            }
				r1 = reduced.data[i];
				r2 = reduced.data[j];
				d = (r1.lat - r2.lat) * (r1.lat - r2.lat) + (r1.lon - r2.lon) * (r1.lon - r2.lon);
	            if (d > 0) {
	                contador=contador+1;
	                if (d < min_dist) {
	                    min_dist = d;
	                    restaurante1 = r1;
	                    restaurante2 = r2;
	                }
	            }
			}
		}
		return {
	        "restaurante1": restaurante1.name,
					"Direccion_restaurante_1": restaurante1.address,
	        "restaurante2": restaurante2.name,
					"Direccion_restaurante_2": restaurante2.address,
	        "evaluations": contador,
	        "dist": Math.sqrt(min_dist)
	    };
	}
	db.runCommand({
		 mapReduce: "restaurants",
		 map: mapCode,
		 reduce: reduceCode,
		 finalize: finalizeCode,
		 query: { "grades.grade": 'A' },
		 out: { merge: "rest_mapreduce" },
		 });

	db.rest_mapreduce.find().pretty();
