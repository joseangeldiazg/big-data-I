var mapCode = function() {
		emit(
   	   		this.cuisine,
        	{
            	"data":
            	[
                	{
                    "name": this.name,
                    "address": this.address,
                    "lat":  this.adress.coord[0],
                    "lon":  this.adress.coord[1],
                }
            ]
        	}
    	);
	}

	var reduceCode = function(key, values) {
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


	var finalize =  function (key, reduced) {
		if (reduced.data.length == 1) {
			return {
	            "message" : "Este tipo de cocina solo contiene un restaurante"
	        };
		}
		var min_dist = 999999999999;
		var restaurante1 = {
	        "name": ""
	    };
		var restaurante2 = {
	        "name": ""
	    };
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
	        "restaurante2": restaurante2.name,
	        "evaluations": contador,
	        "dist": Math.sqrt(min_dist)
	    };
	}


	db.restaurants.mapReduce(
			mapCode,
			reduceCode,
			{
				  out: { merge: "rest_mapreduce" },
					query: { "grades.grade": "A" },
					finalize: finalize
			});

	db.restauranrtes_proximos.find().pretty();
