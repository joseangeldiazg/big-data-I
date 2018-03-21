# Almacenamiento Masivo

En esta práctica usaremos Mongo DB, para almacenamiento masivo de datos. La base de datos sobre la que trabajaremos contiene información sobre restaurantes. 


## Importación de datos

Para comenzar a trabajar tendremos que tener la base de datos creada en nuestra cuenta de Hadoop y Mongo DB. El primer paso es captar los datos en mongo db. Para ello una vez dentro del cluster Hadoop:

	mongoimport -u mdat_DNI -p BDat.2018 --db db_mdat_DNI --collection restaurants --type json --drop --file /var/tmp/restaurantes1.json
	
Tras esto, podremos conectarnos a la base de datos y comenzar con las consultas:

 	mongo localhost:27017/db_mdat_DNI -u mdat_DNI -p BDat.2018
 	
 	
## Consulta Map Reduce:

Tendremos que generar esta consulta;
	
"Obtener el par de restaurantes más próximos de cada tipo de cocina (cuisine), mostrando el nombre, la dirección, la distancia entre ellos y la cantidad de restaurantes evaluados para cada tipo de cocina, para aquellos restaurantes que hayan tenido un grado= ‘A’ en alguna ocasión". 


El primer paso será la función emit() esta recibe dos parámetros, la clave, que será el elemento por el que dividiremos o agruparemos y el valor, que podrá ser una estructura que contenga todos los valores que necesitemos para procesar en siguientes puntos. Tras esta funcion, crearemos la función reduce y la función finalize que aplica la lógica del proceso.

El código final sería el siguiente:


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


# Consulta Aggregate:

db.runCommand({
aggregate: "restaurants",
pipeline : [
   {$match: {"grades.grade" : "A"}}, //Solo considero los registros que hayan tenido al menos una calificacion "A"
    //Agrupa por cÛdigo de cocina y le aÒade los arrays rest1 y rest2 con los datos de los restaurantes de ese tipo de cocina
   {$group: {
   				_id: "$cuisine", 
   				"rest1":{ $push: 
   					{resID: "$restaurant_id", 
   					 nombre:"$name", 
   					 dir:"$address"}}, //, lat:"$Latitude",lon:"$Longitude"}},
                                "rest2":{$push: {resID: "$restaurant_id", nombre:"$name", dir:"$address"}}}},
   {$unwind: "$rest1"}, //Desanida rest1, crea un documento por cada elemento del array rest1
   {$unwind: "$rest2"}, //Desanida rest2 crea un documento por cada elemento del array rest2
    //Calcula la distancia entre cada par de restaurantes en el campo ìdistanciaî, devuelve otros datos necesarios.
   {$project: {_id: 0, Cocina: "$_id", rest1: "$rest1", rest2: "$rest2",
         distancia:{ $sqrt: {$sum: [{$pow: [{$subtract: [{$arrayElemAt: ["$rest1.dir.coord",0]},{$arrayElemAt: ["$rest2.dir.coord",0]}]},2 ]},
                                    {$pow: [{$subtract: [{$arrayElemAt: ["$rest1.dir.coord",-1]},{$arrayElemAt: ["$rest2.dir.coord",-1]}]},2 ]}]}}}},
      // Eliminamos parejas de ciudades redundantes y aquellas parejas que est·n a distancia 0.
   {$redact: {"$cond": [{$and:[{"$lt": ["$rest1.resID", "$rest2.resID"]},{"$ne":["$distancia",0.0]}]},"$$KEEP","$$PRUNE"]}},
   {$group: {_id: "$Cocina", "dist_min": {$min: "$distancia"}, // Obtenemos las distancia mÌnima para cada paÌs
                            // AÒadimos a la salida un ìarrayî con los datos de todas las parejas de ciudades de cada PaÌs
                           "parejas":{$push: {rest1: "$rest1", rest2: "$rest2", distancia: "$distancia"}}}},
   {$unwind: "$parejas"}, // Desanidamos el ìarrayî parejas
    // Nos quedamos con aquellas parejas cuya distancia coincide con la distancia mÌnima de ese paÌs
   {$redact: {"$cond": [{"$eq": ["$dist_min", "$parejas.distancia"]}, "$$KEEP", "$$PRUNE"]}},
   // Proyectamos sobre los datos solicitados
   {$project: {_id: 0, "TipoCocina": "$_id", "Restaurante1": "$parejas.rest1.nombre",
       "Direccion1":"$parejas.rest1.dir", "Restaurante2": "$parejas.rest2.nombre",
       "Direccion2":"$parejas.rest2.dir", "distancia": "$dist_min"}}
  ],
 cursor: { batchSize: 20000} ,
 allowDiskUse: true} // Permite el uso de disco para operaciones intermedias que no quepan en memoria
); 


