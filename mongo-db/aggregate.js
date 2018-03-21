db.runCommand({
aggregate: "restaurants",
pipeline : [
   {$match: {"grades.grade" : "A"}}, //Solo considero los registros que hayan tenido al menos una calificacion "A"
    //Agrupa por cÛdigo de cocina y le aÒade los arrays rest1 y rest2 con los datos de los restaurantes de ese tipo de cocina
   {$group: {_id: "$cuisine", "rest1":{$push: {resID: "$restaurant_id", nombre:"$name", dir:"$address"}}, //, lat:"$Latitude",lon:"$Longitude"}},
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
