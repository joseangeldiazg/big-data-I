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


El primer paso será la función emit() esta recibe dos parámetros, la clave, que será el elemento por el que dividiremos o agruparemos y el valor, que podrá ser una estructura que contenga todos los valores que necesitemos para procesar en siguientes puntos.


	var mapCode = function() {
		emit(
   	   		this.cuisine,
        	{ 
            	"data":
            	[
                	{
                    "name": this.name,
                    "adress": this.adress,
                    "lat":  this.adress.coord[0],
                    "lon":  this.adress.coord[1],
                }
            ]
        	}
    	);
	}
 

	var reduceCode = function(key, values){
  		var reduced = {
        	"data": []
    	};
  		for (var i in values) {
  			var inter = values[i];
  			for (var j in inter.data){
  				reduced.data.push(inter.data[j]);
  			}
  		}
  		return reduced;
	}
	
	var finalize =  function (key, reduced) 
	{
		if (reduced.data.length == 1) 
		{
				return {"message" : "Este tipo de cocina solo contiene un restaurante"};
		}
		var min_dist = 999999999999;
		var restaurante1 = {
        	"restaurante": ""
    	};
		var restaurante2 = {
        	"restaurante": ""
    	};
		var r1;
		var r2;
		var d;
  		var contador=0;
		for (var i in reduced.data) 
		{
			for (var j in reduced.data) 
			{
				if (i >= j) 
				{
                	continue;
            	}
				r1 = reduced.data[i];
				r2 = reduced.data[j];
				d = (r1.lat - r2.lat) * (r1.lat - r2.lat) + (r1.lon - r2.lon) * (r1.lon - r2.lon);
            	if (d > 0) 
            	{
                contador=contador+1;
                		if (d < min_dist) 
                		{
                    	min_dist = d;
                    	restaurante1 = r1;
                    	restaurante2 = r2;
                		}
            	}
			}
		}
		return 
		{
        	"restaurante1": restaurante1.name,
        	"restaurante2": restaurante2.name,
        	"evaluations": contador,
        	"dist": Math.sqrt(min_dist)
    	};
	}
	
	db.restaurants.mapReduce(
    	mapCode,
    	reduceCode,
    	finalize,
    	query: { "grades.grade": { $eq: "A" } } ,
    	out: { merge: "rest_mapreduce" });


# Consulta Aggregate:


