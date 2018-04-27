#	Impala ETL


En esta práctica usaremos la herramienta Impala para generar algunas consultas de sobre una base de datos de gran tamaño que construiremos con la herramienta. La base de datos, está formada por más de 60000 registros y tiene información sobre quejas de usuarios de tarjeta de crédito en EEUU. 

[Enlace para descargar la base de datos](https://www.dropbox.com/s/jr9gae39oe2bpq6/ConsumerComplaints.csv)

## 1 Carga de datos

Dado que realizaremos las prácticas con el cluster de Hadoop de la UGR. Lo primero que hacemos es subir los datos a este cluster por medio de **scp**. Hay que tener en cuenta que deberemos sustituir user, por nuestro usuario. 


	scp ConsumerComplaints.csv user@hadoop.ugr.es:/home/user

## 2 Creación de la base de datos

Para poder generar las consultas sin interferir con las demás que se realicen en el cluster crearemos una base de datos propia. 

Para ello,una vez dentro del cluster de Hadoop, primero conectamos a impala-shell:

	impala-shell

Tras esto, podemos crear la base de datos:

	CREATE DATABASE CD_DNI IF NOT EXISTS	LOCATION '/user/impala/CD_DNI/impalastore.db';
	
Una vez creada, podemos mostrarla y usarla:

	DESCRIBE CD_DNI
	USE CD_DNI
	

## 3 Carga de los datos

Una vez creada la base de datos debemos cargar los datos en la misma, para ello, primero deberemos mandar los datos al HDFS de hadoop.


	 hdfs dfs -put ConsumerComplaints.csv /user/impala/CD_DNI/input
	 hdfs dfs -ls /user/impala/CD_DNI/input
	 
Si todo a ido bien, observaremos que la base de datos ahora está en el hdfs de hadoop, por lo que podremos cargar los datos en una tabla, para ello cargamos el impala-shell y usamos (comando USE) nuestra base de datos creada anteriormente. 	
	
	CREATE TABLE IF NOT EXISTS Complaints(DateReceived STRING, ProductName STRING, SubProduct STRING,Issue STRING, SubIssue STRING,ConsumerComplaintNarrative STRING,CompanyPublicResponse String, Company STRING, StateName STRING,ZipCode INT, Tags STRING, ConsumerConsentProvided STRING,SubmittedVia STRING, DateSenttoCompany STRING, CompanyResponsetoConsumer STRING, TimelyResponse STRING,ConsumerDisputed STRING, ComplaintID INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\,' STORED AS TEXTFILE;
	 
Tras esto  cargamos los datos:

 	LOAD DATA INPATH '/user/impala/CD_DNI/input/ConsumerComplaints.csv' OVERWRITE INTO TABLE complaints;
	 	
	 	
## 4 Diseño de un experimento de datos

En este punto, realizaremos las consultas sobre la base de datos para ello, nos pondremos en lugar de un hipótetico científico de datos para obtener información acerca de las quejas de los usuarios.

### Proceso exploratorio

Somos nuevos en la compañía y queremos saber:

**¿Cuales son las posibles fuentes de queja de nuestros usuarios?**


	SELECT DIFFERENT SubmittedVia FROM complaints;
	
Parace que la salida ofrece más resultados de los que cabría esperar, por lo que de momento, hemos descubierto que la tabla en origen tiene datos de otras columnas introducidos erroneamente. Esto es algo que deberiamos solventar y arreglar en los datos aún así, buscaremos aquellas vías que son más comunes haciendo uso de conteos para evitarnos cierto ruido y dar respuesta a la hipotética pregunta a resolver. 

	SELECT COUNT(*), SubmittedVia FROM complaints GROUP BY SubmittedVia HAVING COUNT(*) > 300;
	
 Parece que así ya sabemos cuales son las más comunes, igualmente siguen apareciendo algunas que no nos interesan por lo que utilizaremos WHERE para eliminarlas y ordenaremos para crear un ranking de las más usadas. 

	SELECT COUNT(*), SubmittedVia FROM complaints GROUP BY SubmittedVia WHERE SubmittedVia IN ('Web', 'Phone', 'Fax', 'Postal mail') HAVING COUNT(*) > 300 ORDER BY 1 DESC;Close	
	
Ya sabemos cuales son aquellos métodos más usados para comunicarse con la empresa, pero ahora se nos encarga obtener aquellos estados en los cuales la gente usa menos la web para comunicarse con la empresa con el fin de potenciar la publicidad de una nueva aplicación web de la compañía. 

	SELECT COUNT(*), SatateName FROM complaints GROUP BY SubmittedVia WHERE SubmittedVia = 'Web' HAVING COUNT(*) < 10;	
		
Sabemos por tanto que los estados que menos usan la web son: AE (Fuerzas armadas en Africa), 	FM (Los estados de la micronesia), GU (Guam), MH (Las Islas Marshall) y VI  (Islas Virgenes) y AP (Las fuerzas armadas del pacífico).

Esto nos ayuda poco, pues podemos comproabar que son casos aislados de soldados o personas destinadas en islas, y nosotros estamos interesados en los estados más relevantes de estados Unidos donde ofrecer nuestro nuevo producto por ello, exigiremos un mínimo de comunicaciones para evitar estos casos aislados, o outliers en nuestro problema.

	SELECT COUNT(*), SatateName FROM complaints GROUP BY SubmittedVia WHERE SubmittedVia = 'Web' HAVING COUNT(*) BETWEEN 100 and 200;


El resultado de esta consulta nos dice cuales son aquellos estados que menos usan la web, por lo que aquí deberíamos potenciar nuestro producto, si los analizamos, excepto Delawere, los demás son estados de predominio rural algo que era de esperar.




	
	 	