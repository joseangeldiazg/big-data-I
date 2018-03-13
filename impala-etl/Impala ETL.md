#	Impala ETL


En esta práctica usaremos la herramienta Impala para generar algunas consultas de sobre una base de datos de gran tamaño que construiremos con la herramienta. La base de datos, está formada por más de 60000 registros y tiene información sobre quejas de usuarios de tarjeta de credito en EEUU. 

## 1 Carga de datos

Dado que realizaremos las prácticas con el cluster de Hadoop de la UGR. Lo primero que hacemos es subir los datos a este cluster por medio de **scp**.


	scp ConsumerComplaints.csv CD_76139799@hadoop.ugr.es:/home/CD_76139799 


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
	
	 CREATE TABLE IF NOT EXISTS Complaints (DateReceived TIMESTAMP, 
	 	ProductName STRING, SubProduct STRING, 
	 	Issue STRING, SubIssue STRING, 
	 	ConsumerComplaintNarrative STRING, 
	 	Company STRING, StateName STRING,
	 	ZipCode INT, Tags STRING, 
	 	ConsumerConsentProvided STRING, 
	 	SubmittedVia STRING, DateSenttoCompany TIMESTAMP,
	 	CompanyResponsetoConsumer STRING, TimelyResponse STRING,
	 	ConsumerDisputed STRING, ComplaintID INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;	 
	 
Tras esto  cargamos los datos:

 	LOAD DATA INPATH '/user/impala/CD_DNI
 	/input/ConsumerComplaints.csv' 
 	OVERWRITE INTO TABLE complaints;
	 	
	 	