# Globant’s Data Engineering Coding Challenge
## API Request


## Base de datos 
Se utiliza Bigquery como base de datos, en ella se pueden encontrar 3 tablas:
    - jobs
    - departments
    - hired_employees
Cada una de ellas tiene la estructura de los csv.


## Cómo realizar las consultas

Las consultas se realizan a traves de un contender deployado en Cloud RUN de gcp

## Challenge 1: 
## Create a Rest API service to receive new data:

    Request: https://globant-challenge-425423236751.us-central1.run.app/Insertbatch?table=jobs
    Insertbatch es el Endpoint.  
        -   table: Es el argumento que recibe el nombre de la tabla en donde se insertarán los datos ( en el ejemplo jobs, pero puede ser cualquiera de las tres tablas).  
        -   file: es el argumento que recibe el archivo csv que se insertará en bloque a la tabla referenciada en el parámetro table.  
    
    El resultado de esta consulta será el mensaje de inserción correcta: "Se insertaron los valores correctamente en la tabla ..."
    Al ejecutar el request se insertan en la "tabla" el contenido del archivo "file". Para las pruebas he usado postman:
    
 ## Create a feature to backup for each table and save it in the file system in AVRO format:
 
    Request: https://globant-challenge-425423236751.us-central1.run.app/Backup?table=jobs
    Backup es el Endpoint.  
        -   table: Es el argumento que recibe el nombre de la tabla de la que se obtendrá el backup ( en el ejemplo jobs, pero puede ser cualquiera de las tres tablas). 
    El resultado de esta consulta será el mensaje: "Se obtuvo AVRO de tabla ... y se copió archivo a Cloud Storage de manera correcta".
    Al ejecutar la request se inserta en un bucket un archivo avro con el formato nombredetabla_fechaactual.avro (fechaactual tiene el formato ddmmyyyy)

## Create a feature to restore a certain table with its backup:  

    Request: https://globant-challenge-425423236751.us-central1.run.app/Restore?name_file=nombredetabla_fechaactual&table=jobs
    Backup es el Endpoint.  
        -   table: Es el argumento que recibe el nombre de la tabla que se restaurará ( en el ejemplo jobs, pero puede ser cualquiera de las tres tablas). 
        -   name_file: Nombre del archivo AVRO
    El resultado de esta consulta será el mensaje: "Se obtuvo AVRO de tabla ... y se copió archivo a Cloud Storage de manera correcta".
    
## Challenge 2:
  
## Primera consulta:
    Request: https://globant-challenge-425423236751.us-central1.run.app/Requerimiento1
    La consulta tendrá como salida el resultado de la query que resuelve el primer requerimiento. Esta salida estará en formato JSON
## Segunda consulta:
 
    Request: https://globant-challenge-425423236751.us-central1.run.app/Requerimiento2
    La consulta tendrá como salida el resultado de la query que resuelve el segundo requerimiento. Esta salida estará en formato JSON


## Importación de librerias
Se importan las librerías:Flask,Pandas,Google,werkzeug,fastavro.
Y se llama al archivo credentials.py que contiene la información de credenciales para conexión con GCP (Bigquery) .
##El archivo json que contiene las credenciales no se ha subido al git por temas de seguridad, sin embargo esto está en el build del contenedor donde se puede realizar las consultas

```python

from flask import Flask,request,jsonify,make_response
from flask_restful import Resource,Api, reqparse
from google.cloud import bigquery,storage
from google.oauth2 import service_account
import io
from datetime import date
import fastavro
import credentials
import logging
```
## Importación de credenciales, conexión con API de Bigquery, Flask

A partir del archivo de service account se genera las credenciales que permitirá interactuar con la API de Bigquery (Bigquery.Client)

```python

credentials = service_account.Credentials.from_service_account_file(
    credentials.path_to_service_account_key_file, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
 
client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
```

Creación del objeto de la APP de Flask y seteo del api
```python
app = Flask(__name__)
api = Api(app)
```
## CLASS Insertbatch:
Esta clase permitirá la inserción  de data en bigquery, la data a insertar será la contenida en un csv. Se toma como parámetro el nombre de la tabla destino.

```python
class Insertbatch(Resource):
```    
Método archivo_permitido permite identificar si el archivo a enviar es de extensión CSV
```python
    def archivo_permitido(self, filename):
        ALLOWED_EXTENSIONS = {'csv'}
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
```
Método Post
```python
    def post(self):
```
Se setea el argumento que será pasado por el método Post. El argumento a pasar es el nombre de la tabla en donde se realizará la inserción de la data
```python
        parser = reqparse.RequestParser()          
        parser.add_argument('table',type=str, required=True,location='args')
        args = parser.parse_args()  
```
La variable Table_id contiene la información del proyecto mas el dataset y la tabla (la cual se obtiene del argumento mencionado en el punto anterior)
```python
        table_id="proyectoglobant2905.proyecto."+str(args['table'])
```
En caso se enviar la consulta y en ella nos e encuentre el archivo, el mensaje será "Archivo no encontrado"
```python
        if 'file' not in request.files:
            return 'Archivo no encontrado'
```   
Se captura  el archivo csv
```python     
        file = request.files.get("file")

        
```   
Se configura la carga del archivo csv a bigquery en modo WRITE_APPEND (se inserta data)
```python      
        if file and self.archivo_permitido(file.filename):          
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV, skip_leading_rows=0, autodetect=False,field_delimiter=",",
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )
```
Se procede con la carga del archivo a bigquery, se toma como parámetro el table_id.
```python
            job = client.load_table_from_file(file.stream, table_id, job_config=job_config)
 ```   
Se obtiene resultado del job
```python            
            job.result()  
             
            client.get_table(table_id) 
  ```   
Se envía mensaje de inserción exitosa
```python          
            mensaje=    "Se insertaron los valores correctamente en la tabla  {}".format(
                     table_id
                )
             
            

        return mensaje
```

## CLASS Backup:
Esta clase obtiene un backup de la tabla especificada y genera automáticamente un archivo AVRO (con el nombre de la tabla y la fecha actual)

Se definen los esquemas que deben contener los archivos AVRO (misma estructura que las tablas)

```python
def schemas_table(self,name):
        

        if name=="departments":
            schema = {
                "namespace": "com.schema.data",
                "type": "record",
                "name": name    ,
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "department", "type": "string"}
              
                ]
            }
...
```

Se obtienen el valor del argumento "table" y se obtiene los datos de la tabla con ese nombre:
```python 
def post(self):
        today = date.today()
        day = today.strftime("%d%m%Y")

        parser = reqparse.RequestParser()          
        parser.add_argument('table',type=str, required=True,location='args')
        args = parser.parse_args()  
        table_id="proyecto."+str(args['table'])
        query = f"SELECT * FROM {table_id}"
        query_job = client.query(query)  
        results = query_job.result() 
```

Detecta si el esquema es el correcto y genera archivo AVRO
```python

        bytes_writer = io.BytesIO()
        
        schema=self.schemas_table(str(args['table']))
        records = []
        for row in results:
            record = {field['name']: row[field['name']] for field in schema['fields']}
            records.append(record)
        
        if schema is None:
            return "Esquema no encontrado para la tabla {}".format(args['table'])
        try:   
            blob_name = f"backups_gb/{args['table']}_{day}.avro" 
            fastavro.writer(bytes_writer, schema,records) 
            
            
            bucket_name = "proyectoglobant2905"
            
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.upload_from_string(bytes_writer.getvalue(), content_type='application/octet-stream')
            mensaje=    "Se obtuvo AVRO de tabla {} y se copió archivo a Cloud Storage de manera correcta".format(
                        table_id
                    )
                
                

            return mensaje    
        except Exception as e:
            return f"Error al escribir el archivo Avro: {str(e)}"


```

## CLASS Restore:
Esta clase restaura una tabla especificada a partir del nombre de un archivo:

```python
def post(self):
        today = date.today()
        parser = reqparse.RequestParser()          
        parser.add_argument('name_file',type=str, required=True,location='args')
        parser.add_argument('table',type=str, required=True,location='args')
        args = parser.parse_args()  
        table_id="proyectoglobant2905.proyecto."+str(args['table'])
        bucket_name = "proyectoglobant2905"
        blob_name =  f"backups_gb/{args['name_file']}.avro"  
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.AVRO,
            autodetect=True, 
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE 
        )   
        # Start the load job
        uri = f"gs://{bucket_name}/{blob_name}"
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)

        # Wait for the job to complete
        load_job.result()
        mensaje=    "Se restauró la tabla {} ".format(
                     table_id
                )
             
            

        return mensaje   


```



## CLASS: Requerimiento1
Hace referencia al primer requerimiento de la Sección 2. End Point --> Requerimiento1
```python
class Requerimiento1(Resource):
```
Método get:
```python     
    def get(self):
```
Se asigna la query al client para su ejecución en Bigquery
```python 
        query_job = client.query(
                """
                select
                    department,
                    job,
                    sum(Q1) as Q1,
                    sum(Q2) as Q2,
                    sum(Q3) as Q3,
                    sum(Q4) as Q4
                    from (
                        select
                        department,
                        job,
                        case when Quarter=1 then cantidad else 0 end as Q1,
                        case when Quarter=2 then cantidad else 0 end as Q2,
                        case when Quarter=3 then cantidad else 0 end as Q3,
                        case when Quarter=4 then cantidad else 0 end as Q4,
                        from (
                            select
                            b.department, 
                            c.job,
                            EXTRACT(quarter FROM  date(a.datetime)) Quarter,
                            count(1) cantidad
                            from proyecto.hired_employees a
                            inner join
                            proyecto.departments b
                            on a.department_id=b.id
                            inner join
                            proyecto.jobs c
                            on c.id=a.job_id
                            where EXTRACT(Year FROM  date(a.datetime)) =2021
                            group by 
                            b.department, 
                            c.job,
                            a.datetime
                        )
                    ) group by 
                    department,
                    job
                    order by department,job asc

                
                
                """
            )
```
Se utiliza to_dataframe() para la conversión del resultado de la query en un dataframe que luego se convierte a un tipo de dato diccionario para que pueda ser retornado en ese formato al momento de la consulta en el api (método get)
  
```python 
        job_result = query_job.to_dataframe()  
        data = job_result.to_dict() 

        return {'data': data}, 200  
 ```  
  

## CLASS: Requerimiento2
Hace referencia al segundo requerimiento de la Sección 2. End Point --> Requerimiento2

```python 
 
class Requerimiento2(Resource):
```
Se asigna la query al client para su ejecución en Bigquery
```python 
     
    def get(self):
        query_job = client.query(
                """
                with 
                    total as (
                        select
                        a.department_id,
                        b.department,
                        extract(year from date(a.datetime)) year,
                        count(1) cantidad
                        from proyecto.hired_employees a
                        inner join
                        proyecto.departments b
                        on a.department_id=b.id
                        group by
                        a.department_id,
                        b.department  ,
                        a.datetime
                    )
                    ,
                    datosacum as (
                        select Year,sum(cantidad) total_cont,count(distinct department) cantidad_dep,   sum(cantidad)/count(distinct department) as promedio
                        from total
                        group by Year
                    ),
                    promedio as (
                        select department_id,department, num_contrata from (
                                select department_id,department,sum(cantidad) num_contrata  , (select promedio from datosacum where year=2021) promedio
                                from total 
                                group by department_id,department
                                )
                                where num_contrata>promedio
                                order by num_contrata desc
                    )
                    select * from promedio

                
                
                """
            )
```
Se utiliza to_dataframe() para la conversión del resultado de la query en un dataframe que luego se convierte a un tipo de dato diccionario para que pueda ser retornado en ese formato al momento de la consulta en el api (método get)
```python
        job_result = query_job.to_dataframe()  
        data = job_result.to_dict()  
        return {'data': data}, 200  
       
```

Se añaden los end_points
```python
api.add_resource(Requerimiento1, '/Requerimiento1')   
api.add_resource(Requerimiento2, '/Requerimiento2')   
api.add_resource(Insertbatch, '/Insertbatch')  

 
    
```
```python
if __name__ == '__main__':
    app.run(debug=True)  
```
