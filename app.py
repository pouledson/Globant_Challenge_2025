from flask import Flask,request,jsonify,make_response
from flask_restful import Resource,Api, reqparse
from google.cloud import bigquery,storage
from google.oauth2 import service_account
import io
from datetime import date
import fastavro
import credentials
import logging
logging.basicConfig(filename='app.log', level=logging.DEBUG)
credentials = service_account.Credentials.from_service_account_file(
    credentials.path_to_service_account_key_file, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
 
client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

storage_client = storage.Client(credentials=credentials)


app = Flask(__name__)
api = Api(app)

class Insertbatch(Resource):
    
    def archivo_permitido(self, filename):
        ALLOWED_EXTENSIONS = {'csv'}
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
    def post(self):
        parser = reqparse.RequestParser()          
        parser.add_argument('table',type=str, required=True,location='args')
        args = parser.parse_args()  

        table_id="proyectoglobant2905.proyecto."+str(args['table'])
        if 'file' not in request.files:
            return 'Archivo no encontrado'
        
        file = request.files.get("file")
        if file and self.archivo_permitido(file.filename):
            
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV, skip_leading_rows=0, autodetect=False,field_delimiter=",",
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )

            job = client.load_table_from_file(file.stream, table_id, job_config=job_config)
                
            job.result()  
             
            client.get_table(table_id)  
             
            mensaje=    "Se insertaron los valores correctamente en la tabla  {}".format(
                     table_id
                )
             
            

        return mensaje    

class Requerimiento1(Resource):

    
    def get(self):
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
        job_result = query_job.to_dataframe()  
        data = job_result.to_dict(orient='records')  
        return make_response(jsonify(data), 200)  
        
    
class Requerimiento2(Resource):

    
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
        job_result = query_job.to_dataframe() 
        data = job_result.to_dict(orient='records')  
        return make_response(jsonify(data), 200)  
       
class Backup(Resource):
    def schemas_table(self,name):
        logging.info("MIRA esto")
        logging.info(name)

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
        elif name=="hired_employees":
            schema = {
                "namespace": "com.schema.data",
                "type": "record",
                "name": name    ,
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"},
                    {"name": "datetime", "type": "string"},
                    {"name": "datetime_id", "type": "int"},
                    {"name": "job_id", "type": "int"}

              
                ]
            }
        elif name=="jobs":
            schema = {
                "namespace": "com.schema.data",
                "type": "record",
                "name": name    ,
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "job", "type": "string"}
              
                ]
            }
        else:
            schema=None
        return schema
    
    
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
        
        bytes_writer = io.BytesIO()
        
        schema=self.schemas_table(str(args['table']))
        records = []
        for row in results:
            record = {field['name']: row[field['name']] for field in schema['fields']}
            records.append(record)
        logging.info(records)
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

class Restore(Resource):
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


api.add_resource(Requerimiento1, '/Requerimiento1')   
api.add_resource(Requerimiento2, '/Requerimiento2')   
api.add_resource(Insertbatch, '/Insertbatch')  
api.add_resource(Backup, '/Backup')  
api.add_resource(Restore, '/Restore')  
    


if __name__ == '__main__':
    app.run(debug=True)  