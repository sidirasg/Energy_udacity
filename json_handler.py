import avro.schema
from avro.datafile import DataFileReader,DataFileWriter
from avro.io import  DatumWriter,DatumReader


#schema=avro.schema.Ptiarse(open("json _schema1.avsc").read())
schema=avro.schema.parse(open("json _schema1.avsc").read())
#schema = avro.schema.parse(open("user.avsc", "rb").read())


writer = DataFileWriter(open("json _schema1.avsc", "wb"), DatumWriter(), schema)

writer.append({"export_date":"03/12/2022","session_id":35,"user": 40, "answers_amount": 7, "test": 9})
writer.close()

reader = DataFileReader(open("json _schema1.avsc", "rb"), DatumReader())


from avro_validator.schema import Schema

schema_file = 'json _schema1.avsc'
schema2= Schema(schema_file)


parsed_schema = schema.parse()

#data for validation
data_v={"export_date":"03/12/2022","session_id":35,"user": 40, "answers_amount": 7, "test": 9}

