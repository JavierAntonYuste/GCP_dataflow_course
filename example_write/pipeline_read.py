import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import argparse
import pandas as pd
import pandasql as psql

class TransformWithSQL(beam.DoFn):
    def process(self, element):
        # Crear un DataFrame de pandas a partir del elemento (asumo que es un diccionario)
        df = pd.DataFrame([element])

        # Definir la consulta SQL para la transformación
        query = """
        SELECT 
            customer_id, 
            email 
        FROM df
        """

        # Ejecutar la consulta SQL
        transformed_df = psql.sqldf(query, locals())

        # Convertir el DataFrame transformado de vuelta a un diccionario
        transformed_element = transformed_df.to_dict(orient='records')[0]

        yield transformed_element

def run(argv=None):
    parser = argparse.ArgumentParser()
    
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Input file to process.')
    
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output BigQuery table to write results to.')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Definir opciones del pipeline
    options = PipelineOptions(pipeline_args)
    #google_cloud_options = options.view_as(GoogleCloudOptions)
    options.view_as(PipelineOptions).save_main_session = True

    # Definir el pipeline
    with beam.Pipeline(options=options) as p:
        rows = (
            p
            | 'ReadFromGCS' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
            | 'ParseCSV' >> beam.Map(lambda line: dict(zip(['customer_id', 'email'], line.split(','))))
            | 'TransformWithSQL' >> beam.ParDo(TransformWithSQL())
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                known_args.output,
                schema='SCHEMA_AUTODETECT',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    import sys
    run(sys.argv)
