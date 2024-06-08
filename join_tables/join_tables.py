import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import pandas as pd
import pandasql as psql
import argparse

class JoinTables(beam.DoFn):
    def process(self, element, join_data):
        # Convertir el elemento y los datos de unión a DataFrames
        df = pd.DataFrame([element])
        join_df = pd.DataFrame(join_data)

        # Definir la consulta SQL para la unión
        query = """
        SELECT 
            a.customer_id, 
            a.email, 
            b.order_id, 
            b.amount 
        FROM df as a
        JOIN join_df as b
        ON a.customer_id = b.customer_id
        """

        # Ejecutar la consulta SQL
        result_df = psql.sqldf(query, locals())

        # Convertir el DataFrame resultante a un diccionario y emitir
        for _, row in result_df.iterrows():
            yield row.to_dict()

def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input_customers',
        dest='input_customers',
        required=True,
        help='Input file with customer data')

    parser.add_argument(
        '--input_orders',
        dest='input_orders',
        required=True,
        help='Input file with order data')

    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output BigQuery table to write results to')

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Leer datos de unión (orders) de GCS
    join_data = []
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        orders = (
            p
            | 'ReadOrders' >> beam.io.ReadFromText(known_args.input_orders, skip_header_lines=1)
            | 'ParseOrders' >> beam.Map(lambda line: dict(zip(['order_id', 'customer_id', 'amount'], line.split(','))))
            | 'CollectOrders' >> beam.CombineGlobally(beam.combiners.ToListCombineFn())
        )
        join_data = p.run().wait_until_finish()

    # Definir el pipeline
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        customers = (
            p
            | 'ReadCustomers' >> beam.io.ReadFromText(known_args.input_customers, skip_header_lines=1)
            | 'ParseCustomers' >> beam.Map(lambda line: dict(zip(['customer_id', 'email'], line.split(','))))
            | 'JoinTables' >> beam.ParDo(JoinTables(), join_data=join_data)
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                known_args.output,
                schema='SCHEMA_AUTODETECT',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    import sys
    run(sys.argv)
