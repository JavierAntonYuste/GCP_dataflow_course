import apache_beam as beam

def run():
    # Definir el pipeline
    with beam.Pipeline() as p:
        (p
         | 'Create' >> beam.Create(['Hello, World!'])
         | 'Print' >> beam.Map(print))

if __name__ == '__main__':
    run()
