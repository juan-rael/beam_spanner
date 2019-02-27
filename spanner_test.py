from google.cloud.spanner import Client
from google.cloud.spanner_v1.batch import Batch
from faker import Faker

PROJECT_NAME = 'grass-clump-479'
INSTANCE_NAME = 'python-write'
DATABASE_NAME = 'pythontest'

client = Client(project=PROJECT_NAME)
instance = client.instance(INSTANCE_NAME)
database = instance.database(DATABASE_NAME)

def update_albums(transaction):
    fake = Faker()
    for i in range(10):
        transaction.insert(
            table='table_python',
            columns=(
                'key', 'textcolumn',),
            values=[
                (i, fake.name()),])
    transaction.commit()

database.run_in_transaction(update_albums)