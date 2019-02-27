from google.cloud.spanner import Client
from google.cloud.spanner_v1.session import Session
from faker import Faker

PROJECT_NAME = 'grass-clump-479'
INSTANCE_NAME = 'python-write'
DATABASE_NAME = 'pythontest'

client = Client(project=PROJECT_NAME)
instance = client.instance(INSTANCE_NAME)
database = instance.database(DATABASE_NAME)

session = Session(database)
session.create()
transaction = session.transaction()
transaction.begin()
fake = Faker()
for i in range(10000):
    transaction.insert(
        table='table_python',
        columns=(
            'key', 'textcolumn',),
        values=[
            (i, fake.name()),])
transaction.commit()

with database.snapshot() as snapshot:
    results = snapshot.execute_sql('SELECT key,textcolumn from table_python')
    for row in results:
        print(row)