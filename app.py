import boto3
import psycopg2
from preprocessing import *

access_key = 'YOUR_ACCESS_KEY'
secret_key = 'YOUR_SECRET_KEY'
region = 'us-west-2'

session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region
)

redshift = session.client('redshift')

cluster_identifier = 'my-redshift-cluster'
database_name = 'my_database'
master_username = 'master_user'
master_password = 'mypassword'

try:
    # Create Amazon Redshift cluster
    response = redshift.create_cluster(
        ClusterIdentifier=cluster_identifier,
        DBName=database_name,
        MasterUsername=master_username,
        MasterUserPassword=master_password,
        NodeType='dc2.large',
        NumberOfNodes=2,
        PubliclyAccessible=True
    )

    # Wait for the cluster to be available
    redshift.get_waiter('cluster_available').wait(ClusterIdentifier=cluster_identifier)

    print('Amazon Redshift cluster created.')

    # Connect to the cluster
    conn = psycopg2.connect(
        host=redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]['Endpoint']['Address'],
        dbname=database_name,
        user=master_username,
        password=master_password,
        port=5439
    )

    # Perform ETL processes and data transformations
    # Execute optimized queries for query performance
    perform_etl(conn)

    # Execute optimized queries for query performance
    execute_optimized_queries(conn)

    print('ETL processes and query optimization completed.')

finally:
    # Delete Amazon Redshift cluster (optional)
    redshift.delete_cluster(ClusterIdentifier=cluster_identifier, SkipFinalClusterSnapshot=True)

    # Wait for the cluster to be deleted
    redshift.get_waiter('cluster_deleted').wait(ClusterIdentifier=cluster_identifier)

    print('Amazon Redshift cluster deleted.')
