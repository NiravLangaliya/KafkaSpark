from pyhocon import ConfigFactory
import textwrap
from databuilder.extractor.hive_table_metadata_extractor import HiveTableMetadataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.models.table_metadata import DESCRIPTION_NODE_LABEL
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.job.job import DefaultJob
from databuilder.task.task import DefaultTask
#### Constants
# Neo4j cluster endpoint
NEO4J_ENDPOINT = 'bolt://localhost:7687'
neo4j_endpoint = NEO4J_ENDPOINT
neo4j_user = 'neo4j'
neo4j_password = 'test'
# Hive Config
hive_metastore_connection_string = 'mysql+pymysql://hiveuser:123@localhost/metastore'
SUPPORTED_HIVE_SCHEMAS = ['hive']
SUPPORTED_HIVE_SCHEMA_SQL_IN_CLAUSE = "('{schemas}')".format(schemas="', '".join(SUPPORTED_HIVE_SCHEMAS))
def create_table_metadata_databuilder_job():
    where_clause_suffix = textwrap.dedent("""
        WHERE d.NAME IN {schemas}
        AND t.TBL_NAME NOT REGEXP '^[0-9]+'
        AND t.TBL_TYPE IN ( 'EXTERNAL_TABLE', 'MANAGED_TABLE' )
    """).format(schemas=SUPPORTED_HIVE_SCHEMA_SQL_IN_CLAUSE)
    tmp_folder = '/var/tmp/amundsen/table_metadata'
    node_files_folder = '{tmp_folder}/nodes/'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships/'.format(tmp_folder=tmp_folder)
    job_config = ConfigFactory.from_dict({
        'extractor.hive_table_metadata.{}'.format(HiveTableMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY):
        where_clause_suffix,
        'extractor.hive_table_metadata.extractor.sqlalchemy.{}'.format(SQLAlchemyExtractor.CONN_STRING):
        hive_metastore_connection_string,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.NODE_DIR_PATH):
            node_files_folder,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.RELATION_DIR_PATH):
            relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NODE_FILES_DIR):
            node_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.RELATION_FILES_DIR):
            relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_END_POINT_KEY):
            neo4j_endpoint,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_USER):
            neo4j_user,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_PASSWORD):
            neo4j_password,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_CREATE_ONLY_NODES):
            [DESCRIPTION_NODE_LABEL],
        'publisher.neo4j.job_publish_tag':'hive_metadata'
    })
    job = DefaultJob(conf=job_config,
                    task=DefaultTask(extractor=HiveTableMetadataExtractor(), loader=FsNeo4jCSVLoader()),
                    publisher=Neo4jCsvPublisher())
    job.launch()
if __name__ == "__main__":
    create_table_metadata_databuilder_job()
