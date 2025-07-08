#!/usr/bin/env python3
"""
Automated RDS to On-Premise Database Migration Script
"""

import os
import sys
import logging
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sshtunnel import SSHTunnelForwarder
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DatabaseMigrator:
    def __init__(self):
        # SSH Tunnel Configuration
        self.ssh_config = {
            'ssh_host': os.getenv('SSH_HOST', '3.108.72.195'),
            'ssh_port': int(os.getenv('SSH_PORT', 22)),
            'ssh_username': os.getenv('SSH_USERNAME', 'ubuntu'),
            'ssh_key_path': os.getenv('SSH_KEY_PATH', '/app/keys/PCCMkey.pem')
        }
        
        # RDS Configuration
        self.rds_config = {
            'host': os.getenv('RDS_HOST', 'pccmdb.cfo6ceeaqrdk.ap-south-1.rds.amazonaws.com'),
            'port': int(os.getenv('RDS_PORT', 5432)),
            'database': os.getenv('RDS_DATABASE', 'PCCM_database1'),
            'user': os.getenv('RDS_USER', 'elixir'),
            'password': os.getenv('RDS_PASSWORD', 'Elixir0920')
        }
        
        # On-Premise Database Configuration
        self.onprem_config = {
            'host': os.getenv('ONPREM_HOST', '192.168.1.7'),
            'port': int(os.getenv('ONPREM_PORT', 5432)),
            'database': os.getenv('ONPREM_DATABASE', 'PCCM_Pilot_1'),
            'user': os.getenv('ONPREM_USER', 'postgres'),
            'password': os.getenv('ONPREM_PASSWORD', 'admin')
        }
        
        # Migration Configuration
        self.batch_size = int(os.getenv('BATCH_SIZE', 10000))
        self.tables_to_migrate = os.getenv('TABLES_TO_MIGRATE', 'pccm_website_patientbasicinfo').split(',')
        
        self.tunnel = None
        self.rds_engine = None
        self.onprem_engine = None

    def setup_ssh_tunnel(self):
        """Setup SSH tunnel to RDS"""
        try:
            logger.info("Setting up SSH tunnel...")
            self.tunnel = SSHTunnelForwarder(
                (self.ssh_config['ssh_host'], self.ssh_config['ssh_port']),
                ssh_username=self.ssh_config['ssh_username'],
                ssh_pkey=self.ssh_config['ssh_key_path'],
                remote_bind_address=(self.rds_config['host'], self.rds_config['port']),
                local_bind_address=('localhost', 5434)
            )
            self.tunnel.start()
            logger.info(f"SSH tunnel established on local port {self.tunnel.local_bind_port}")
            time.sleep(2)  # Give tunnel time to establish
            return True
        except Exception as e:
            logger.error(f"Failed to setup SSH tunnel: {e}")
            return False

    def create_database_connections(self):
        """Create database connections"""
        try:
            # RDS connection through tunnel
            rds_url = f"postgresql+psycopg2://{self.rds_config['user']}:{self.rds_config['password']}@localhost:{self.tunnel.local_bind_port}/{self.rds_config['database']}"
            self.rds_engine = create_engine(rds_url, pool_pre_ping=True)
            
            # On-premise connection
            onprem_url = f"postgresql+psycopg2://{self.onprem_config['user']}:{self.onprem_config['password']}@{self.onprem_config['host']}:{self.onprem_config['port']}/{self.onprem_config['database']}"
            self.onprem_engine = create_engine(onprem_url, pool_pre_ping=True)
            
            # Test connections
            with self.rds_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("RDS connection successful")
            
            with self.onprem_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("On-premise database connection successful")
            
            return True
        except Exception as e:
            logger.error(f"Failed to create database connections: {e}")
            return False

    def get_table_schema(self, table_name):
        """Get table schema from RDS"""
        try:
            inspector = inspect(self.rds_engine)
            columns = inspector.get_columns(table_name)
            return columns
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {e}")
            return None

    def migrate_table(self, table_name):
        """Migrate a single table"""
        try:
            logger.info(f"Starting migration for table: {table_name}")
            
            # Get total count
            with self.rds_engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                total_rows = result.scalar()
            
            logger.info(f"Total rows to migrate: {total_rows}")
            
            if total_rows == 0:
                logger.warning(f"Table {table_name} is empty, skipping...")
                return True
            
            # Migrate in batches
            migrated_rows = 0
            offset = 0
            
            while offset < total_rows:
                try:
                    # Read batch from RDS
                    query = f"SELECT * FROM {table_name} LIMIT {self.batch_size} OFFSET {offset}"
                    df = pd.read_sql(query, self.rds_engine)
                    
                    if df.empty:
                        break
                    
                    # Write batch to on-premise database
                    df.to_sql(
                        table_name, 
                        self.onprem_engine, 
                        if_exists='append', 
                        index=False,
                        method='multi'
                    )
                    
                    migrated_rows += len(df)
                    offset += self.batch_size
                    
                    progress = (migrated_rows / total_rows) * 100
                    logger.info(f"Progress: {migrated_rows}/{total_rows} ({progress:.2f}%)")
                    
                except Exception as e:
                    logger.error(f"Error migrating batch starting at offset {offset}: {e}")
                    # Continue with next batch
                    offset += self.batch_size
                    continue
            
            logger.info(f"Successfully migrated {migrated_rows} rows for table {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to migrate table {table_name}: {e}")
            return False

    def verify_migration(self, table_name):
        """Verify migration by comparing row counts"""
        try:
            # Count rows in source
            with self.rds_engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                source_count = result.scalar()
            
            # Count rows in destination
            with self.onprem_engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                dest_count = result.scalar()
            
            logger.info(f"Verification for {table_name}: Source={source_count}, Destination={dest_count}")
            
            if source_count == dest_count:
                logger.info(f"✓ Migration verified successfully for {table_name}")
                return True
            else:
                logger.warning(f"✗ Row count mismatch for {table_name}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to verify migration for {table_name}: {e}")
            return False

    def cleanup(self):
        """Cleanup connections and tunnel"""
        try:
            if self.rds_engine:
                self.rds_engine.dispose()
            if self.onprem_engine:
                self.onprem_engine.dispose()
            if self.tunnel:
                self.tunnel.stop()
            logger.info("Cleanup completed")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

    def run_migration(self):
        """Run the complete migration process"""
        try:
            logger.info("Starting database migration process...")
            
            # Setup SSH tunnel
            if not self.setup_ssh_tunnel():
                return False
            
            # Create database connections
            if not self.create_database_connections():
                return False
            
            # Migrate each table
            successful_tables = []
            failed_tables = []
            
            for table_name in self.tables_to_migrate:
                table_name = table_name.strip()
                logger.info(f"Processing table: {table_name}")
                
                if self.migrate_table(table_name):
                    if self.verify_migration(table_name):
                        successful_tables.append(table_name)
                    else:
                        failed_tables.append(table_name)
                else:
                    failed_tables.append(table_name)
            
            # Summary
            logger.info("Migration Summary:")
            logger.info(f"Successful tables: {successful_tables}")
            if failed_tables:
                logger.error(f"Failed tables: {failed_tables}")
            
            return len(failed_tables) == 0
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            return False
        finally:
            self.cleanup()

if __name__ == "__main__":
    migrator = DatabaseMigrator()
    success = migrator.run_migration()
    sys.exit(0 if success else 1)