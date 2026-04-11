# -*- coding: utf-8 -*-
"""
Migration script from PostgreSQL/S3 to Supabase using REST API
Handles data migration and validation

@author: trent
"""

import pandas as pd
import logging
from datetime import datetime
from supabase_rest_api import get_supabase_rest
from supabase_storage import supabase_storage, migrate_s3_to_supabase
import os
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SupabaseMigration:
    """Handles migration from PostgreSQL/S3 to Supabase using REST API"""
    
    def __init__(self):
        self.migration_stats = {
            'tables_migrated': 0,
            'files_migrated': 0,
            'errors': [],
            'warnings': []
        }
        # Initialize REST API connection
        self.supabase_rest = get_supabase_rest()
        logger.info("✅ Supabase REST API connection initialized")
    
    def migrate_database_tables(self):
        """Migrate PostgreSQL tables to Supabase using REST API"""
        logger.info("🔄 Starting database table migration via REST API...")
        
        # List of tables to migrate
        tables_to_migrate = [
            'PLAYER_GAME_LOGS',
            'injuries', 
            'TEAMS',
            'PLAYERS',
            'Games',
            'wowy',
            'pace',
            'underdog',
            'draftkings',
            'prizepicks',
            'betmgm',
            'caesars',
            'projmins',
            'todaysmins'
        ]
        
        try:
            # Get legacy PostgreSQL connection
            from db_conn import get_db_engine
            legacy_engine = get_db_engine()
            
            for table in tables_to_migrate:
                try:
                    logger.info(f"📊 Migrating table: {table}")
                    
                    # Read from legacy PostgreSQL
                    query = f"SELECT * FROM wnba.{table}"
                    df = pd.read_sql(query, legacy_engine)
                    
                    if not df.empty:
                        # Upload to Supabase using REST API
                        success = self.supabase_rest.upload_dataframe(
                            df, table, schema='wnba', if_exists='replace'
                        )
                        
                        if success:
                            self.migration_stats['tables_migrated'] += 1
                            logger.info(f"✅ Successfully migrated {table} ({len(df)} rows) via REST API")
                        else:
                            self.migration_stats['errors'].append(f"Failed to upload {table}")
                            logger.error(f"❌ Failed to migrate {table}")
                    else:
                        self.migration_stats['warnings'].append(f"Table {table} is empty")
                        logger.warning(f"⚠️ Table {table} is empty")
                        
                except Exception as e:
                    self.migration_stats['errors'].append(f"Error migrating {table}: {str(e)}")
                    logger.error(f"❌ Error migrating {table}: {e}")
                    
        except Exception as e:
            logger.error(f"❌ Database migration failed: {e}")
            self.migration_stats['errors'].append(f"Database migration failed: {str(e)}")
    
    def migrate_storage_files(self):
        """Migrate S3 files to Supabase Storage"""
        logger.info("🔄 Starting storage file migration...")
        
        try:
            # Migrate S3 to Supabase Storage
            migrate_s3_to_supabase()
            self.migration_stats['files_migrated'] += 1
            logger.info("✅ Storage migration completed")
            
        except Exception as e:
            logger.error(f"❌ Storage migration failed: {e}")
            self.migration_stats['errors'].append(f"Storage migration failed: {str(e)}")
    
    def validate_migration(self):
        """Validate that migration was successful using REST API"""
        logger.info("🔍 Validating migration via REST API...")
        
        validation_results = {
            'database_tables': {},
            'storage_files': {},
            'overall_success': True
        }
        
        # Validate database tables using REST API
        tables_to_validate = [
            'PLAYER_GAME_LOGS',
            'injuries',
            'TEAMS',
            'PLAYERS',
            'Games'
        ]
        
        for table in tables_to_validate:
            try:
                df = self.supabase_rest.get_table_data(table, schema='wnba', limit=10)
                if df is not None and not df.empty:
                    validation_results['database_tables'][table] = {
                        'status': 'success',
                        'row_count': len(df)
                    }
                    logger.info(f"✅ Table {table} validation passed via REST API ({len(df)} sample rows)")
                else:
                    validation_results['database_tables'][table] = {
                        'status': 'failed',
                        'error': 'No data found'
                    }
                    validation_results['overall_success'] = False
                    logger.error(f"❌ Table {table} validation failed")
                    
            except Exception as e:
                validation_results['database_tables'][table] = {
                    'status': 'failed',
                    'error': str(e)
                }
                validation_results['overall_success'] = False
                logger.error(f"❌ Table {table} validation error: {e}")
        
        # Validate storage files
        try:
            files = supabase_storage.list_files('wnba-data')
            validation_results['storage_files'] = {
                'status': 'success',
                'file_count': len(files)
            }
            logger.info(f"✅ Storage validation passed ({len(files)} files)")
            
        except Exception as e:
            validation_results['storage_files'] = {
                'status': 'failed',
                'error': str(e)
            }
            validation_results['overall_success'] = False
            logger.error(f"❌ Storage validation failed: {e}")
        
        return validation_results
    
    def generate_migration_report(self):
        """Generate comprehensive migration report"""
        logger.info("📊 Generating migration report...")
        
        report = {
            'migration_date': datetime.now().isoformat(),
            'statistics': self.migration_stats,
            'validation': self.validate_migration(),
            'recommendations': [],
            'migration_method': 'REST API'
        }
        
        # Add recommendations based on results
        if self.migration_stats['errors']:
            report['recommendations'].append("Review and fix migration errors before proceeding")
        
        if self.migration_stats['warnings']:
            report['recommendations'].append("Address warnings to ensure data integrity")
        
        if report['validation']['overall_success']:
            report['recommendations'].append("Migration successful - can proceed with Supabase deployment")
        else:
            report['recommendations'].append("Migration validation failed - review before proceeding")
        
        # Save report
        report_file = f"migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        supabase_storage.upload_json(report, 'migration-reports', report_file, public=False)
        
        logger.info(f"📄 Migration report saved: {report_file}")
        return report
    
    def run_full_migration(self):
        """Run complete migration process using REST API"""
        logger.info("🚀 Starting full migration to Supabase via REST API...")
        
        # Step 1: Migrate database tables
        self.migrate_database_tables()
        
        # Step 2: Migrate storage files
        self.migrate_storage_files()
        
        # Step 3: Validate migration
        validation = self.validate_migration()
        
        # Step 4: Generate report
        report = self.generate_migration_report()
        
        # Step 5: Print summary
        self.print_migration_summary(report)
        
        return report
    
    def print_migration_summary(self, report):
        """Print migration summary to console"""
        print("\n" + "="*60)
        print("📊 SUPABASE MIGRATION SUMMARY (REST API)")
        print("="*60)
        
        print(f"📅 Migration Date: {report['migration_date']}")
        print(f"🔗 Migration Method: {report['migration_method']}")
        print(f"📊 Tables Migrated: {report['statistics']['tables_migrated']}")
        print(f"📁 Files Migrated: {report['statistics']['files_migrated']}")
        print(f"❌ Errors: {len(report['statistics']['errors'])}")
        print(f"⚠️ Warnings: {len(report['statistics']['warnings'])}")
        
        print(f"\n🔍 Validation Results:")
        print(f"   Database Tables: {sum(1 for t in report['validation']['database_tables'].values() if t['status'] == 'success')}/{len(report['validation']['database_tables'])} passed")
        print(f"   Storage Files: {'✅ Passed' if report['validation']['storage_files']['status'] == 'success' else '❌ Failed'}")
        
        print(f"\n📋 Overall Status: {'✅ SUCCESS' if report['validation']['overall_success'] else '❌ FAILED'}")
        
        if report['recommendations']:
            print(f"\n💡 Recommendations:")
            for rec in report['recommendations']:
                print(f"   • {rec}")
        
        print("="*60)

def main():
    """Main migration execution"""
    migration = SupabaseMigration()
    report = migration.run_full_migration()
    
    if report['validation']['overall_success']:
        print("\n🎉 Migration completed successfully!")
        print("You can now update your applications to use Supabase.")
    else:
        print("\n⚠️ Migration completed with issues.")
        print("Please review the report and fix any errors before proceeding.")

if __name__ == "__main__":
    main()
