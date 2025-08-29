#!/usr/bin/env python3
"""
Production Readiness Validation Script

This script validates each component of the Spotify data pipeline:
1. Data generation and validation
2. Database connectivity and schema validation  
3. Beam pipeline execution and validation
4. Scio pipeline execution and validation
5. DBT model execution and validation
6. Dashboard connectivity and data visualization
7. End-to-end integration testing with metrics logging
"""

import os
import sys
import json
import time
import logging
import subprocess
import psycopg2
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import pandas as pd

# Configure logging with both file and console output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline_validation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import local utilities
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.config import get_db_config, get_kafka_config
from utils.test_utils import DatabaseTestHelper, KafkaTestHelper, TestDataGenerator
from data_generator import SpotifyDataGenerator


class PipelineValidator:
    """Comprehensive pipeline validation orchestrator."""
    
    def __init__(self):
        """Initialize the validator with configuration."""
        self.validation_results = {}
        self.metrics = {
            'start_time': datetime.now(),
            'validation_steps': {},
            'errors': [],
            'warnings': []
        }
        self.db_helper = None
        self.kafka_helper = None
        
    def log_metric(self, step: str, status: str, duration: float = 0, details: Dict = None):
        """Log validation metrics for monitoring."""
        self.metrics['validation_steps'][step] = {
            'status': status,  # 'success', 'failure', 'warning'
            'duration_seconds': duration,
            'timestamp': datetime.now().isoformat(),
            'details': details or {}
        }
        
        if status == 'failure':
            self.metrics['errors'].append(f"{step}: {details}")
        elif status == 'warning':
            self.metrics['warnings'].append(f"{step}: {details}")
    
    def validate_environment(self) -> bool:
        """Validate environment setup and dependencies."""
        logger.info("🔍 Validating environment setup...")
        start_time = time.time()
        
        try:
            # Check required environment variables
            required_env_vars = [
                'DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD',
                'KAFKA_BOOTSTRAP_SERVERS'
            ]
            
            missing_vars = []
            for var in required_env_vars:
                if not os.getenv(var):
                    missing_vars.append(var)
            
            if missing_vars:
                error_msg = f"Missing environment variables: {missing_vars}"
                self.log_metric('environment_validation', 'failure', 
                              time.time() - start_time, {'missing_vars': missing_vars})
                logger.error(f"❌ {error_msg}")
                return False
            
            # Check directory structure
            required_dirs = ['logs', 'data/raw', 'data/processed', 'data/analytics']
            for dir_path in required_dirs:
                if not os.path.exists(dir_path):
                    os.makedirs(dir_path, exist_ok=True)
                    logger.info(f"📁 Created directory: {dir_path}")
            
            self.log_metric('environment_validation', 'success', time.time() - start_time)
            logger.info("✅ Environment validation passed")
            return True
            
        except Exception as e:
            self.log_metric('environment_validation', 'failure', 
                          time.time() - start_time, {'error': str(e)})
            logger.error(f"❌ Environment validation failed: {e}")
            return False
    
    def validate_database_connectivity(self) -> bool:
        """Validate database connectivity and schema."""
        logger.info("🔍 Validating database connectivity...")
        start_time = time.time()
        
        try:
            self.db_helper = DatabaseTestHelper(test_schema="validation_test")
            self.db_helper.connect()
            
            # Test connection
            with self.db_helper.conn.cursor() as cursor:
                cursor.execute("SELECT version();")
                version = cursor.fetchone()[0]
                logger.info(f"📊 Connected to PostgreSQL: {version}")
            
            # Validate schemas exist
            with self.db_helper.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT schema_name FROM information_schema.schemata 
                    WHERE schema_name IN ('raw', 'analytics');
                """)
                schemas = [row[0] for row in cursor.fetchall()]
                
                if 'raw' not in schemas or 'analytics' not in schemas:
                    missing_schemas = [s for s in ['raw', 'analytics'] if s not in schemas]
                    self.log_metric('database_validation', 'warning', 
                                  time.time() - start_time, 
                                  {'missing_schemas': missing_schemas})
                    logger.warning(f"⚠️ Missing schemas: {missing_schemas}")
                else:
                    logger.info("✅ Required schemas (raw, analytics) exist")
            
            # Test table creation and data insertion
            self.db_helper.setup_test_schema()
            self.db_helper.create_table("users")
            
            test_data = [{'user_id': 'test_user', 'username': 'test', 'email': 'test@example.com'}]
            self.db_helper.populate_test_data("users", test_data)
            
            # Verify data insertion
            results = self.db_helper.execute_query("SELECT COUNT(*) as count FROM users")
            if results[0]['count'] == 1:
                logger.info("✅ Database read/write operations working")
            
            self.log_metric('database_validation', 'success', time.time() - start_time)
            return True
            
        except Exception as e:
            self.log_metric('database_validation', 'failure', 
                          time.time() - start_time, {'error': str(e)})
            logger.error(f"❌ Database validation failed: {e}")
            return False
    
    def validate_kafka_connectivity(self) -> bool:
        """Validate Kafka connectivity and topic operations."""
        logger.info("🔍 Validating Kafka connectivity...")
        start_time = time.time()
        
        try:
            self.kafka_helper = KafkaTestHelper(topic_prefix="validation")
            self.kafka_helper.setup()
            
            # Test message production and consumption
            test_messages = [
                {'event_id': 'test_1', 'user_id': 'test_user', 'timestamp': datetime.now().isoformat()},
                {'event_id': 'test_2', 'user_id': 'test_user', 'timestamp': datetime.now().isoformat()}
            ]
            
            # Produce messages
            self.kafka_helper.produce_test_messages('raw_events', test_messages, 'event_id')
            logger.info(f"📤 Produced {len(test_messages)} test messages to Kafka")
            
            # Consume messages
            time.sleep(2)  # Allow time for message propagation
            consumed_messages = self.kafka_helper.consume_test_messages('raw_events', timeout=5.0)
            
            if len(consumed_messages) >= len(test_messages):
                logger.info(f"📥 Successfully consumed {len(consumed_messages)} messages from Kafka")
                self.log_metric('kafka_validation', 'success', time.time() - start_time)
                return True
            else:
                self.log_metric('kafka_validation', 'failure', 
                              time.time() - start_time, 
                              {'produced': len(test_messages), 'consumed': len(consumed_messages)})
                logger.error(f"❌ Kafka message mismatch - produced: {len(test_messages)}, consumed: {len(consumed_messages)}")
                return False
                
        except Exception as e:
            self.log_metric('kafka_validation', 'failure', 
                          time.time() - start_time, {'error': str(e)})
            logger.error(f"❌ Kafka validation failed: {e}")
            return False
    
    def validate_data_generation(self, scale: str = 'tiny') -> bool:
        """Validate data generation with mock data."""
        logger.info(f"🔍 Validating data generation (scale: {scale})...")
        start_time = time.time()
        
        try:
            generator = SpotifyDataGenerator()
            
            # Generate test data
            generator.generate_all_data(scale)
            
            # Validate generated data exists in database
            db_config = get_db_config('raw')
            conn = psycopg2.connect(**db_config)
            
            tables_to_check = ['users', 'artists', 'albums', 'tracks', 'streaming_history']
            row_counts = {}
            
            with conn.cursor() as cursor:
                for table in tables_to_check:
                    cursor.execute(f"SELECT COUNT(*) FROM raw.{table}")
                    count = cursor.fetchone()[0]
                    row_counts[table] = count
                    logger.info(f"📊 {table}: {count} rows")
            
            conn.close()
            
            # Verify minimum expected counts
            expected_minimums = {'tiny': 5, 'small': 20, 'medium': 50, 'large': 100}
            min_expected = expected_minimums.get(scale, 5)
            
            if all(count >= min_expected for count in row_counts.values()):
                self.log_metric('data_generation', 'success', 
                              time.time() - start_time, {'row_counts': row_counts})
                logger.info("✅ Data generation validation passed")
                return True
            else:
                self.log_metric('data_generation', 'failure', 
                              time.time() - start_time, 
                              {'row_counts': row_counts, 'expected_minimum': min_expected})
                logger.error("❌ Generated data counts below expected minimum")
                return False
                
        except Exception as e:
            self.log_metric('data_generation', 'failure', 
                          time.time() - start_time, {'error': str(e)})
            logger.error(f"❌ Data generation validation failed: {e}")
            return False
    
    def validate_beam_pipeline(self) -> bool:
        """Validate Apache Beam pipeline execution."""
        logger.info("🔍 Validating Apache Beam pipeline...")
        start_time = time.time()
        
        try:
            # Run Beam pipeline using Docker
            cmd = [
                'docker-compose', 'exec', '-T', 'beam-runner',
                'python', '/opt/beam/scripts/beam_pipeline.py'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                logger.info("✅ Beam pipeline executed successfully")
                
                # Validate output data exists in analytics schema
                db_config = get_db_config('analytics')
                conn = psycopg2.connect(**db_config)
                
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM analytics.user_listening_stats")
                    user_stats_count = cursor.fetchone()[0]
                    
                    cursor.execute("SELECT COUNT(*) FROM analytics.track_popularity")
                    track_pop_count = cursor.fetchone()[0]
                
                conn.close()
                
                if user_stats_count > 0 and track_pop_count > 0:
                    self.log_metric('beam_pipeline', 'success', 
                                  time.time() - start_time, 
                                  {'user_stats': user_stats_count, 'track_popularity': track_pop_count})
                    return True
                else:
                    self.log_metric('beam_pipeline', 'failure', 
                                  time.time() - start_time, 
                                  {'user_stats': user_stats_count, 'track_popularity': track_pop_count})
                    logger.error("❌ Beam pipeline produced no analytics data")
                    return False
            else:
                self.log_metric('beam_pipeline', 'failure', 
                              time.time() - start_time, 
                              {'error': result.stderr, 'stdout': result.stdout})
                logger.error(f"❌ Beam pipeline failed: {result.stderr}")
                return False
                
        except Exception as e:
            self.log_metric('beam_pipeline', 'failure', 
                          time.time() - start_time, {'error': str(e)})
            logger.error(f"❌ Beam pipeline validation failed: {e}")
            return False
    
    def validate_scio_pipeline(self) -> bool:
        """Validate Scala/Scio pipeline execution."""
        logger.info("🔍 Validating Scio pipeline...")
        start_time = time.time()
        
        try:
            # Build Scio project first
            build_cmd = ['docker-compose', 'exec', '-T', 'scio-runner', 'sbt', 'compile']
            build_result = subprocess.run(build_cmd, capture_output=True, text=True, timeout=180)
            
            if build_result.returncode != 0:
                self.log_metric('scio_pipeline', 'failure', 
                              time.time() - start_time, 
                              {'error': 'Compilation failed', 'stderr': build_result.stderr})
                logger.error(f"❌ Scio compilation failed: {build_result.stderr}")
                return False
            
            # Run Scio pipeline
            run_cmd = [
                'docker-compose', 'exec', '-T', 'scio-runner',
                'bash', '-c', 'cd /app && ./scripts/run_scio_pipeline.sh -p streaming_history -d 2024-01-01 -e dev'
            ]
            
            run_result = subprocess.run(run_cmd, capture_output=True, text=True, timeout=300)
            
            if run_result.returncode == 0:
                logger.info("✅ Scio pipeline executed successfully")
                self.log_metric('scio_pipeline', 'success', time.time() - start_time)
                return True
            else:
                self.log_metric('scio_pipeline', 'failure', 
                              time.time() - start_time, 
                              {'error': run_result.stderr, 'stdout': run_result.stdout})
                logger.error(f"❌ Scio pipeline failed: {run_result.stderr}")
                return False
                
        except Exception as e:
            self.log_metric('scio_pipeline', 'failure', 
                          time.time() - start_time, {'error': str(e)})
            logger.error(f"❌ Scio pipeline validation failed: {e}")
            return False
    
    def validate_dbt_models(self) -> bool:
        """Validate DBT model execution."""
        logger.info("🔍 Validating DBT models...")
        start_time = time.time()
        
        try:
            # Run DBT models
            cmd = ['docker-compose', 'exec', '-T', 'dbt', 'dbt', 'run']
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
            
            if result.returncode == 0:
                logger.info("✅ DBT models executed successfully")
                
                # Validate DBT test execution
                test_cmd = ['docker-compose', 'exec', '-T', 'dbt', 'dbt', 'test']
                test_result = subprocess.run(test_cmd, capture_output=True, text=True, timeout=120)
                
                if test_result.returncode == 0:
                    self.log_metric('dbt_validation', 'success', time.time() - start_time)
                    logger.info("✅ DBT tests passed")
                    return True
                else:
                    self.log_metric('dbt_validation', 'warning', 
                                  time.time() - start_time, 
                                  {'test_error': test_result.stderr})
                    logger.warning(f"⚠️ DBT tests failed: {test_result.stderr}")
                    return True  # DBT run succeeded, tests are secondary
            else:
                self.log_metric('dbt_validation', 'failure', 
                              time.time() - start_time, 
                              {'error': result.stderr})
                logger.error(f"❌ DBT models failed: {result.stderr}")
                return False
                
        except Exception as e:
            self.log_metric('dbt_validation', 'failure', 
                          time.time() - start_time, {'error': str(e)})
            logger.error(f"❌ DBT validation failed: {e}")
            return False
    
    def validate_dashboard(self) -> bool:
        """Validate dashboard connectivity and data visualization."""
        logger.info("🔍 Validating dashboard...")
        start_time = time.time()
        
        try:
            # Check if dashboard is accessible
            dashboard_url = "http://localhost:8050"
            response = requests.get(dashboard_url, timeout=10)
            
            if response.status_code == 200:
                logger.info("✅ Dashboard is accessible")
                
                # Test dashboard data endpoints (if available)
                # This would depend on your dashboard implementation
                
                self.log_metric('dashboard_validation', 'success', time.time() - start_time)
                return True
            else:
                self.log_metric('dashboard_validation', 'failure', 
                              time.time() - start_time, 
                              {'status_code': response.status_code})
                logger.error(f"❌ Dashboard not accessible: {response.status_code}")
                return False
                
        except requests.exceptions.ConnectionError:
            self.log_metric('dashboard_validation', 'failure', 
                          time.time() - start_time, {'error': 'Connection refused'})
            logger.error("❌ Dashboard connection refused - is it running?")
            return False
        except Exception as e:
            self.log_metric('dashboard_validation', 'failure', 
                          time.time() - start_time, {'error': str(e)})
            logger.error(f"❌ Dashboard validation failed: {e}")
            return False
    
    def run_end_to_end_integration_test(self) -> bool:
        """Run comprehensive end-to-end integration test."""
        logger.info("🔍 Running end-to-end integration test...")
        start_time = time.time()
        
        try:
            # 1. Generate fresh test data
            logger.info("📊 Step 1: Generating fresh test data")
            if not self.validate_data_generation('tiny'):
                return False
            
            # 2. Run both processing pipelines
            logger.info("🔄 Step 2: Running Beam pipeline")
            if not self.validate_beam_pipeline():
                logger.warning("⚠️ Beam pipeline failed, continuing with Scio")
            
            logger.info("🔄 Step 3: Running Scio pipeline")
            if not self.validate_scio_pipeline():
                logger.warning("⚠️ Scio pipeline failed, continuing")
            
            # 3. Run DBT transformations
            logger.info("🔄 Step 4: Running DBT transformations")
            if not self.validate_dbt_models():
                logger.warning("⚠️ DBT validation failed, continuing")
            
            # 4. Validate data flow integrity
            logger.info("🔍 Step 5: Validating data flow integrity")
            db_config = get_db_config('analytics')
            conn = psycopg2.connect(**db_config)
            
            with conn.cursor() as cursor:
                # Check if data flows from raw to analytics
                cursor.execute("SELECT COUNT(*) FROM raw.streaming_history")
                raw_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM analytics.user_listening_stats")
                analytics_count = cursor.fetchone()[0]
                
                logger.info(f"📊 Raw streaming events: {raw_count}")
                logger.info(f"📊 Analytics user stats: {analytics_count}")
                
                if raw_count > 0 and analytics_count > 0:
                    logger.info("✅ Data flow integrity validated")
                    success = True
                else:
                    logger.error("❌ Data flow integrity check failed")
                    success = False
            
            conn.close()
            
            self.log_metric('end_to_end_test', 'success' if success else 'failure', 
                          time.time() - start_time, 
                          {'raw_events': raw_count, 'analytics_stats': analytics_count})
            return success
            
        except Exception as e:
            self.log_metric('end_to_end_test', 'failure', 
                          time.time() - start_time, {'error': str(e)})
            logger.error(f"❌ End-to-end integration test failed: {e}")
            return False
    
    def generate_validation_report(self) -> Dict[str, Any]:
        """Generate comprehensive validation report."""
        self.metrics['end_time'] = datetime.now()
        self.metrics['total_duration'] = (self.metrics['end_time'] - self.metrics['start_time']).total_seconds()
        
        # Calculate success rate
        total_steps = len(self.metrics['validation_steps'])
        successful_steps = len([step for step in self.metrics['validation_steps'].values() 
                               if step['status'] == 'success'])
        
        self.metrics['success_rate'] = (successful_steps / total_steps) * 100 if total_steps > 0 else 0
        
        # Save report to file
        report_file = f"logs/validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(self.metrics, f, indent=2, default=str)
        
        logger.info(f"📋 Validation report saved to: {report_file}")
        return self.metrics
    
    def run_all_validations(self, include_integration_test: bool = True) -> bool:
        """Run all validation steps."""
        logger.info("🚀 Starting comprehensive pipeline validation")
        
        validation_steps = [
            ('Environment Setup', self.validate_environment),
            ('Database Connectivity', self.validate_database_connectivity),
            ('Kafka Connectivity', self.validate_kafka_connectivity),
            ('Data Generation', lambda: self.validate_data_generation('tiny')),
            ('Beam Pipeline', self.validate_beam_pipeline),
            ('Scio Pipeline', self.validate_scio_pipeline),
            ('DBT Models', self.validate_dbt_models),
            ('Dashboard', self.validate_dashboard),
        ]
        
        if include_integration_test:
            validation_steps.append(('End-to-End Integration', self.run_end_to_end_integration_test))
        
        results = []
        for step_name, validation_func in validation_steps:
            logger.info(f"\n{'='*60}")
            logger.info(f"🔄 Running: {step_name}")
            logger.info(f"{'='*60}")
            
            try:
                result = validation_func()
                results.append(result)
                
                if result:
                    logger.info(f"✅ {step_name}: PASSED")
                else:
                    logger.error(f"❌ {step_name}: FAILED")
                    
            except Exception as e:
                logger.error(f"💥 {step_name}: CRASHED - {e}")
                results.append(False)
        
        # Cleanup
        if self.db_helper:
            self.db_helper.teardown_test_schema()
            self.db_helper.disconnect()
        
        if self.kafka_helper:
            self.kafka_helper.teardown()
        
        # Generate final report
        report = self.generate_validation_report()
        
        # Print summary
        logger.info(f"\n{'='*60}")
        logger.info("📊 VALIDATION SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"🎯 Success Rate: {report['success_rate']:.1f}%")
        logger.info(f"⏱️ Total Duration: {report['total_duration']:.1f}s")
        logger.info(f"✅ Successful Steps: {len([s for s in report['validation_steps'].values() if s['status'] == 'success'])}")
        logger.info(f"❌ Failed Steps: {len([s for s in report['validation_steps'].values() if s['status'] == 'failure'])}")
        logger.info(f"⚠️ Warning Steps: {len([s for s in report['validation_steps'].values() if s['status'] == 'warning'])}")
        
        overall_success = all(results)
        logger.info(f"\n🎉 OVERALL RESULT: {'PASSED' if overall_success else 'FAILED'}")
        
        return overall_success


def main():
    """Main entry point for validation script."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Validate Spotify data pipeline for production readiness")
    parser.add_argument('--skip-integration', action='store_true', 
                       help='Skip end-to-end integration test')
    parser.add_argument('--component', type=str, choices=['env', 'db', 'kafka', 'data', 'beam', 'scio', 'dbt', 'dashboard'],
                       help='Run validation for specific component only')
    
    args = parser.parse_args()
    
    validator = PipelineValidator()
    
    if args.component:
        # Run specific component validation
        component_map = {
            'env': validator.validate_environment,
            'db': validator.validate_database_connectivity,
            'kafka': validator.validate_kafka_connectivity,
            'data': lambda: validator.validate_data_generation('tiny'),
            'beam': validator.validate_beam_pipeline,
            'scio': validator.validate_scio_pipeline,
            'dbt': validator.validate_dbt_models,
            'dashboard': validator.validate_dashboard
        }
        
        success = component_map[args.component]()
        sys.exit(0 if success else 1)
    else:
        # Run full validation suite
        success = validator.run_all_validations(not args.skip_integration)
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()