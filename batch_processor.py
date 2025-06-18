#!/usr/bin/env python3
"""
Main batch processor orchestrator for Lakeland Dairies Batch Processing System
Version 18: Modular, robust, service-ready implementation
"""

import time
import logging
import signal
import sys
from typing import Optional, Dict, Any

from config_settings import Settings
from communication.modbus_client import ModbusClientFactory
from communication.firebase_client import FirebaseClientFactory
from communication.zanasi_client import ZanasiClientFactory
from processing.status_manager import StatusManager, StatusMonitor
from processing.batch_manager import BatchManager
from processing.data_parser import DataParser
from core.enums import TriggerStates, ProcessingStates, ErrorCodes
from core.exceptions import LakelandBatchException, CriticalSystemException


class BatchProcessor:
    """Main orchestrator for the batch processing system"""
    
    def __init__(self, config_path: Optional[str] = None):
        # Load configuration
        self.settings = Settings(config_path)
        if not self.settings.validate():
            raise CriticalSystemException("Invalid configuration", requires_restart=True)
        
        # Setup logging
        self._setup_logging()
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing Lakeland Dairies Batch Processing System v18")
        
        # Initialize components
        self._initialize_components()
        
        # Runtime state
        self.is_running = False
        self.shutdown_requested = False
        self.last_trigger_state = TriggerStates.IDLE
        self.operation_count = 0
        self.error_count = 0
        self.last_error = None
        
        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()
        
        self.logger.info("Batch processor initialization completed")
    
    def _setup_logging(self):
        """Configure structured logging"""
        import os
        from logging.handlers import RotatingFileHandler
        
        # Create log directory
        log_dir = os.path.expanduser(self.settings.logging.log_dir)
        os.makedirs(log_dir, exist_ok=True)
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, self.settings.logging.level))
        
        # Clear existing handlers
        root_logger.handlers.clear()
        
        # File handler with rotation
        log_file = os.path.join(log_dir, self.settings.logging.log_file)
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=self.settings.logging.max_file_size,
            backupCount=self.settings.logging.backup_count
        )
        file_handler.setFormatter(logging.Formatter(self.settings.logging.format))
        root_logger.addHandler(file_handler)
        
        # Console handler
        if self.settings.logging.console_output:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter(self.settings.logging.format))
            root_logger.addHandler(console_handler)
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("Logging configured successfully")
    
    def _initialize_components(self):
        """Initialize all system components"""
        try:
            # Initialize clients
            self.modbus_client = ModbusClientFactory.create_client(self.settings.modbus)
            self.firebase_client = FirebaseClientFactory.create_client(self.settings.firebase)
            self.zanasi_client = ZanasiClientFactory.create_client(self.settings.zanasi)
            
            # Initialize managers
            self.status_manager = StatusManager(self.modbus_client)
            self.data_parser = DataParser()
            self.batch_manager = BatchManager(
                self.modbus_client,
                self.firebase_client,
                self.zanasi_client,
                self.status_manager,
                self.data_parser
            )
            
            # Initialize monitoring
            self.status_monitor = StatusMonitor(
                self.status_manager,
                self.settings.processing.polling_interval
            )
            
            # Register status callbacks
            self._register_status_callbacks()
            
            self.logger.info("All components initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing components: {e}")
            raise CriticalSystemException(
                f"Failed to initialize system components: {e}",
                requires_restart=True
            ) from e
    
    def _register_status_callbacks(self):
        """Register callbacks for status monitoring"""
        self.status_monitor.register_callback('trigger_change', self._on_trigger_change)
        self.status_monitor.register_callback('error', self._on_error_detected)
        self.status_monitor.register_callback('state_change', self._on_state_change)
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            self.shutdown_requested = True
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def _on_trigger_change(self, event_data: Dict[str, Any]):
        """Handle trigger state changes"""
        old_trigger = event_data.get('old_trigger')
        new_trigger = event_data.get('new_trigger')
        
        self.logger.info(f"Trigger changed: {old_trigger} -> {new_trigger}")
        
        # Process trigger changes in main loop to avoid threading issues
        # This callback just logs the change
    
    def _on_error_detected(self, event_data: Dict[str, Any]):
        """Handle error state detection"""
        error_code = event_data.get('error_code')
        self.logger.error(f"System error detected: {error_code}")
        self.error_count += 1
    
    def _on_state_change(self, event_data: Dict[str, Any]):
        """Handle processing state changes"""
        old_state = event_data.get('old_state')
        new_state = event_data.get('new_state')
        
        self.logger.info(f"Processing state changed: {old_state} -> {new_state}")
    
    def start(self):
        """Start the batch processing system"""
        try:
            self.logger.info("Starting Lakeland Dairies Batch Processing System")
            
            # Test all connections
            self._test_connections()
            
            # Reset system to known state
            self.status_manager.reset_system_state()
            
            # Start monitoring
            self.status_monitor.start_monitoring()
            
            # Enter main processing loop
            self.is_running = True
            self._main_processing_loop()
            
        except KeyboardInterrupt:
            self.logger.info("System shutdown requested by user")
        except CriticalSystemException as e:
            self.logger.critical(f"Critical system error: {e}")
            if e.requires_restart:
                self.logger.critical("System requires restart")
                sys.exit(1)
        except Exception as e:
            self.logger.error(f"Unexpected error in main system: {e}")
            self.last_error = e
        finally:
            self._shutdown()
    
    def _test_connections(self):
        """Test all system connections"""
        self.logger.info("Testing system connections...")
        
        # Test Modbus connection
        try:
            self.modbus_client.connect()
            self.logger.info("✓ PLC connection successful")
        except Exception as e:
            raise CriticalSystemException(f"PLC connection failed: {e}", requires_restart=True)
        
        # Test Firebase connection
        try:
            self.firebase_client.test_connection()
            self.logger.info("✓ Firebase connection successful")
        except Exception as e:
            self.logger.warning(f"Firebase connection test failed: {e}")
            # Firebase errors are not critical for startup
        
        # Test Zanasi connections
        try:
            zanasi_results = self.zanasi_client.test_both_printheads()
            if zanasi_results['overall_success']:
                self.logger.info("✓ Zanasi printheads connection successful")
            else:
                self.logger.warning(f"Zanasi connection issues: {zanasi_results}")
                # Zanasi errors are not critical for startup
        except Exception as e:
            self.logger.warning(f"Zanasi connection test failed: {e}")
    
    def _main_processing_loop(self):
        """Main processing loop"""
        self.logger.info("Entering main processing loop")
        
        while self.is_running and not self.shutdown_requested:
            try:
                # Read current trigger state
                current_trigger = self.status_manager.get_current_trigger()
                
                # Process triggers based on state changes
                if current_trigger != self.last_trigger_state:
                    self._process_trigger_change(self.last_trigger_state, current_trigger)
                    self.last_trigger_state = current_trigger
                
                # Sleep before next iteration
                time.sleep(self.settings.processing.polling_interval)
                
            except LakelandBatchException as e:
                self.logger.error(f"Batch processing error: {e}")
                self.error_count += 1
                self.last_error = e
                
                # Set error state and continue
                self.status_manager.set_error_code(e.error_code or ErrorCodes.DATA_FORMAT_ERROR)
                self.status_manager.set_processing_status(ProcessingStates.ERROR)
                
                # Wait longer after error
                time.sleep(self.settings.processing.polling_interval * 3)
                
            except Exception as e:
                self.logger.error(f"Unexpected error in processing loop: {e}")
                self.error_count += 1
                self.last_error = e
                
                # Brief recovery pause
                time.sleep(self.settings.processing.polling_interval * 2)
    
    def _process_trigger_change(self, old_trigger: TriggerStates, new_trigger: TriggerStates):
        """Process trigger state changes"""
        self.logger.info(f"Processing trigger change: {old_trigger} -> {new_trigger}")
        
        try:
            if new_trigger == TriggerStates.DOWNLOAD_BATCH:
                self.logger.info("Download batch trigger detected")
                self._handle_download_batch()
                
            elif new_trigger == TriggerStates.LOAD_TO_ZANASI:
                self.logger.info("Load to Zanasi trigger detected")
                self._handle_load_to_zanasi()
                
            self.operation_count += 1
            
        except Exception as e:
            self.logger.error(f"Error processing trigger {new_trigger}: {e}")
            self.status_manager.set_error_code(ErrorCodes.DATA_FORMAT_ERROR)
            self.status_manager.set_processing_status(ProcessingStates.ERROR)
            raise
    
    def _handle_download_batch(self):
        """Handle download batch operation"""
        try:
            success = self.batch_manager.process_download_batch_trigger()
            if success:
                self.logger.info("Download batch operation completed successfully")
            else:
                self.logger.error("Download batch operation failed")
                
        except Exception as e:
            self.logger.error(f"Error in download batch operation: {e}")
            raise
    
    def _handle_load_to_zanasi(self):
        """Handle load to Zanasi operation"""
        try:
            success = self.batch_manager.process_load_to_zanasi_trigger()
            if success:
                self.logger.info("Load to Zanasi operation completed successfully")
            else:
                self.logger.error("Load to Zanasi operation failed")
                
        except Exception as e:
            self.logger.error(f"Error in load to Zanasi operation: {e}")
            raise
    
    def _shutdown(self):
        """Graceful system shutdown"""
        self.logger.info("Initiating system shutdown...")
        
        try:
            # Stop monitoring
            self.status_monitor.stop_monitoring()
            
            # Reset system state
            if hasattr(self, 'status_manager'):
                self.status_manager.reset_system_state()
            
            # Close connections
            if hasattr(self, 'modbus_client'):
                self.modbus_client.disconnect()
            
            self.is_running = False
            self.logger.info("System shutdown completed")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        try:
            return {
                'system_info': {
                    'version': '18.0',
                    'is_running': self.is_running,
                    'operation_count': self.operation_count,
                    'error_count': self.error_count,
                    'last_error': str(self.last_error) if self.last_error else None
                },
                'component_status': {
                    'modbus': self.modbus_client.get_connection_info() if hasattr(self, 'modbus_client') else None,
                    'firebase': self.firebase_client.get_connection_info() if hasattr(self, 'firebase_client') else None,
                    'zanasi': self.zanasi_client.get_comprehensive_status() if hasattr(self, 'zanasi_client') else None
                },
                'processing_status': self.status_manager.get_system_status_summary() if hasattr(self, 'status_manager') else None,
                'batch_status': self.batch_manager.get_status_summary() if hasattr(self, 'batch_manager') else None
            }
        except Exception as e:
            return {'error': f"Error getting system status: {e}"}


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Lakeland Dairies Batch Processing System')
    parser.add_argument('--config', '-c', help='Configuration file path')
    parser.add_argument('--daemon', '-d', action='store_true', help='Run as daemon')
    parser.add_argument('--create-config', help='Create sample configuration file')
    parser.add_argument('--test', action='store_true', help='Test configuration and connections')
    parser.add_argument('--status', action='store_true', help='Show system status')
    
    args = parser.parse_args()
    
    # Handle special commands
    if args.create_config:
        settings = Settings()
        settings.create_sample_config(args.create_config)
        print(f"Sample configuration created at {args.create_config}")
        return
    
    try:
        # Initialize processor
        processor = BatchProcessor(args.config)
        
        if args.test:
            # Test mode - validate config and test connections
            print("Testing configuration and connections...")
            processor._test_connections()
            print("✓ All tests passed")
            return
        
        if args.status:
            # Status mode - show current system status
            status = processor.get_system_status()
            import json
            print(json.dumps(status, indent=2, default=str))
            return
        
        if args.daemon:
            # Daemon mode - implement proper daemon behavior
            _run_as_daemon(processor)
        else:
            # Foreground mode
            processor.start()
            
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


def _run_as_daemon(processor: BatchProcessor):
    """Run processor as a daemon"""
    import os
    import atexit
    
    # Write PID file
    pid_file = processor.settings.service.pid_file
    try:
        with open(pid_file, 'w') as f:
            f.write(str(os.getpid()))
        
        def cleanup_pid():
            try:
                os.unlink(pid_file)
            except:
                pass
        
        atexit.register(cleanup_pid)
        
    except Exception as e:
        print(f"Warning: Could not write PID file {pid_file}: {e}")
    
    # Change working directory
    try:
        os.chdir(processor.settings.service.working_directory)
    except Exception as e:
        print(f"Warning: Could not change to working directory: {e}")
    
    # Start processor
    processor.start()


if __name__ == "__main__":
    main()
