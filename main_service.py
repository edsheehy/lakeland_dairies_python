#!/usr/bin/env python3
"""
Main service entry point for Lakeland Dairies Batch Processing System
Designed to run as a system service with proper daemon behavior
"""

import os
import sys
import time
import signal
import logging
import argparse
from pathlib import Path

# Add the current directory to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from batch_processor import BatchProcessor
from config_settings import Settings
from core.exceptions import CriticalSystemException


class ServiceManager:
    """Manages the batch processing service lifecycle"""
    
    def __init__(self):
        self.processor = None
        self.logger = None
        self.shutdown_requested = False
        
    def setup_service_environment(self, config_path: str = None):
        """Setup the service environment"""
        # Load settings first to get service configuration
        settings = Settings(config_path)
        
        # Change to working directory if specified
        if settings.service.working_directory and os.path.exists(settings.service.working_directory):
            os.chdir(settings.service.working_directory)
            print(f"Changed working directory to {settings.service.working_directory}")
        
        # Setup basic logging before initializing processor
        log_dir = os.path.expanduser(settings.logging.log_dir)
        os.makedirs(log_dir, exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, settings.logging.level),
            format=settings.logging.format,
            handlers=[
                logging.FileHandler(os.path.join(log_dir, 'service.log')),
                logging.StreamHandler() if settings.logging.console_output else logging.NullHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("Service environment setup completed")
        
        return settings
    
    def create_pid_file(self, pid_file_path: str):
        """Create PID file for service management"""
        try:
            pid_dir = os.path.dirname(pid_file_path)
            if pid_dir:
                os.makedirs(pid_dir, exist_ok=True)
            
            with open(pid_file_path, 'w') as f:
                f.write(str(os.getpid()))
            
            self.logger.info(f"PID file created: {pid_file_path}")
            
            # Register cleanup
            import atexit
            atexit.register(lambda: self.cleanup_pid_file(pid_file_path))
            
        except Exception as e:
            self.logger.warning(f"Could not create PID file {pid_file_path}: {e}")
    
    def cleanup_pid_file(self, pid_file_path: str):
        """Clean up PID file on shutdown"""
        try:
            if os.path.exists(pid_file_path):
                os.unlink(pid_file_path)
                if self.logger:
                    self.logger.info(f"PID file removed: {pid_file_path}")
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Error removing PID file: {e}")
    
    def setup_signal_handlers(self):
        """Setup signal handlers for service management"""
        def shutdown_handler(signum, frame):
            signal_name = signal.Signals(signum).name
            if self.logger:
                self.logger.info(f"Received {signal_name}, initiating shutdown...")
            self.shutdown_requested = True
            
            if self.processor:
                self.processor.shutdown_requested = True
        
        def reload_handler(signum, frame):
            if self.logger:
                self.logger.info("Received SIGHUP, configuration reload not implemented yet")
        
        # Handle termination signals
        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)
        
        # Handle reload signal (if supported on platform)
        if hasattr(signal, 'SIGHUP'):
            signal.signal(signal.SIGHUP, reload_handler)
        
        if self.logger:
            self.logger.info("Signal handlers configured")
    
    def run_as_daemon(self):
        """Convert the process to run as a daemon"""
        try:
            # First fork
            pid = os.fork()
            if pid > 0:
                # Parent process - exit
                sys.exit(0)
        except OSError as e:
            self.logger.error(f"First fork failed: {e}")
            sys.exit(1)
        
        # Become session leader
        os.setsid()
        
        try:
            # Second fork
            pid = os.fork()
            if pid > 0:
                # Parent process - exit
                sys.exit(0)
        except OSError as e:
            self.logger.error(f"Second fork failed: {e}")
            sys.exit(1)
        
        # Change working directory and file permissions
        os.chdir("/")
        os.umask(0)
        
        # Redirect standard streams
        sys.stdout.flush()
        sys.stderr.flush()
        
        # Close standard streams and redirect to /dev/null
        with open('/dev/null', 'r') as null_in:
            os.dup2(null_in.fileno(), sys.stdin.fileno())
        
        with open('/dev/null', 'w') as null_out:
            os.dup2(null_out.fileno(), sys.stdout.fileno())
            os.dup2(null_out.fileno(), sys.stderr.fileno())
        
        if self.logger:
            self.logger.info("Successfully daemonized process")
    
    def start_service(self, config_path: str = None, daemon_mode: bool = False):
        """Start the batch processing service"""
        try:
            # Setup environment
            settings = self.setup_service_environment(config_path)
            
            # Setup signal handlers
            self.setup_signal_handlers()
            
            # Create PID file
            if settings.service.pid_file:
                self.create_pid_file(settings.service.pid_file)
            
            # Daemonize if requested
            if daemon_mode and settings.service.run_as_daemon:
                self.run_as_daemon()
            
            # Initialize and start the processor
            self.logger.info("Starting Lakeland Dairies Batch Processing Service")
            self.processor = BatchProcessor(config_path)
            
            # Service main loop
            self._service_main_loop()
            
        except CriticalSystemException as e:
            if self.logger:
                self.logger.critical(f"Critical system error: {e}")
            sys.exit(1)
        except Exception as e:
            if self.logger:
                self.logger.error(f"Service startup failed: {e}")
            else:
                print(f"Service startup failed: {e}")
            sys.exit(1)
    
    def _service_main_loop(self):
        """Main service loop with restart capability"""
        restart_attempts = 0
        max_restart_attempts = 5
        restart_delay = 30  # seconds
        
        while not self.shutdown_requested:
            try:
                self.logger.info("Starting batch processor...")
                self.processor.start()
                
                # If we get here, processor stopped normally
                if not self.shutdown_requested:
                    self.logger.warning("Processor stopped unexpectedly, checking for restart...")
                    restart_attempts += 1
                    
                    if restart_attempts <= max_restart_attempts:
                        self.logger.info(f"Restarting processor (attempt {restart_attempts}/{max_restart_attempts}) in {restart_delay} seconds...")
                        time.sleep(restart_delay)
                        
                        # Create new processor instance
                        try:
                            self.processor = BatchProcessor()
                        except Exception as e:
                            self.logger.error(f"Failed to create new processor instance: {e}")
                            break
                    else:
                        self.logger.error(f"Maximum restart attempts ({max_restart_attempts}) reached, stopping service")
                        break
                else:
                    self.logger.info("Service shutdown requested, stopping gracefully")
                    break
                    
            except CriticalSystemException as e:
                self.logger.critical(f"Critical error in processor: {e}")
                if e.requires_restart:
                    self.logger.critical("System requires restart, exiting...")
                    sys.exit(1)
                break
                
            except KeyboardInterrupt:
                self.logger.info("Service interrupted by user")
                break
                
            except Exception as e:
                self.logger.error(f"Unexpected error in service loop: {e}")
                restart_attempts += 1
                
                if restart_attempts <= max_restart_attempts:
                    self.logger.info(f"Attempting restart in {restart_delay} seconds...")
                    time.sleep(restart_delay)
                else:
                    self.logger.error("Too many consecutive failures, stopping service")
                    break
        
        self.logger.info("Service main loop exited")
    
    def stop_service(self, pid_file_path: str = None):
        """Stop running service using PID file"""
        if not pid_file_path:
            # Try default locations
            possible_paths = [
                "/var/run/lakeland_batch_processor.pid",
                "/tmp/lakeland_batch_processor.pid",
                "./lakeland_batch_processor.pid"
            ]
            
            for path in possible_paths:
                if os.path.exists(path):
                    pid_file_path = path
                    break
        
        if not pid_file_path or not os.path.exists(pid_file_path):
            print("PID file not found, cannot stop service")
            return False
        
        try:
            with open(pid_file_path, 'r') as f:
                pid = int(f.read().strip())
            
            print(f"Stopping service with PID {pid}...")
            os.kill(pid, signal.SIGTERM)
            
            # Wait for process to stop
            for _ in range(30):  # Wait up to 30 seconds
                try:
                    os.kill(pid, 0)  # Check if process exists
                    time.sleep(1)
                except OSError:
                    print("Service stopped successfully")
                    return True
            
            # Force kill if still running
            print("Service did not stop gracefully, forcing...")
            os.kill(pid, signal.SIGKILL)
            return True
            
        except Exception as e:
            print(f"Error stopping service: {e}")
            return False
    
    def get_service_status(self, pid_file_path: str = None):
        """Check service status"""
        if not pid_file_path:
            settings = Settings()
            pid_file_path = settings.service.pid_file
        
        if not os.path.exists(pid_file_path):
            return {"status": "stopped", "message": "PID file not found"}
        
        try:
            with open(pid_file_path, 'r') as f:
                pid = int(f.read().strip())
            
            # Check if process is running
            try:
                os.kill(pid, 0)
                return {
                    "status": "running", 
                    "pid": pid,
                    "pid_file": pid_file_path,
                    "message": f"Service running with PID {pid}"
                }
            except OSError:
                return {
                    "status": "dead", 
                    "pid": pid,
                    "message": f"PID file exists but process {pid} is not running"
                }
                
        except Exception as e:
            return {"status": "unknown", "message": f"Error checking status: {e}"}


def main():
    """Main entry point for service management"""
    parser = argparse.ArgumentParser(
        description='Lakeland Dairies Batch Processing Service',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s start --daemon              # Start as daemon service
  %(prog)s start --config ./config.json  # Start with custom config
  %(prog)s stop                        # Stop running service
  %(prog)s status                      # Check service status
  %(prog)s --create-config config.json # Create sample configuration
        """
    )
    
    parser.add_argument('action', nargs='?', choices=['start', 'stop', 'restart', 'status'], 
                       default='start', help='Service action (default: start)')
    parser.add_argument('--config', '-c', help='Configuration file path')
    parser.add_argument('--daemon', '-d', action='store_true', help='Run as daemon')
    parser.add_argument('--pid-file', help='PID file path')
    parser.add_argument('--create-config', metavar='PATH', help='Create sample configuration file')
    parser.add_argument('--test-config', action='store_true', help='Test configuration and exit')
    parser.add_argument('--version', action='version', version='Lakeland Batch Processor v18.0')
    
    args = parser.parse_args()
    
    # Handle configuration creation
    if args.create_config:
        try:
            settings = Settings()
            settings.create_sample_config(args.create_config)
            print(f"✓ Sample configuration created at {args.create_config}")
            return 0
        except Exception as e:
            print(f"✗ Error creating configuration: {e}")
            return 1
    
    # Handle configuration testing
    if args.test_config:
        try:
            settings = Settings(args.config)
            if settings.validate():
                print("✓ Configuration is valid")
                return 0
            else:
                print("✗ Configuration validation failed")
                return 1
        except Exception as e:
            print(f"✗ Configuration error: {e}")
            return 1
    
    # Initialize service manager
    service_manager = ServiceManager()
    
    # Handle service actions
    if args.action == 'start':
        try:
            service_manager.start_service(args.config, args.daemon)
            return 0
        except KeyboardInterrupt:
            print("\nService stopped by user")
            return 0
        except Exception as e:
            print(f"Failed to start service: {e}")
            return 1
    
    elif args.action == 'stop':
        if service_manager.stop_service(args.pid_file):
            return 0
        else:
            return 1
    
    elif args.action == 'restart':
        print("Stopping service...")
        service_manager.stop_service(args.pid_file)
        time.sleep(2)
        print("Starting service...")
        try:
            service_manager.start_service(args.config, args.daemon)
            return 0
        except Exception as e:
            print(f"Failed to restart service: {e}")
            return 1
    
    elif args.action == 'status':
        status = service_manager.get_service_status(args.pid_file)
        print(f"Service status: {status['status']}")
        print(f"Message: {status['message']}")
        
        if 'pid' in status:
            print(f"PID: {status['pid']}")
        if 'pid_file' in status:
            print(f"PID file: {status['pid_file']}")
        
        return 0 if status['status'] == 'running' else 1


if __name__ == "__main__":
    sys.exit(main())