#!/usr/bin/env python3
"""
Zanasi printer client for Lakeland Dairies Batch Processing System
"""

import socket
import time
import logging
from typing import Dict, Any, List, Optional, Tuple
from enum import Enum

from config_settings import ZanasiConfig
from core.enums import SystemComponent, OperationResult, ZanasiStatus
from core.exceptions import ZanasiException, TimeoutException, RetryExhaustedException


class PrintheadNumber(Enum):
    """Enumeration for printhead identification"""
    PRINTHEAD_1 = 1
    PRINTHEAD_2 = 2


class ZanasiCommand:
    """Zanasi printer command builder and validator"""
    
    @staticmethod
    def build_external_field_command(field_index: int, field_type: str, value: str) -> str:
        """
        Build external field command according to Zanasi protocol
        
        Args:
            field_index: Field index (0-19 for string, 0-4 for bitmap, 0-9 for table)
            field_type: Field type ('string', 'bitmap', 'table')
            value: Field value
            
        Returns:
            Formatted command string
        """
        if field_type == 'string':
            if not (0 <= field_index <= 19):
                raise ZanasiException(f"String field index must be 0-19, got {field_index}")
            return f'external_field string {field_index} "{value}"'
        elif field_type == 'bitmap':
            if not (0 <= field_index <= 4):
                raise ZanasiException(f"Bitmap field index must be 0-4, got {field_index}")
            return f'external_field bitmap {field_index} {value}'
        elif field_type == 'table':
            if not (0 <= field_index <= 9):
                raise ZanasiException(f"Table field index must be 0-9, got {field_index}")
            return f'external_field table {field_index} {value}'
        else:
            raise ZanasiException(f"Invalid field type: {field_type}")
    
    @staticmethod
    def build_batch_commands(batch_data: Dict[str, Any]) -> List[str]:
        """
        Build commands for batch data according to Lakeland requirements
        
        Args:
            batch_data: Dictionary containing batch information
            
        Returns:
            List of formatted command strings
        """
        commands = [
            ZanasiCommand.build_external_field_command(0, 'string', batch_data.get('batchCode', '')),
            ZanasiCommand.build_external_field_command(1, 'string', batch_data.get('dryerCode', '')),
            ZanasiCommand.build_external_field_command(2, 'string', batch_data.get('productionDate', '')),
            ZanasiCommand.build_external_field_command(3, 'string', batch_data.get('expiryDate', ''))
        ]
        return commands
    
    @staticmethod
    def validate_command(command: str) -> bool:
        """Validate command format"""
        if not command or not isinstance(command, str):
            return False
        
        # Basic validation - commands should not be empty and should contain valid characters
        if len(command.strip()) == 0:
            return False
            
        # Check for potentially dangerous characters
        dangerous_chars = ['\x00', '\r']  # Null and CR characters
        for char in dangerous_chars:
            if char in command:
                return False
                
        return True


class ZanasiPrintheadClient:
    """Client for individual Zanasi printhead communication"""
    
    def __init__(self, config: ZanasiConfig, printhead: PrintheadNumber):
        self.config = config
        self.printhead = printhead
        self.logger = logging.getLogger(f"{__name__}.ZanasiPrintheadClient.PH{printhead.value}")
        
        # Determine port based on printhead
        self.port = (self.config.printhead1_port if printhead == PrintheadNumber.PRINTHEAD_1 
                    else self.config.printhead2_port)
        
        self.last_error = None
        self.connection_count = 0
        self.command_count = 0
    
    def send_commands(self, commands: List[str]) -> bool:
        """
        Send list of commands to printhead
        
        Args:
            commands: List of command strings
            
        Returns:
            True if all commands sent successfully
            
        Raises:
            ZanasiException: On communication errors
        """
        if not commands:
            raise ZanasiException("No commands provided", printhead=self.printhead.value)
        
        # Validate all commands first
        for i, command in enumerate(commands):
            if not ZanasiCommand.validate_command(command):
                raise ZanasiException(
                    f"Invalid command at index {i}: '{command}'",
                    printhead=self.printhead.value,
                    command=command
                )
        
        self.logger.info(f"Sending {len(commands)} commands to printhead {self.printhead.value}")
        
        for attempt in range(self.config.retry_attempts):
            sock = None
            try:
                # Create socket connection
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self.config.timeout)
                
                self.logger.debug(f"Connecting to {self.config.host}:{self.port} (attempt {attempt + 1})")
                sock.connect((self.config.host, self.port))
                self.connection_count += 1
                
                self.logger.debug(f"Connected to printhead {self.printhead.value}")
                
                # Send each command
                for i, command in enumerate(commands):
                    try:
                        # Add LF terminator as required by Zanasi protocol
                        message = command + '\n'
                        sock.sendall(message.encode('utf-8'))
                        self.command_count += 1
                        
                        self.logger.debug(f"Sent command {i + 1}/{len(commands)}: {command}")
                        
                        # Brief pause between commands
                        if i < len(commands) - 1:  # Don't delay after last command
                            time.sleep(self.config.command_delay)
                            
                    except socket.timeout:
                        raise ZanasiException(
                            f"Timeout sending command {i + 1}: '{command}'",
                            printhead=self.printhead.value,
                            command=command
                        )
                    except socket.error as e:
                        raise ZanasiException(
                            f"Socket error sending command {i + 1}: {e}",
                            printhead=self.printhead.value,
                            command=command
                        ) from e
                
                # Try to receive acknowledgment (optional)
                try:
                    sock.settimeout(1.0)  # Short timeout for response
                    response = sock.recv(1024)
                    if response:
                        response_text = response.decode('utf-8', errors='ignore').strip()
                        self.logger.debug(f"Response from printhead {self.printhead.value}: {response_text}")
                except socket.timeout:
                    self.logger.debug(f"No response from printhead {self.printhead.value} (may be normal)")
                except Exception as e:
                    self.logger.debug(f"Error reading response: {e}")
                
                self.logger.info(f"Successfully sent all commands to printhead {self.printhead.value}")
                self.last_error = None
                return True
                
            except socket.timeout:
                error_msg = f"Connection timeout to printhead {self.printhead.value}"
                self.last_error = error_msg
                self.logger.warning(f"Attempt {attempt + 1} failed: {error_msg}")
                
            except socket.error as e:
                error_msg = f"Socket error connecting to printhead {self.printhead.value}: {e}"
                self.last_error = e
                self.logger.warning(f"Attempt {attempt + 1} failed: {error_msg}")
                
            except Exception as e:
                error_msg = f"Unexpected error communicating with printhead {self.printhead.value}: {e}"
                self.last_error = e
                self.logger.warning(f"Attempt {attempt + 1} failed: {error_msg}")
                
            finally:
                if sock:
                    try:
                        sock.close()
                    except:
                        pass
            
            # Wait before retry (except on last attempt)
            if attempt < self.config.retry_attempts - 1:
                wait_time = 1.0 * (attempt + 1)  # Progressive delay
                self.logger.info(f"Retrying in {wait_time:.1f} seconds...")
                time.sleep(wait_time)
        
        # All attempts failed
        raise RetryExhaustedException(
            f"Failed to send commands to printhead {self.printhead.value} after {self.config.retry_attempts} attempts",
            max_attempts=self.config.retry_attempts,
            last_error=self.last_error
        )
    
    def send_batch_data(self, batch_data: Dict[str, Any]) -> bool:
        """
        Send batch data to printhead
        
        Args:
            batch_data: Dictionary containing batch information
            
        Returns:
            True if successful
        """
        commands = ZanasiCommand.build_batch_commands(batch_data)
        return self.send_commands(commands)
    
    def test_connection(self) -> bool:
        """
        Test connection to printhead
        
        Returns:
            True if connection test successful
        """
        try:
            # Send a simple test command
            test_commands = [ZanasiCommand.build_external_field_command(0, 'string', 'TEST')]
            self.send_commands(test_commands)
            return True
        except Exception as e:
            self.logger.warning(f"Connection test to printhead {self.printhead.value} failed: {e}")
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """Get current printhead status"""
        return {
            'printhead': self.printhead.value,
            'host': self.config.host,
            'port': self.port,
            'connection_count': self.connection_count,
            'command_count': self.command_count,
            'last_error': str(self.last_error) if self.last_error else None
        }


class ZanasiClient:
    """Main Zanasi client that manages both printheads"""
    
    def __init__(self, config: ZanasiConfig):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.ZanasiClient")
        
        # Create printhead clients
        self.printhead1 = ZanasiPrintheadClient(config, PrintheadNumber.PRINTHEAD_1)
        self.printhead2 = ZanasiPrintheadClient(config, PrintheadNumber.PRINTHEAD_2)
        
        self.operation_count = 0
        self.last_batch_sent = None
        self.last_operation_time = None
    
    def send_batch_to_both_printheads(self, batch_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """
        Send batch data to both printheads
        
        Args:
            batch_data: Dictionary containing batch information
            
        Returns:
            Tuple of (overall_success, detailed_results)
        """
        self.logger.info(f"Sending batch data to both printheads: batchIndex={batch_data.get('batchIndex', 'unknown')}")
        
        results = {
            'printhead1': {'success': False, 'error': None},
            'printhead2': {'success': False, 'error': None},
            'overall_success': False,
            'batch_index': batch_data.get('batchIndex'),
            'timestamp': time.time()
        }
        
        start_time = time.time()
        
        # Send to printhead 1
        try:
            self.printhead1.send_batch_data(batch_data)
            results['printhead1']['success'] = True
            self.logger.info("Successfully sent batch data to printhead 1")
        except Exception as e:
            results['printhead1']['error'] = str(e)
            self.logger.error(f"Failed to send batch data to printhead 1: {e}")
        
        # Send to printhead 2
        try:
            self.printhead2.send_batch_data(batch_data)
            results['printhead2']['success'] = True
            self.logger.info("Successfully sent batch data to printhead 2")
        except Exception as e:
            results['printhead2']['error'] = str(e)
            self.logger.error(f"Failed to send batch data to printhead 2: {e}")
        
        # Determine overall success
        results['overall_success'] = results['printhead1']['success'] and results['printhead2']['success']
        
        operation_time = time.time() - start_time
        results['operation_time'] = operation_time
        
        self.operation_count += 1
        self.last_batch_sent = batch_data
        self.last_operation_time = operation_time
        
        if results['overall_success']:
            self.logger.info(f"Successfully sent batch data to both printheads in {operation_time:.2f}s")
        else:
            ph1_status = "OK" if results['printhead1']['success'] else "FAILED"
            ph2_status = "OK" if results['printhead2']['success'] else "FAILED"
            self.logger.error(f"Batch send completed with errors - PH1: {ph1_status}, PH2: {ph2_status}")
        
        return results['overall_success'], results
    
    def send_commands_to_both_printheads(self, commands: List[str]) -> Tuple[bool, Dict[str, Any]]:
        """
        Send custom commands to both printheads
        
        Args:
            commands: List of command strings
            
        Returns:
            Tuple of (overall_success, detailed_results)
        """
        self.logger.info(f"Sending {len(commands)} commands to both printheads")
        
        results = {
            'printhead1': {'success': False, 'error': None},
            'printhead2': {'success': False, 'error': None},
            'overall_success': False,
            'command_count': len(commands),
            'timestamp': time.time()
        }
        
        # Send to printhead 1
        try:
            self.printhead1.send_commands(commands)
            results['printhead1']['success'] = True
        except Exception as e:
            results['printhead1']['error'] = str(e)
            self.logger.error(f"Failed to send commands to printhead 1: {e}")
        
        # Send to printhead 2
        try:
            self.printhead2.send_commands(commands)
            results['printhead2']['success'] = True
        except Exception as e:
            results['printhead2']['error'] = str(e)
            self.logger.error(f"Failed to send commands to printhead 2: {e}")
        
        results['overall_success'] = results['printhead1']['success'] and results['printhead2']['success']
        
        return results['overall_success'], results
    
    def test_both_printheads(self) -> Dict[str, Any]:
        """
        Test connection to both printheads
        
        Returns:
            Dictionary with test results for both printheads
        """
        self.logger.info("Testing connection to both printheads")
        
        results = {
            'printhead1': False,
            'printhead2': False,
            'overall_success': False,
            'timestamp': time.time()
        }
        
        # Test printhead 1
        try:
            results['printhead1'] = self.printhead1.test_connection()
        except Exception as e:
            self.logger.error(f"Error testing printhead 1: {e}")
        
        # Test printhead 2
        try:
            results['printhead2'] = self.printhead2.test_connection()
        except Exception as e:
            self.logger.error(f"Error testing printhead 2: {e}")
        
        results['overall_success'] = results['printhead1'] and results['printhead2']
        
        if results['overall_success']:
            self.logger.info("Both printheads are responding")
        else:
            ph1_status = "OK" if results['printhead1'] else "FAILED"
            ph2_status = "OK" if results['printhead2'] else "FAILED"
            self.logger.warning(f"Printhead test results - PH1: {ph1_status}, PH2: {ph2_status}")
        
        return results
    
    def send_single_printhead(self, printhead_number: int, batch_data: Dict[str, Any]) -> bool:
        """
        Send batch data to a specific printhead
        
        Args:
            printhead_number: Printhead number (1 or 2)
            batch_data: Dictionary containing batch information
            
        Returns:
            True if successful
        """
        if printhead_number == 1:
            return self.printhead1.send_batch_data(batch_data)
        elif printhead_number == 2:
            return self.printhead2.send_batch_data(batch_data)
        else:
            raise ZanasiException(f"Invalid printhead number: {printhead_number}")
    
    def get_comprehensive_status(self) -> Dict[str, Any]:
        """Get comprehensive status of Zanasi system"""
        return {
            'config': {
                'host': self.config.host,
                'printhead1_port': self.config.printhead1_port,
                'printhead2_port': self.config.printhead2_port,
                'timeout': self.config.timeout,
                'retry_attempts': self.config.retry_attempts
            },
            'statistics': {
                'operation_count': self.operation_count,
                'last_operation_time': self.last_operation_time,
                'last_batch_index': self.last_batch_sent.get('batchIndex') if self.last_batch_sent else None
            },
            'printhead1_status': self.printhead1.get_status(),
            'printhead2_status': self.printhead2.get_status()
        }
    
    def reset_statistics(self):
        """Reset operation statistics"""
        self.operation_count = 0
        self.last_batch_sent = None
        self.last_operation_time = None
        self.printhead1.connection_count = 0
        self.printhead1.command_count = 0
        self.printhead2.connection_count = 0
        self.printhead2.command_count = 0
        self.logger.info("Statistics reset")


class ZanasiClientFactory:
    """Factory for creating configured Zanasi clients"""
    
    @staticmethod
    def create_client(config: ZanasiConfig) -> ZanasiClient:
        """Create a new Zanasi client with the given configuration"""
        return ZanasiClient(config)
    
    @staticmethod
    def create_printhead_client(config: ZanasiConfig, printhead: PrintheadNumber) -> ZanasiPrintheadClient:
        """Create a single printhead client"""
        return ZanasiPrintheadClient(config, printhead)
    
    @staticmethod
    def create_with_custom_ports(base_config: ZanasiConfig, ph1_port: int, ph2_port: int) -> ZanasiClient:
        """Create Zanasi client with custom printhead ports"""
        custom_config = ZanasiConfig(
            host=base_config.host,
            printhead1_port=ph1_port,
            printhead2_port=ph2_port,
            timeout=base_config.timeout,
            command_delay=base_config.command_delay,
            retry_attempts=base_config.retry_attempts
        )
        return ZanasiClient(custom_config)


class ZanasiProtocolHelper:
    """Helper class for Zanasi protocol operations"""
    
    @staticmethod
    def format_batch_for_logging(batch_data: Dict[str, Any]) -> str:
        """Format batch data for logging"""
        return (f"Batch {batch_data.get('batchIndex', 'unknown')}: "
                f"Code={batch_data.get('batchCode', '')}, "
                f"Dryer={batch_data.get('dryerCode', '')}, "
                f"Prod={batch_data.get('productionDate', '')}, "
                f"Exp={batch_data.get('expiryDate', '')}")
    
    @staticmethod
    def validate_batch_data(batch_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate batch data for Zanasi transmission
        
        Args:
            batch_data: Dictionary containing batch information
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        required_fields = ['batchCode', 'dryerCode', 'productionDate', 'expiryDate']
        for field in required_fields:
            if field not in batch_data:
                errors.append(f"Missing required field: {field}")
            elif not isinstance(batch_data[field], str):
                errors.append(f"Field {field} must be a string")
        
        # Check string lengths (Zanasi protocol limits)
        max_lengths = {
            'batchCode': 100,  # Zanasi supports up to 100 chars per string field
            'dryerCode': 100,
            'productionDate': 100,
            'expiryDate': 100
        }
        
        for field, max_length in max_lengths.items():
            if field in batch_data:
                value = str(batch_data[field])
                if len(value) > max_length:
                    errors.append(f"Field {field} too long (max {max_length}): {len(value)} chars")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def escape_string_for_zanasi(value: str) -> str:
        """Escape string value for Zanasi protocol"""
        # Escape quotes and handle special characters
        escaped = value.replace('"', '\\"')
        
        # Remove or replace problematic characters
        escaped = escaped.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        
        return escaped