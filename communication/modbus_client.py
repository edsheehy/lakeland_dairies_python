#!/usr/bin/env python3
"""
Modbus client for PLC communication in Lakeland Dairies Batch Processing System
"""

import time
import logging
from typing import Optional, List, Union
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ConnectionException

from ..config_settings import ModbusConfig
from ..core.enums import SystemComponent, ConnectionState, OperationResult
from ..core.exceptions import ModbusException, TimeoutException, RetryExhaustedException


class ModbusClient:
    """Enhanced Modbus TCP client with connection management and retry logic"""
    
    def __init__(self, config: ModbusConfig):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.ModbusClient")
        self.client = ModbusTcpClient(self.config.host, port=self.config.port)
        self.connection_state = ConnectionState.DISCONNECTED
        self.last_error = None
        self.retry_count = 0
        
    def connect(self) -> bool:
        """
        Establish connection to PLC with retry logic
        
        Returns:
            True if connection successful, False otherwise
        """
        self.connection_state = ConnectionState.CONNECTING
        
        for attempt in range(self.config.retry_attempts):
            try:
                self.logger.info(f"Attempting to connect to PLC at {self.config.host}:{self.config.port} (attempt {attempt + 1})")
                
                if self.client.connect():
                    self.connection_state = ConnectionState.CONNECTED
                    self.retry_count = 0
                    self.last_error = None
                    self.logger.info("Successfully connected to PLC")
                    return True
                else:
                    raise ModbusException(f"Failed to connect to PLC at {self.config.host}:{self.config.port}")
                    
            except Exception as e:
                self.last_error = e
                self.logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                
                if attempt < self.config.retry_attempts - 1:
                    self.logger.info(f"Retrying in {self.config.retry_delay} seconds...")
                    time.sleep(self.config.retry_delay)
        
        self.connection_state = ConnectionState.FAILED
        raise RetryExhaustedException(
            f"Failed to connect to PLC after {self.config.retry_attempts} attempts",
            max_attempts=self.config.retry_attempts,
            last_error=self.last_error
        )
    
    def disconnect(self):
        """Disconnect from PLC"""
        try:
            if self.client.connected:
                self.client.close()
                self.logger.info("Disconnected from PLC")
        except Exception as e:
            self.logger.warning(f"Error during disconnect: {e}")
        finally:
            self.connection_state = ConnectionState.DISCONNECTED
    
    def is_connected(self) -> bool:
        """Check if client is connected to PLC"""
        return self.client.connected and self.connection_state == ConnectionState.CONNECTED
    
    def ensure_connected(self):
        """Ensure connection is established, reconnect if necessary"""
        if not self.is_connected():
            self.logger.info("Connection lost, attempting to reconnect...")
            self.connect()
    
    def read_holding_register(self, register: int, count: int = 1) -> Union[int, List[int]]:
        """
        Read holding register(s) from PLC with error handling
        
        Args:
            register: Starting register address (1-based)
            count: Number of registers to read
            
        Returns:
            Single register value or list of values
            
        Raises:
            ModbusException: On communication errors
        """
        self.ensure_connected()
        
        try:
            self.logger.debug(f"Reading register {register}, count={count}")
            
            result = self.client.read_holding_registers(
                register, 
                count=count, 
                slave=self.config.slave_id
            )
            
            if hasattr(result, 'isError') and result.isError():
                raise ModbusException(
                    f"Modbus read error for register {register}: {result}",
                    register=register,
                    slave_id=self.config.slave_id
                )
            
            values = result.registers
            self.logger.debug(f"Successfully read register {register}: {values}")
            
            return values[0] if count == 1 else values
            
        except ConnectionException as e:
            self.connection_state = ConnectionState.FAILED
            raise ModbusException(
                f"Connection error reading register {register}: {e}",
                register=register,
                slave_id=self.config.slave_id
            ) from e
        except Exception as e:
            raise ModbusException(
                f"Unexpected error reading register {register}: {e}",
                register=register,
                slave_id=self.config.slave_id
            ) from e
    
    def write_holding_register(self, register: int, value: int) -> bool:
        """
        Write single holding register to PLC
        
        Args:
            register: Register address (1-based)
            value: Value to write (0-65535)
            
        Returns:
            True if successful
            
        Raises:
            ModbusException: On communication errors
        """
        self.ensure_connected()
        
        # Validate register value
        if not (0 <= value <= 65535):
            raise ModbusException(
                f"Register value out of range (0-65535): {value}",
                register=register
            )
        
        try:
            self.logger.debug(f"Writing register {register} = {value}")
            
            result = self.client.write_register(
                register, 
                value, 
                slave=self.config.slave_id
            )
            
            if hasattr(result, 'isError') and result.isError():
                raise ModbusException(
                    f"Modbus write error for register {register}: {result}",
                    register=register,
                    slave_id=self.config.slave_id
                )
            
            self.logger.debug(f"Successfully wrote register {register} = {value}")
            return True
            
        except ConnectionException as e:
            self.connection_state = ConnectionState.FAILED
            raise ModbusException(
                f"Connection error writing register {register}: {e}",
                register=register,
                slave_id=self.config.slave_id
            ) from e
        except Exception as e:
            raise ModbusException(
                f"Unexpected error writing register {register}: {e}",
                register=register,
                slave_id=self.config.slave_id
            ) from e
    
    def write_holding_registers(self, start_register: int, values: List[int]) -> bool:
        """
        Write multiple holding registers to PLC
        
        Args:
            start_register: Starting register address (1-based)
            values: List of values to write
            
        Returns:
            True if successful
            
        Raises:
            ModbusException: On communication errors
        """
        self.ensure_connected()
        
        # Validate register values
        for i, value in enumerate(values):
            if not isinstance(value, int) or not (0 <= value <= 65535):
                raise ModbusException(
                    f"Invalid value at index {i}: {value} (must be 0-65535)",
                    register=start_register + i
                )
        
        try:
            self.logger.debug(f"Writing {len(values)} registers starting at {start_register}")
            
            result = self.client.write_registers(
                start_register, 
                values, 
                slave=self.config.slave_id
            )
            
            if hasattr(result, 'isError') and result.isError():
                raise ModbusException(
                    f"Modbus write error for registers {start_register}-{start_register + len(values) - 1}: {result}",
                    register=start_register,
                    slave_id=self.config.slave_id
                )
            
            self.logger.info(f"Successfully wrote {len(values)} registers starting at {start_register}")
            return True
            
        except ConnectionException as e:
            self.connection_state = ConnectionState.FAILED
            raise ModbusException(
                f"Connection error writing registers {start_register}-{start_register + len(values) - 1}: {e}",
                register=start_register,
                slave_id=self.config.slave_id
            ) from e
        except Exception as e:
            raise ModbusException(
                f"Unexpected error writing registers {start_register}-{start_register + len(values) - 1}: {e}",
                register=start_register,
                slave_id=self.config.slave_id
            ) from e
    
    def read_multiple_registers(self, register_map: dict) -> dict:
        """
        Read multiple registers efficiently
        
        Args:
            register_map: Dictionary of {name: (register, count)}
            
        Returns:
            Dictionary of {name: value} results
        """
        results = {}
        
        for name, (register, count) in register_map.items():
            try:
                value = self.read_holding_register(register, count)
                results[name] = value
            except ModbusException as e:
                self.logger.error(f"Failed to read {name} (register {register}): {e}")
                raise
        
        return results
    
    def write_multiple_registers(self, register_map: dict) -> bool:
        """
        Write multiple individual registers
        
        Args:
            register_map: Dictionary of {register: value}
            
        Returns:
            True if all writes successful
        """
        success_count = 0
        
        for register, value in register_map.items():
            try:
                self.write_holding_register(register, value)
                success_count += 1
            except ModbusException as e:
                self.logger.error(f"Failed to write register {register} = {value}: {e}")
                raise
        
        self.logger.info(f"Successfully wrote {success_count} registers")
        return True
    
    def test_connection(self) -> bool:
        """
        Test connection by reading a known register
        
        Returns:
            True if test successful
        """
        try:
            # Try to read register 1 (trigger register)
            self.read_holding_register(1)
            return True
        except Exception as e:
            self.logger.warning(f"Connection test failed: {e}")
            return False
    
    def get_connection_info(self) -> dict:
        """Get current connection information"""
        return {
            'host': self.config.host,
            'port': self.config.port,
            'slave_id': self.config.slave_id,
            'connected': self.is_connected(),
            'connection_state': self.connection_state.value,
            'retry_count': self.retry_count,
            'last_error': str(self.last_error) if self.last_error else None
        }
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()


class PLCRegisterManager:
    """High-level interface for PLC register operations"""
    
    def __init__(self, modbus_client: ModbusClient):
        self.client = modbus_client
        self.logger = logging.getLogger(f"{__name__}.PLCRegisterManager")
    
    def read_control_registers(self) -> dict:
        """Read all control and status registers"""
        control_registers = {
            'trigger': (1, 1),
            'rasp_pi_status': (2, 1),
            'plc_status': (3, 1),
            'zanasi_status': (4, 1),
            'error_code': (5, 1),
            'selected_batch': (7, 1)
        }
        
        return self.client.read_multiple_registers(control_registers)
    
    def write_control_register(self, register_name: str, value: int) -> bool:
        """Write to a specific control register"""
        register_map = {
            'trigger': 1,
            'rasp_pi_status': 2,
            'plc_status': 3,
            'zanasi_status': 4,
            'error_code': 5,
            'selected_batch': 7
        }
        
        if register_name not in register_map:
            raise ModbusException(f"Unknown control register: {register_name}")
        
        register = register_map[register_name]
        return self.client.write_holding_register(register, value)
    
    def read_batch_registers(self, batch_number: int, register_count: int = 20) -> List[int]:
        """
        Read registers for a specific batch
        
        Args:
            batch_number: Batch number (1-5)
            register_count: Number of registers per batch (default 20)
            
        Returns:
            List of register values for the batch
        """
        if not 1 <= batch_number <= 5:
            raise ModbusException(f"Invalid batch number: {batch_number}")
        
        start_register = 10 + (batch_number - 1) * register_count
        return self.client.read_holding_register(start_register, register_count)
    
    def write_batch_registers(self, batch_number: int, values: List[int]) -> bool:
        """
        Write registers for a specific batch
        
        Args:
            batch_number: Batch number (1-5)
            values: List of 20 register values
            
        Returns:
            True if successful
        """
        if not 1 <= batch_number <= 5:
            raise ModbusException(f"Invalid batch number: {batch_number}")
        
        if len(values) != 20:
            raise ModbusException(f"Expected 20 register values, got {len(values)}")
        
        start_register = 10 + (batch_number - 1) * 20
        return self.client.write_holding_registers(start_register, values)
    
    def read_all_batch_data(self) -> List[int]:
        """Read all 120 registers from PLC"""
        return self.client.read_holding_register(1, 120)
    
    def write_all_batch_data(self, register_array: List[int]) -> bool:
        """
        Write complete 120-register array to PLC
        
        Args:
            register_array: Complete array of 120 register values
            
        Returns:
            True if successful
        """
        if len(register_array) != 120:
            raise ModbusException(f"Expected 120 register values, got {len(register_array)}")
        
        return self.client.write_holding_registers(1, register_array)
    
    def monitor_trigger_changes(self, callback_func, poll_interval: float = 1.0):
        """
        Monitor trigger register for changes and call callback
        
        Args:
            callback_func: Function to call when trigger changes
            poll_interval: Polling interval in seconds
        """
        last_trigger = None
        
        while True:
            try:
                current_trigger = self.client.read_holding_register(1)
                
                if last_trigger is not None and current_trigger != last_trigger:
                    self.logger.info(f"Trigger changed from {last_trigger} to {current_trigger}")
                    callback_func(current_trigger, last_trigger)
                
                last_trigger = current_trigger
                time.sleep(poll_interval)
                
            except ModbusException as e:
                self.logger.error(f"Error monitoring trigger: {e}")
                # Try to reconnect
                try:
                    self.client.connect()
                except Exception as reconnect_error:
                    self.logger.error(f"Failed to reconnect: {reconnect_error}")
                    time.sleep(poll_interval * 5)  # Wait longer before retrying
            except KeyboardInterrupt:
                self.logger.info("Trigger monitoring stopped by user")
                break
            except Exception as e:
                self.logger.error(f"Unexpected error monitoring trigger: {e}")
                time.sleep(poll_interval)


class ModbusClientFactory:
    """Factory for creating configured Modbus clients"""
    
    @staticmethod
    def create_client(config: ModbusConfig) -> ModbusClient:
        """Create a new Modbus client with the given configuration"""
        return ModbusClient(config)
    
    @staticmethod
    def create_plc_manager(config: ModbusConfig) -> PLCRegisterManager:
        """Create a PLC register manager with configured client"""
        client = ModbusClient(config)
        return PLCRegisterManager(client)
    
    @staticmethod
    def create_connected_client(config: ModbusConfig) -> ModbusClient:
        """Create and connect a Modbus client"""
        client = ModbusClient(config)
        client.connect()
        return client