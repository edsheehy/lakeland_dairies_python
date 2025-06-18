#!/usr/bin/env python3
"""
Batch management orchestrator for Lakeland Dairies Batch Processing System
"""

import time
import logging
from typing import Dict, Any, List, Optional, Tuple

from ..communication.modbus_client import ModbusClient
from ..communication.firebase_client import FirebaseClient
from ..communication.zanasi_client import ZanasiClient
from ..processing.status_manager import StatusManager
from ..processing.data_parser import DataParser
from ..core.enums import ProcessingStates, PLCStates, ErrorCodes, BatchStates
from ..core.exceptions import (BatchProcessingException, DataValidationException, 
                              FirebaseException, ZanasiException, ModbusException)


class BatchManager:
    """Main orchestrator for batch processing operations"""
    
    def __init__(self, modbus_client: ModbusClient, firebase_client: FirebaseClient,
                 zanasi_client: ZanasiClient, status_manager: StatusManager, 
                 data_parser: DataParser):
        self.modbus_client = modbus_client
        self.firebase_client = firebase_client
        self.zanasi_client = zanasi_client
        self.status_manager = status_manager
        self.data_parser = data_parser
        
        self.logger = logging.getLogger(f"{__name__}.BatchManager")
        
        # Operation tracking
        self.current_batch_data = []
        self.last_operation_time = None
        self.operation_count = 0
        self.last_firebase_fetch = None
        self.last_zanasi_send = None
    
    def process_download_batch_trigger(self) -> bool:
        """
        Handle download batch data trigger with intelligent batch mapping
        
        Returns:
            True if successful
            
        Raises:
            BatchProcessingException: On processing errors
        """
        operation_start = time.time()
        self.logger.info("Processing download batch trigger with intelligent mapping...")
        
        try:
            # Set initial status
            self.status_manager.set_processing_status(ProcessingStates.DOWNLOADING)
            self.status_manager.set_plc_status(PLCStates.TRIGGERING_DOWNLOAD)
            
            # Read current PLC state
            current_plc_batches = self._read_current_plc_batches()
            self.logger.info(f"Current PLC state: {len([b for b in current_plc_batches if b and b['batchIndex'] > 0])} active batches")
            
            # Fetch data from Firebase
            self.status_manager.set_processing_status(ProcessingStates.DOWNLOADING)
            firebase_data = self._fetch_firebase_data()
            
            # Parse and validate Firebase data
            self.status_manager.set_processing_status(ProcessingStates.PROCESSING_DATA)
            firebase_batches = self._parse_firebase_data(firebase_data)
            
            # Map Firebase batches to PLC positions intelligently
            mapped_batches = self._map_batches_to_plc_positions(firebase_batches, current_plc_batches)
            
            # Convert to register format and write to PLC
            register_array = self._convert_and_write_batch_data(mapped_batches)
            
            # Update status to completion
            self.status_manager.set_plc_status(PLCStates.DATA_RECEIVED)
            self.status_manager.set_processing_status(ProcessingStates.READY_TO_SEND)
            
            # Reset trigger and update PLC status
            self.status_manager.reset_trigger()
            self.status_manager.set_plc_status(PLCStates.DISPLAYING)
            
            # Update tracking
            self.current_batch_data = mapped_batches
            self.last_operation_time = time.time() - operation_start
            self.operation_count += 1
            
            self.logger.info(f"Download batch process completed successfully in {self.last_operation_time:.2f}s")
            return True
            
        except FirebaseException as e:
            self.logger.error(f"Firebase error during download: {e}")
            self.status_manager.set_error_code(ErrorCodes.FIREBASE_FAIL)
            self.status_manager.set_processing_status(ProcessingStates.ERROR)
            raise BatchProcessingException(
                f"Firebase download failed: {e}",
                operation="download_batch"
            ) from e
            
        except DataValidationException as e:
            self.logger.error(f"Data validation error during download: {e}")
            self.status_manager.set_error_code(ErrorCodes.DATA_FORMAT_ERROR)
            self.status_manager.set_processing_status(ProcessingStates.ERROR)
            raise BatchProcessingException(
                f"Data validation failed: {e}",
                operation="download_batch"
            ) from e
            
        except ModbusException as e:
            self.logger.error(f"PLC communication error during download: {e}")
            self.status_manager.set_error_code(ErrorCodes.DATA_FORMAT_ERROR)
            self.status_manager.set_processing_status(ProcessingStates.ERROR)
            raise BatchProcessingException(
                f"PLC communication failed: {e}",
                operation="download_batch"
            ) from e
            
        except Exception as e:
            self.logger.error(f"Unexpected error during download: {e}")
            self.status_manager.set_error_code(ErrorCodes.DATA_FORMAT_ERROR)
            self.status_manager.set_processing_status(ProcessingStates.ERROR)
            raise BatchProcessingException(
                f"Download operation failed: {e}",
                operation="download_batch"
            ) from e
    
    def process_load_to_zanasi_trigger(self) -> bool:
        """
        Handle load to Zanasi with batch selection
        
        Returns:
            True if successful
            
        Raises:
            BatchProcessingException: On processing errors
        """
        operation_start = time.time()
        self.logger.info("Processing load to Zanasi trigger...")
        
        try:
            # Read selected batch number from PLC
            selected_batch_number = self.status_manager.get_selected_batch()
            if not (1 <= selected_batch_number <= 5):
                raise BatchProcessingException(
                    f"Invalid selected batch number: {selected_batch_number}",
                    operation="load_to_zanasi"
                )
            
            self.logger.info(f"Loading batch number {selected_batch_number} to Zanasi")
            
            # Read selected batch data from PLC
            batch_data = self._read_batch_from_plc(selected_batch_number)
            if not batch_data or batch_data['batchIndex'] == 0:
                raise BatchProcessingException(
                    f"No valid batch data found for batch {selected_batch_number}",
                    batch_index=batch_data.get('batchIndex') if batch_data else None,
                    operation="load_to_zanasi"
                )
            
            # Validate data for Zanasi
            self._validate_batch_for_zanasi(batch_data)
            
            # Set status to sending
            self.status_manager.set_processing_status(ProcessingStates.SENDING_TO_ZANASI)
            
            # Send to both printheads
            success = self._send_batch_to_zanasi(batch_data)
            
            if success:
                # Success - mark complete
                self.status_manager.set_processing_status(ProcessingStates.COMPLETE)
                self.status_manager.reset_trigger()
                
                # Update tracking
                self.last_zanasi_send = {
                    'batch_index': batch_data['batchIndex'],
                    'timestamp': time.time(),
                    'selected_position': selected_batch_number
                }
                self.last_operation_time = time.time() - operation_start
                self.operation_count += 1
                
                self.logger.info(f"Successfully loaded batch {selected_batch_number} (Index: {batch_data['batchIndex']}) to Zanasi in {self.last_operation_time:.2f}s")
                return True
            else:
                raise BatchProcessingException(
                    f"Failed to send batch to Zanasi printheads",
                    batch_index=batch_data['batchIndex'],
                    operation="load_to_zanasi"
                )
                
        except ZanasiException as e:
            self.logger.error(f"Zanasi communication error: {e}")
            self.status_manager.set_error_code(ErrorCodes.ZANASI_COMM_FAIL)
            self.status_manager.set_processing_status(ProcessingStates.ERROR)
            raise BatchProcessingException(
                f"Zanasi communication failed: {e}",
                operation="load_to_zanasi"
            ) from e
            
        except DataValidationException as e:
            self.logger.error(f"Batch validation error for Zanasi: {e}")
            self.status_manager.set_error_code(ErrorCodes.DATA_FORMAT_ERROR)
            self.status_manager.set_processing_status(ProcessingStates.ERROR)
            raise BatchProcessingException(
                f"Batch validation failed: {e}",
                operation="load_to_zanasi"
            ) from e
            
        except Exception as e:
            self.logger.error(f"Unexpected error during Zanasi load: {e}")
            self.status_manager.set_error_code(ErrorCodes.ZANASI_COMM_FAIL)
            self.status_manager.set_processing_status(ProcessingStates.ERROR)
            raise BatchProcessingException(
                f"Zanasi load operation failed: {e}",
                operation="load_to_zanasi"
            ) from e
    
    def _read_current_plc_batches(self) -> List[Optional[Dict[str, Any]]]:
        """Read all current batch data from PLC"""
        try:
            # Read complete register array
            register_array = self.modbus_client.read_holding_register(1, 120)
            
            # Extract batches from registers
            batches = self.data_parser.extract_batches_from_registers(register_array)
            
            self.logger.debug(f"Read {len([b for b in batches if b])} active batches from PLC")
            return batches
            
        except Exception as e:
            self.logger.error(f"Error reading current PLC batches: {e}")
            raise
    
    def _fetch_firebase_data(self) -> List[Dict[str, Any]]:
        """Fetch batch data from Firebase"""
        try:
            firebase_data = self.firebase_client.fetch_batch_data()
            self.last_firebase_fetch = time.time()
            
            if not firebase_data:
                self.logger.warning("No batch data returned from Firebase")
                return []
            
            self.logger.info(f"Fetched {len(firebase_data)} batch entries from Firebase")
            return firebase_data
            
        except Exception as e:
            self.logger.error(f"Error fetching Firebase data: {e}")
            raise
    
    def _parse_firebase_data(self, firebase_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Parse and validate Firebase data"""
        try:
            parsed_batches = self.data_parser.parse_firebase_data(firebase_data)
            
            # Log batch summary
            for i, batch in enumerate(parsed_batches):
                summary = self.data_parser.get_batch_summary_for_logging(batch)
                self.logger.info(f"  Firebase batch {i+1}: {summary}")
            
            return parsed_batches
            
        except Exception as e:
            self.logger.error(f"Error parsing Firebase data: {e}")
            raise
    
    def _map_batches_to_plc_positions(self, firebase_batches: List[Dict[str, Any]], 
                                     current_plc_batches: List[Optional[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Map Firebase batches to PLC positions intelligently"""
        try:
            # Convert None entries to empty batch dictionaries for processing
            plc_batches_for_mapping = []
            for batch in current_plc_batches:
                if batch is None:
                    plc_batches_for_mapping.append(self.data_parser._create_empty_batch())
                else:
                    plc_batches_for_mapping.append(batch)
            
            mapped_batches = self.data_parser.map_firebase_to_plc_positions(
                firebase_batches, plc_batches_for_mapping
            )
            
            # Log mapping results
            for i, batch in enumerate(mapped_batches):
                if batch['batchIndex'] > 0:
                    summary = self.data_parser.get_batch_summary_for_logging(batch)
                    self.logger.info(f"  Position {i+1}: {summary}")
                else:
                    self.logger.info(f"  Position {i+1}: Empty")
            
            return mapped_batches
            
        except Exception as e:
            self.logger.error(f"Error mapping batches to PLC positions: {e}")
            raise
    
    def _convert_and_write_batch_data(self, mapped_batches: List[Dict[str, Any]]) -> List[int]:
        """Convert batch data to registers and write to PLC"""
        try:
            # Convert to register format
            register_array = self.data_parser.convert_batches_to_registers(mapped_batches)
            
            # Write to PLC
            success = self.modbus_client.write_holding_registers(1, register_array)
            if not success:
                raise BatchProcessingException(
                    "Failed to write batch data to PLC",
                    operation="write_registers"
                )
            
            self.logger.info(f"Successfully wrote {len(mapped_batches)} mapped batches to PLC (120 registers)")
            return register_array
            
        except Exception as e:
            self.logger.error(f"Error converting and writing batch data: {e}")
            raise
    
    def _read_batch_from_plc(self, batch_number: int) -> Optional[Dict[str, Any]]:
        """Read specific batch data from PLC"""
        try:
            # Read registers for this batch (20 registers)
            start_register = 10 + (batch_number - 1) * 20
            batch_registers = self.modbus_client.read_holding_register(start_register, 20)
            
            # Extract batch data
            register_array = [0] * 120  # Create dummy full array
            # Fill in the batch registers at the correct position
            for i, reg_val in enumerate(batch_registers):
                register_array[start_register - 1 + i] = reg_val
            
            batch_data = self.data_parser.register_builder.extract_batch_from_registers(
                register_array, batch_number
            )
            
            if batch_data:
                summary = self.data_parser.get_batch_summary_for_logging(batch_data)
                self.logger.info(f"Read from PLC - {summary}")
            
            return batch_data
            
        except Exception as e:
            self.logger.error(f"Error reading batch {batch_number} from PLC: {e}")
            raise
    
    def _validate_batch_for_zanasi(self, batch_data: Dict[str, Any]):
        """Validate batch data for Zanasi transmission"""
        is_valid, errors = self.data_parser.validate_zanasi_data(batch_data)
        if not is_valid:
            raise DataValidationException(
                f"Batch data invalid for Zanasi transmission",
                validation_errors=errors,
                field="zanasi_validation"
            )
    
    def _send_batch_to_zanasi(self, batch_data: Dict[str, Any]) -> bool:
        """Send batch data to both Zanasi printheads"""
        try:
            # Sanitize data for Zanasi protocol
            sanitized_data = self.data_parser.sanitize_for_zanasi(batch_data)
            
            # Send to both printheads
            success, results = self.zanasi_client.send_batch_to_both_printheads(sanitized_data)
            
            # Log detailed results
            ph1_status = "✓" if results['printhead1']['success'] else "✗"
            ph2_status = "✓" if results['printhead2']['success'] else "✗"
            self.logger.info(f"Zanasi send results - PH1: {ph1_status}, PH2: {ph2_status}")
            
            if not success:
                error_details = []
                if results['printhead1']['error']:
                    error_details.append(f"PH1: {results['printhead1']['error']}")
                if results['printhead2']['error']:
                    error_details.append(f"PH2: {results['printhead2']['error']}")
                
                raise ZanasiException(
                    f"Zanasi send failed - {'; '.join(error_details)}",
                    details=results
                )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error sending batch to Zanasi: {e}")
            raise
    
    def get_status_summary(self) -> Dict[str, Any]:
        """Get comprehensive batch manager status"""
        return {
            'operation_count': self.operation_count,
            'last_operation_time': self.last_operation_time,
            'current_batch_count': len([b for b in self.current_batch_data if b.get('batchIndex', 0) > 0]) if self.current_batch_data else 0,
            'last_firebase_fetch': self.last_firebase_fetch,
            'last_zanasi_send': self.last_zanasi_send,
            'current_batches': [
                self.data_parser.get_batch_summary_for_logging(batch) 
                for batch in self.current_batch_data 
                if batch.get('batchIndex', 0) > 0
            ] if self.current_batch_data else []
        }
    
    def get_current_batch_details(self) -> List[Dict[str, Any]]:
        """Get detailed information about current batches"""
        if not self.current_batch_data:
            return []
        
        detailed_batches = []
        for i, batch in enumerate(self.current_batch_data):
            if batch.get('batchIndex', 0) > 0:
                batch_detail = batch.copy()
                batch_detail['plc_position'] = i + 1
                batch_detail['status_name'] = BatchStates(batch['status']).name
                detailed_batches.append(batch_detail)
        
        return detailed_batches
    
    def force_refresh_from_plc(self) -> List[Dict[str, Any]]:
        """Force refresh of batch data from PLC"""
        try:
            self.logger.info("Force refreshing batch data from PLC")
            current_batches = self._read_current_plc_batches()
            
            # Update current data (filter out None values)
            self.current_batch_data = [
                batch if batch else self.data_parser._create_empty_batch()
                for batch in current_batches
            ]
            
            active_count = len([b for b in self.current_batch_data if b['batchIndex'] > 0])
            self.logger.info(f"Refreshed {active_count} active batches from PLC")
            
            return self.get_current_batch_details()
            
        except Exception as e:
            self.logger.error(f"Error force refreshing from PLC: {e}")
            raise BatchProcessingException(
                f"Failed to refresh batch data from PLC: {e}",
                operation="force_refresh"
            ) from e
