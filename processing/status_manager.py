#!/usr/bin/env python3
"""
Status management for Lakeland Dairies Batch Processing System
"""

import logging
from typing import Dict, Any, Optional
from communication.modbus_client import ModbusClient
from core.enums import (TriggerStates, ProcessingStates, PLCStates,
                         ErrorCodes, SystemComponent)
from core.registers import PLCRegisters
from core.exceptions import ModbusException, StateException


class StatusManager:
    """Manages system status across PLC registers and internal state"""
    
    def __init__(self, modbus_client: ModbusClient):
        self.modbus_client = modbus_client
        self.logger = logging.getLogger(f"{__name__}.StatusManager")
        
        # Internal state tracking
        self.current_processing_state = ProcessingStates.IDLE
        self.current_plc_state = PLCStates.IDLE
        self.current_trigger = TriggerStates.IDLE
        self.current_error_code = ErrorCodes.NO_ERROR
        self.selected_batch = 0
        
        # State history for debugging
        self.state_history = []
        self.max_history_length = 50
    
    def read_all_status_registers(self) -> Dict[str, int]:
        """
        Read all status registers from PLC
        
        Returns:
            Dictionary with current register values
        """
        try:
            status_registers = {}
            
            # Read control registers
            status_registers['trigger'] = self.modbus_client.read_holding_register(PLCRegisters.TRIGGER)
            status_registers['rasp_pi_status'] = self.modbus_client.read_holding_register(PLCRegisters.RASP_PI_STATUS)
            status_registers['plc_status'] = self.modbus_client.read_holding_register(PLCRegisters.PLC_STATUS)
            status_registers['zanasi_status'] = self.modbus_client.read_holding_register(PLCRegisters.ZANASI_STATUS)
            status_registers['error_code'] = self.modbus_client.read_holding_register(PLCRegisters.ERROR_CODE)
            status_registers['selected_batch'] = self.modbus_client.read_holding_register(PLCRegisters.SELECTED_BATCH)
            
            # Update internal state
            self._update_internal_state(status_registers)
            
            self.logger.debug(f"Read status registers: {status_registers}")
            return status_registers
            
        except ModbusException as e:
            self.logger.error(f"Error reading status registers: {e}")
            raise
    
    def _update_internal_state(self, status_registers: Dict[str, int]):
        """Update internal state tracking from register values"""
        old_state = {
            'processing': self.current_processing_state,
            'plc': self.current_plc_state,
            'trigger': self.current_trigger,
            'error': self.current_error_code
        }
        
        # Update current state
        self.current_trigger = TriggerStates(status_registers['trigger'])
        self.current_processing_state = ProcessingStates(status_registers['rasp_pi_status'])
        self.current_plc_state = PLCStates(status_registers['plc_status'])
        self.current_error_code = ErrorCodes(status_registers['error_code'])
        self.selected_batch = status_registers['selected_batch']
        
        # Check for state changes
        new_state = {
            'processing': self.current_processing_state,
            'plc': self.current_plc_state,
            'trigger': self.current_trigger,
            'error': self.current_error_code
        }
        
        if old_state != new_state:
            self._record_state_change(old_state, new_state)
    
    def _record_state_change(self, old_state: Dict, new_state: Dict):
        """Record state change in history"""
        import time
        
        change_record = {
            'timestamp': time.time(),
            'old_state': old_state.copy(),
            'new_state': new_state.copy()
        }
        
        self.state_history.append(change_record)
        
        # Limit history size
        if len(self.state_history) > self.max_history_length:
            self.state_history.pop(0)
        
        # Log significant changes
        for key in new_state:
            if old_state[key] != new_state[key]:
                self.logger.info(f"State change - {key}: {old_state[key]} -> {new_state[key]}")
    
    def set_processing_status(self, status: ProcessingStates) -> bool:
        """
        Update Raspberry Pi processing status register
        
        Args:
            status: New processing status
            
        Returns:
            True if successful
        """
        try:
            success = self.modbus_client.write_holding_register(PLCRegisters.RASP_PI_STATUS, status.value)
            if success:
                old_status = self.current_processing_state
                self.current_processing_state = status
                self.logger.info(f"Processing status updated: {old_status} -> {status}")
            return success
        except ModbusException as e:
            self.logger.error(f"Error setting processing status to {status}: {e}")
            raise
    
    def set_plc_status(self, status: PLCStates) -> bool:
        """
        Update PLC status register
        
        Args:
            status: New PLC status
            
        Returns:
            True if successful
        """
        try:
            success = self.modbus_client.write_holding_register(PLCRegisters.PLC_STATUS, status.value)
            if success:
                old_status = self.current_plc_state
                self.current_plc_state = status
                self.logger.info(f"PLC status updated: {old_status} -> {status}")
            return success
        except ModbusException as e:
            self.logger.error(f"Error setting PLC status to {status}: {e}")
            raise
    
    def set_error_code(self, error_code: ErrorCodes) -> bool:
        """
        Set error code register
        
        Args:
            error_code: Error code to set
            
        Returns:
            True if successful
        """
        try:
            success = self.modbus_client.write_holding_register(PLCRegisters.ERROR_CODE, error_code.value)
            if success:
                old_error = self.current_error_code
                self.current_error_code = error_code
                if error_code != ErrorCodes.NO_ERROR:
                    self.logger.error(f"Error code set: {old_error} -> {error_code}")
                else:
                    self.logger.info(f"Error code cleared: {old_error} -> {error_code}")
            return success
        except ModbusException as e:
            self.logger.error(f"Error setting error code to {error_code}: {e}")
            raise
    
    def clear_error(self) -> bool:
        """Clear error code register"""
        return self.set_error_code(ErrorCodes.NO_ERROR)
    
    def reset_trigger(self) -> bool:
        """
        Reset trigger register to acknowledge completion
        
        Returns:
            True if successful
        """
        try:
            success = self.modbus_client.write_holding_register(PLCRegisters.TRIGGER, TriggerStates.IDLE.value)
            if success:
                old_trigger = self.current_trigger
                self.current_trigger = TriggerStates.IDLE
                self.logger.info(f"Trigger reset: {old_trigger} -> IDLE")
            return success
        except ModbusException as e:
            self.logger.error(f"Error resetting trigger: {e}")
            raise
    
    def get_current_trigger(self) -> TriggerStates:
        """
        Get current trigger state (with fresh read from PLC)
        
        Returns:
            Current trigger state
        """
        try:
            trigger_value = self.modbus_client.read_holding_register(PLCRegisters.TRIGGER)
            self.current_trigger = TriggerStates(trigger_value)
            return self.current_trigger
        except ModbusException as e:
            self.logger.error(f"Error reading trigger state: {e}")
            raise
    
    def get_selected_batch(self) -> int:
        """
        Get currently selected batch number from PLC
        
        Returns:
            Selected batch number (1-5)
        """
        try:
            batch_value = self.modbus_client.read_holding_register(PLCRegisters.SELECTED_BATCH)
            self.selected_batch = batch_value
            return batch_value
        except ModbusException as e:
            self.logger.error(f"Error reading selected batch: {e}")
            raise
    
    def validate_state_transition(self, from_state: ProcessingStates, to_state: ProcessingStates) -> bool:
        """
        Validate if state transition is allowed
        
        Args:
            from_state: Current state
            to_state: Desired state
            
        Returns:
            True if transition is valid
        """
        # Define valid state transitions
        valid_transitions = {
            ProcessingStates.IDLE: [ProcessingStates.DOWNLOADING, ProcessingStates.SENDING_TO_ZANASI],
            ProcessingStates.DOWNLOADING: [ProcessingStates.PROCESSING_DATA, ProcessingStates.ERROR],
            ProcessingStates.PROCESSING_DATA: [ProcessingStates.READY_TO_SEND, ProcessingStates.ERROR],
            ProcessingStates.READY_TO_SEND: [ProcessingStates.SENDING_TO_ZANASI, ProcessingStates.COMPLETE],
            ProcessingStates.SENDING_TO_ZANASI: [ProcessingStates.COMPLETE, ProcessingStates.ERROR],
            ProcessingStates.COMPLETE: [ProcessingStates.IDLE],
            ProcessingStates.ERROR: [ProcessingStates.IDLE]
        }
        
        allowed_states = valid_transitions.get(from_state, [])
        return to_state in allowed_states
    
    def transition_to_state(self, new_state: ProcessingStates, force: bool = False) -> bool:
        """
        Transition to new state with validation
        
        Args:
            new_state: Desired new state
            force: Skip validation if True
            
        Returns:
            True if successful
            
        Raises:
            StateException: If transition is invalid
        """
        if not force and not self.validate_state_transition(self.current_processing_state, new_state):
            raise StateException(
                f"Invalid state transition: {self.current_processing_state} -> {new_state}",
                current_state=self.current_processing_state,
                attempted_operation=f"transition_to_{new_state}"
            )
        
        return self.set_processing_status(new_state)
    
    def get_system_status_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive system status summary
        
        Returns:
            Dictionary with current system status
        """
        try:
            # Read fresh status from PLC
            status_registers = self.read_all_status_registers()
            
            return {
                'timestamp': time.time(),
                'trigger_state': self.current_trigger.name,
                'processing_state': self.current_processing_state.name,
                'plc_state': self.current_plc_state.name,
                'error_code': self.current_error_code.name,
                'selected_batch': self.selected_batch,
                'has_error': self.current_error_code != ErrorCodes.NO_ERROR,
                'is_processing': self.current_processing_state not in [ProcessingStates.IDLE, ProcessingStates.COMPLETE],
                'raw_registers': status_registers,
                'state_history_count': len(self.state_history)
            }
        except Exception as e:
            self.logger.error(f"Error getting system status: {e}")
            return {
                'timestamp': time.time(),
                'error': str(e),
                'last_known_state': {
                    'trigger': self.current_trigger.name,
                    'processing': self.current_processing_state.name,
                    'plc': self.current_plc_state.name,
                    'error': self.current_error_code.name
                }
            }
    
    def get_state_history(self, limit: Optional[int] = None) -> list:
        """
        Get state change history
        
        Args:
            limit: Maximum number of entries to return
            
        Returns:
            List of state change records
        """
        history = self.state_history.copy()
        if limit:
            history = history[-limit:]
        return history
    
    def is_system_ready(self) -> bool:
        """Check if system is ready for new operations"""
        return (self.current_processing_state == ProcessingStates.IDLE and
                self.current_error_code == ErrorCodes.NO_ERROR and
                self.current_trigger == TriggerStates.IDLE)
    
    def is_error_state(self) -> bool:
        """Check if system is in error state"""
        return (self.current_processing_state == ProcessingStates.ERROR or
                self.current_error_code != ErrorCodes.NO_ERROR)
    
    def reset_system_state(self) -> bool:
        """
        Reset system to idle state
        
        Returns:
            True if successful
        """
        try:
            self.logger.info("Resetting system state to idle")
            
            # Reset all status registers
            success = True
            success &= self.reset_trigger()
            success &= self.set_processing_status(ProcessingStates.IDLE)
            success &= self.set_plc_status(PLCStates.IDLE)
            success &= self.clear_error()
            
            if success:
                self.logger.info("System state reset completed successfully")
            else:
                self.logger.error("System state reset completed with errors")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error resetting system state: {e}")
            return False


class StatusMonitor:
    """Background monitor for status changes"""
    
    def __init__(self, status_manager: StatusManager, poll_interval: float = 1.0):
        self.status_manager = status_manager
        self.poll_interval = poll_interval
        self.logger = logging.getLogger(f"{__name__}.StatusMonitor")
        self.callbacks = {}
        self.is_monitoring = False
        self.last_status = None
    
    def register_callback(self, event_type: str, callback_func):
        """
        Register callback for status events
        
        Args:
            event_type: Type of event ('trigger_change', 'error', 'state_change')
            callback_func: Function to call on event
        """
        if event_type not in self.callbacks:
            self.callbacks[event_type] = []
        self.callbacks[event_type].append(callback_func)
        self.logger.debug(f"Registered callback for {event_type}")
    
    def start_monitoring(self):
        """Start status monitoring loop"""
        import threading
        
        if self.is_monitoring:
            self.logger.warning("Monitoring already active")
            return
        
        self.is_monitoring = True
        monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        monitor_thread.start()
        self.logger.info("Status monitoring started")
    
    def stop_monitoring(self):
        """Stop status monitoring"""
        self.is_monitoring = False
        self.logger.info("Status monitoring stopped")
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        import time
        
        while self.is_monitoring:
            try:
                current_status = self.status_manager.get_system_status_summary()
                
                if self.last_status:
                    self._check_for_changes(self.last_status, current_status)
                
                self.last_status = current_status
                time.sleep(self.poll_interval)
                
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(self.poll_interval * 2)  # Wait longer on error
    
    def _check_for_changes(self, old_status: Dict, new_status: Dict):
        """Check for status changes and trigger callbacks"""
        # Check for trigger changes
        if old_status.get('trigger_state') != new_status.get('trigger_state'):
            self._trigger_callbacks('trigger_change', {
                'old_trigger': old_status.get('trigger_state'),
                'new_trigger': new_status.get('trigger_state')
            })
        
        # Check for error state changes
        if old_status.get('has_error') != new_status.get('has_error'):
            if new_status.get('has_error'):
                self._trigger_callbacks('error', {
                    'error_code': new_status.get('error_code'),
                    'status': new_status
                })
        
        # Check for processing state changes
        if old_status.get('processing_state') != new_status.get('processing_state'):
            self._trigger_callbacks('state_change', {
                'old_state': old_status.get('processing_state'),
                'new_state': new_status.get('processing_state')
            })
    
    def _trigger_callbacks(self, event_type: str, event_data: Dict):
        """Trigger registered callbacks for event type"""
        callbacks = self.callbacks.get(event_type, [])
        for callback in callbacks:
            try:
                callback(event_data)
            except Exception as e:
                self.logger.error(f"Error in callback for {event_type}: {e}")


class StatusReporter:
    """Generate status reports and metrics"""
    
    def __init__(self, status_manager: StatusManager):
        self.status_manager = status_manager
        self.logger = logging.getLogger(f"{__name__}.StatusReporter")
    
    def generate_status_report(self, include_history: bool = True) -> Dict[str, Any]:
        """Generate comprehensive status report"""
        import time
        
        report = {
            'report_timestamp': time.time(),
            'system_status': self.status_manager.get_system_status_summary(),
            'health_check': self._perform_health_check()
        }
        
        if include_history:
            report['state_history'] = self.status_manager.get_state_history(limit=10)
        
        return report
    
    def _perform_health_check(self) -> Dict[str, Any]:
        """Perform basic system health check"""
        health = {
            'overall_health': 'unknown',
            'issues': [],
            'recommendations': []
        }
        
        try:
            # Check if system is in error state
            if self.status_manager.is_error_state():
                health['issues'].append(f"System in error state: {self.status_manager.current_error_code.name}")
                health['recommendations'].append("Check error logs and reset system if needed")
            
            # Check for stuck states
            if self.status_manager.current_processing_state not in [ProcessingStates.IDLE, ProcessingStates.COMPLETE]:
                health['issues'].append(f"System may be stuck in {self.status_manager.current_processing_state.name}")
                health['recommendations'].append("Monitor for state progression or consider reset")
            
            # Check PLC connection
            try:
                self.status_manager.read_all_status_registers()
            except Exception as e:
                health['issues'].append(f"PLC communication error: {e}")
                health['recommendations'].append("Check PLC connection and network")
            
            # Determine overall health
            if not health['issues']:
                health['overall_health'] = 'good'
            elif len(health['issues']) == 1:
                health['overall_health'] = 'warning'
            else:
                health['overall_health'] = 'critical'
                
        except Exception as e:
            health['overall_health'] = 'error'
            health['issues'].append(f"Health check failed: {e}")
        
        return health
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get system metrics summary"""
        history = self.status_manager.get_state_history()
        
        if not history:
            return {'message': 'No metrics available'}
        
        # Calculate state durations and transitions
        state_counts = {}
        transition_counts = {}
        
        for record in history:
            new_state = record['new_state']['processing']
            state_counts[new_state.name] = state_counts.get(new_state.name, 0) + 1
            
            old_state = record['old_state']['processing']
            transition = f"{old_state.name} -> {new_state.name}"
            transition_counts[transition] = transition_counts.get(transition, 0) + 1
        
        return {
            'total_state_changes': len(history),
            'state_distribution': state_counts,
            'common_transitions': dict(sorted(transition_counts.items(), key=lambda x: x[1], reverse=True)[:5]),
            'recent_activity': len([r for r in history if time.time() - r['timestamp'] < 3600])  # Last hour
        }