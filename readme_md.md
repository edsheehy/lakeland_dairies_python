# Lakeland Dairies Batch Processing System

A robust, modular system for managing batch data flow from Firebase to Zanasi printers via PLC control for milk powder production lines.

## Overview

This system orchestrates the flow of batch information for milk powder bag printing:

1. **Supervisors** enter batch details via Flutter web app → Firebase Cloud Firestore
2. **Operators** press "Download New Batch" on HMI → triggers PLC
3. **Raspberry Pi** monitors PLC, downloads from Firebase, and writes to PLC registers
4. **PLC** displays batch info on HMI for operator review
5. **Operators** select batch and press "Load to Zanasi" → triggers Pi to send data to printers
6. **Zanasi printers** receive batch data and print on milk powder bags

## Architecture

```
Firebase ←→ Raspberry Pi ←→ PLC ←→ HMI
                ↓
         Zanasi Printers (2x)
```

### Key Features

- **Intelligent Batch Mapping**: Preserves batch status and print counts during updates
- **Dual Printhead Support**: Sends data to both Zanasi printheads simultaneously  
- **Robust Error Handling**: Comprehensive exception handling with retry logic
- **Service Management**: Runs as system daemon with restart capability
- **Structured Logging**: Detailed logging with rotation and levels
- **Configuration Management**: JSON-based config with environment variable overrides
- **Modular Design**: Clean separation of concerns for maintainability

## Installation

### Prerequisites

- Python 3.8+
- Network access to PLC (Modbus TCP)
- Network access to Firebase Cloud Firestore
- Network access to Zanasi printers (TCP/IP)

### Setup

1. **Clone or download the system files**
```bash
# Create directory structure
mkdir -p /opt/lakeland_batch_system
cd /opt/lakeland_batch_system

# Copy all system files to this directory
```

2. **Install Python dependencies**
```bash
pip install -r requirements.txt
```

3. **Create configuration**
```bash
python main.py --create-config /etc/lakeland_batch_system/config.json
```

4. **Edit configuration file**
```bash
sudo nano /etc/lakeland_batch_system/config.json
```

Update network addresses and settings:
```json
{
  "modbus": {
    "host": "10.100.1.20",
    "port": 502
  },
  "zanasi": {
    "host": "10.100.1.10",
    "printhead1_port": 43110,
    "printhead2_port": 43111
  },
  "firebase": {
    "url": "https://your-firebase-function-url"
  }
}
```

5. **Test configuration**
```bash
python main.py --test-config --config /etc/lakeland_batch_system/config.json
```

## Usage

### Running as Foreground Service (Development)

```bash
# Start in foreground with console output
python main.py start --config /etc/lakeland_batch_system/config.json

# Test connections only
python main.py --test-config

# Check system status  
python main.py status
```

### Running as System Service (Production)

```bash
# Start as daemon
python main.py start --daemon --config /etc/lakeland_batch_system/config.json

# Stop service
python main.py stop

# Restart service
python main.py restart

# Check status
python main.py status
```

### Creating Systemd Service (Linux)

Create `/etc/systemd/system/lakeland-batch.service`:

```ini
[Unit]
Description=Lakeland Dairies Batch Processing System
After=network.target

[Service]
Type=forking
User=pi
Group=pi
WorkingDirectory=/opt/lakeland_batch_system
ExecStart=/usr/bin/python3 /opt/lakeland_batch_system/main.py start --daemon
ExecStop=/usr/bin/python3 /opt/lakeland_batch_system/main.py stop
PIDFile=/var/run/lakeland_batch_processor.pid
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl enable lakeland-batch
sudo systemctl start lakeland-batch
sudo systemctl status lakeland-batch
```

## Configuration

### Configuration File Locations

The system searches for configuration in this order:
1. `./config/lakeland_batch_config.json`
2. `/etc/lakeland_batch_system/config.json`  
3. `~/lakeland_batch_config.json`

### Environment Variable Overrides

Key settings can be overridden with environment variables:

```bash
export MODBUS_HOST=10.100.1.20
export ZANASI_HOST=10.100.1.10
export FIREBASE_URL=https://your-firebase-url
export LOG_LEVEL=DEBUG
```

### Configuration Sections

- **modbus**: PLC connection settings
- **zanasi**: Printer connection settings  
- **firebase**: Cloud database settings
- **processing**: Batch processing parameters
- **logging**: Log levels, files, and rotation
- **service**: Daemon and PID file settings

## System Operation

### Batch Download Process

1. Operator presses "Download New Batch" on HMI
2. PLC sets trigger register (DB[1] = 1)
3. Raspberry Pi detects trigger and:
   - Downloads latest batches from Firebase
   - Reads current PLC batch data
   - Intelligently maps batches to preserve status/counts
   - Converts to register format and writes to PLC
   - Updates status registers

### Batch Load Process  

1. Operator selects batch (1-5) and presses "Load to Zanasi"
2. PLC sets load trigger (DB[1] = 2) and batch selection (DB[7])
3. Raspberry Pi:
   - Reads selected batch data from PLC
   - Validates data for Zanasi protocol
   - Sends to both printheads simultaneously
   - Updates completion status

### Status Management

The system uses multiple status registers for coordination:

- **DB[1] TRIGGER**: Operation requests (0=idle, 1=download, 2=load)
- **DB[2] RASP_PI_STATUS**: Pi processing state (0-5, 9=error)
- **DB[3] PLC_STATUS**: PLC internal state (0-5)
- **DB[4] ZANASI_STATUS**: Printer communication status
- **DB[5] ERROR_CODE**: Error types (0=none, 1=Firebase, 2=Zanasi, 3=data)
- **DB[7] SELECTED_BATCH**: Batch selection for Zanasi load (1-5)

## Data Format

### Batch Data Structure

Each batch contains:
- **batchIndex**: Unique identifier (1001-99999)
- **status**: Processing status (0-4, see BatchStates)
- **printCount**: Number of bags printed (0-65535)
- **batchCode**: Batch identification code (5 chars max)
- **dryerCode**: Dryer identification code (5 chars max)  
- **productionDate**: Production date (10 chars max)
- **expiryDate**: Expiry date (10 chars max)

### PLC Register Mapping

- **Registers 1-9**: Control and status
- **Registers 10-29**: Batch 1 (20 registers)
- **Registers 30-49**: Batch 2 (20 registers)
- **Registers 50-69**: Batch 3 (20 registers)
- **Registers 70-89**: Batch 4 (20 registers)
- **Registers 90-109**: Batch 5 (20 registers)
- **Registers 110-120**: Reserved for expansion

## Monitoring and Troubleshooting

### Log Files

Default log location: `~/logs/batch_processor.log`

Log levels available: DEBUG, INFO, WARNING, ERROR, CRITICAL

### Common Issues

**Connection Errors**:
```bash
# Test individual connections
python -c "from communication.modbus_client import ModbusClientFactory; from config.settings import Settings; client = ModbusClientFactory.create_client(Settings().modbus); client.connect()"
```

**Configuration Issues**:
```bash
# Validate configuration
python main.py --test-config --config your_config.json
```

**Service Status**:
```bash
# Check detailed status
python main.py status

# Check logs
tail -f ~/logs/batch_processor.log
```

### Status Monitoring

The system provides comprehensive status information:

```python
# Get system status programmatically
from batch_processor import BatchProcessor
processor = BatchProcessor()
status = processor.get_system_status()
```

## Development

### Project Structure

```
lakeland_batch_system/
├── main.py                    # Service entry point
├── batch_processor.py         # Main orchestrator
├── requirements.txt           # Dependencies
├── README.md                  # Documentation
├── config/
│   └── settings.py           # Configuration management
├── core/
│   ├── enums.py              # System enumerations
│   ├── registers.py          # PLC register mapping
│   └── exceptions.py         # Custom exceptions
├── communication/
│   ├── modbus_client.py      # PLC communication
│   ├── firebase_client.py    # Cloud database client
│   └── zanasi_client.py      # Printer communication
└── processing/
    ├── status_manager.py     # Status coordination
    ├── batch_manager.py      # Batch orchestration
    └── data_parser.py        # Data validation/conversion
```

### Adding Features

1. **New Communication Protocols**: Add clients in `communication/`
2. **Additional Validation**: Extend `DataParser` class
3. **Enhanced Monitoring**: Extend `StatusManager` capabilities
4. **Custom Error Handling**: Add exception types in `core/exceptions.py`

### Testing

```bash
# Run unit tests (when available)
pytest tests/

# Test specific components
python -m communication.modbus_client  # Test Modbus
python -m communication.firebase_client  # Test Firebase  
python -m communication.zanasi_client   # Test Zanasi
```

## Version History

- **v18.0**: Complete modular rewrite with enhanced error handling and service management
- **v17.0**: Intelligent batch mapping with status preservation
- **Previous versions**: Monolithic implementation

## License

Proprietary software for Lakeland Dairies. All rights reserved.

## Support

For technical support, check:
1. Log files for error details
2. Network connectivity to all systems
3. Configuration file validity
4. PLC register accessibility

Contact system integrator for advanced troubleshooting.
