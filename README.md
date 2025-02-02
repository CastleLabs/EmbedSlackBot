# Embed Swiper Monitor for Castle Fun Center

## Overview
This Python script monitors an MS SQL Server database for "embed swiper offline" events in the arcade at Castle Fun Center. When a card swiper is placed offline, the script sends real-time notifications to a specified Slack channel. The application is designed to run on a Raspberry Pi using the FreeTDS ODBC driver via unixODBC.

## Features
- Real-time monitoring of embed swiper offline events
- Slack notifications with detailed event information
- Robust error handling and retry mechanisms
- Concurrent notification processing
- Metric tracking and persistence
- Graceful shutdown handling
- Comprehensive logging system
- Health check monitoring
- Configuration validation

## Prerequisites

### System Requirements
- Python 3.9+
- Raspberry Pi (recommended) or any Linux system
- FreeTDS ODBC driver
- unixODBC

### Required Python Packages
```
pyodbc
slack_sdk
```

### Database Requirements
- MS SQL Server instance
- Database user with read access to the following tables:
  - ecs7.dbo.game_swipers
  - ecs7.dbo.game_log
  - ecs7.dbo.game_events
  - ecs7.dbo.users
  - ecs7.dbo.swiper_units

### Slack Requirements
- Slack workspace
- Bot token with the following scopes:
  - chat:write
  - chat:write.public

## Installation

1. Install system dependencies:
```bash
sudo apt-get update
sudo apt-get install -y python3-pip unixodbc-dev freetds-dev freetds-bin tdsodbc
```

2. Clone the repository:
```bash
git clone <repository-url>
cd embed-swiper-monitor
```

3. Install Python dependencies:
```bash
pip3 install -r requirements.txt
```

4. Create the configuration file:
```bash
cp config.ini.example config.ini
```

## Configuration

### config.ini Structure
```ini
[GENERAL]
POLL_INTERVAL = 60

[DATABASE]
DRIVER = FreeTDS
SERVER = your_server_address
PORT = 1433
DATABASE = your_database_name
UID = your_username
PWD = your_password
TDS_VERSION = 7.4

[SLACK]
BOT_TOKEN = xoxb-your-bot-token
CHANNEL = #your-channel-name
```

### Configuration Parameters

#### General Section
- `POLL_INTERVAL`: Time in seconds between database checks (default: 60)

#### Database Section
- `DRIVER`: ODBC driver name (FreeTDS recommended)
- `SERVER`: SQL Server hostname or IP address
- `PORT`: SQL Server port (default: 1433)
- `DATABASE`: Database name
- `UID`: Database username
- `PWD`: Database password
- `TDS_VERSION`: FreeTDS protocol version (7.4 recommended)

#### Slack Section
- `BOT_TOKEN`: Slack bot user OAuth token
- `CHANNEL`: Slack channel for notifications (include # for public channels)

## Usage

### Starting the Monitor
```bash
python3 swiper_monitor.py
```

### Running as a Service
1. Create a systemd service file:
```bash
sudo nano /etc/systemd/system/swiper-monitor.service
```

2. Add the following content:
```ini
[Unit]
Description=Embed Swiper Monitor
After=network.target

[Service]
Type=simple
User=your_user
WorkingDirectory=/path/to/script/directory
ExecStart=/usr/bin/python3 /path/to/script/swiper_monitor.py
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

3. Enable and start the service:
```bash
sudo systemctl enable swiper-monitor
sudo systemctl start swiper-monitor
```

## Monitoring and Maintenance

### Logs
- Location: `logs/swiper_monitor.log`
- Format: `timestamp - level - message`
- Log levels: INFO, WARNING, ERROR

### Metrics
- Location: `monitor_metrics.json`
- Tracked metrics:
  - notifications_sent
  - failed_notifications
  - db_connection_attempts
  - db_connection_failures
  - last_successful_check
  - timestamp

### Health Checks
The script performs regular health checks on:
- Database connectivity
- Query execution
- Slack API connection

### Slack Notifications
Each notification includes:
- Game/swiper description
- User who marked the swiper offline
- Number of days offline
- Log timestamp
- Comment/reason for offline status

## Error Handling

### Database Connections
- Maximum retry attempts: 3
- Retry delay: 5 seconds
- Connection pooling for efficient resource usage

### Slack Notifications
- Maximum retry attempts: 3
- Retry delay: 5 seconds
- Concurrent processing via ThreadPool

### Graceful Shutdown
The script handles the following signals:
- SIGTERM
- SIGINT (Ctrl+C)

## Troubleshooting

### Common Issues

1. Database Connection Failures
```
Error: [SQL Server]Login failed for user...
Solution: Verify database credentials and permissions
```

2. ODBC Driver Issues
```
Error: [unixODBC]Driver not found
Solution: Verify FreeTDS installation and ODBC configuration
```

3. Slack API Errors
```
Error: invalid_auth
Solution: Verify Slack bot token and permissions
```

### Debug Mode
To enable debug logging:
1. Modify the logging level in `setup_logging()`:
```python
logger.setLevel(logging.DEBUG)
```

## Architecture

### Components
1. **Configuration Management**
   - Config file parsing
   - Validation of required parameters
   - Type checking

2. **Database Interface**
   - Connection management
   - Query execution
   - Error handling

3. **Notification System**
   - Slack message formatting
   - Retry mechanism
   - Concurrent processing

4. **Monitoring**
   - Health checks
   - Metrics collection
   - Logging

### Data Flow
1. Script initializes and validates configuration
2. Establishes database connection
3. Polls for new offline events
4. Formats and sends notifications
5. Updates metrics and logs
6. Repeats from step 3

## Security Considerations

### Database Security
- Use encrypted connections
- Implement least-privilege access
- Regularly rotate credentials
- Never store credentials in version control

### Slack Security
- Use bot tokens with minimal required permissions
- Rotate tokens periodically
- Verify channel permissions

## Contributing
Please follow these steps for contributions:
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## Author
Seth Morrow

## Version History
- 1.0.0 (2025-02-01): Initial release
