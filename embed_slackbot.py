#!/usr/bin/env python3
"""
Project: Embed Swiper Monitor for Castle Fun Center
Description: This Python script monitors an MS SQL Server database for "embed swiper offline" events
             in the arcade at the Castle Fun Center. When a new event is detected (i.e. when a comment
             indicates "Swiper placed Offline"), the script sends a notification to a specified Slack channel.
             The script is designed to run on a Raspberry Pi using the FreeTDS ODBC driver (via unixODBC).
Author: Seth Morrow
Date: 2025-02-01
"""

import pyodbc
import time
import datetime
import configparser
import logging
import sys
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Optional, Dict, Any
import signal
import json
from pathlib import Path

# --- Dataclasses for Configuration and Metrics ---

@dataclass
class DatabaseConfig:
    driver: str
    server: str
    port: str
    database: str
    uid: str
    pwd: str
    tds_version: str

@dataclass
class SlackConfig:
    bot_token: str
    channel: str

@dataclass
class Metrics:
    notifications_sent: int = 0
    failed_notifications: int = 0
    db_connection_attempts: int = 0
    db_connection_failures: int = 0
    last_successful_check: Optional[datetime.datetime] = None

# Global metrics instance and shutdown flag.
metrics = Metrics()
shutdown_flag = False

# --- Logging Setup ---

def setup_logging(log_file: Optional[str] = None) -> logging.Logger:
    """
    Configures and returns a logger to output debug, info, and error messages.
    
    Args:
        log_file (Optional[str]): Path to a file to log messages, if provided.
        
    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger('SwipeMonitor')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger

# --- Configuration Validation and Reading ---

def validate_config(config: configparser.ConfigParser) -> tuple[DatabaseConfig, SlackConfig]:
    """
    Validates that all required configuration parameters exist in the config file.
    
    Args:
        config (configparser.ConfigParser): Parsed configuration data.
        
    Returns:
        tuple: A tuple containing DatabaseConfig and SlackConfig objects.
    
    Raises:
        ValueError: If a required configuration parameter is missing.
    """
    required_db_params = {
        'DRIVER': str,
        'SERVER': str,
        'PORT': str,
        'DATABASE': str,
        'UID': str,
        'PWD': str,
        'TDS_VERSION': str
    }
    
    required_slack_params = {
        'BOT_TOKEN': str,
        'CHANNEL': str
    }
    
    db_config = {}
    for param, param_type in required_db_params.items():
        value = config.get('DATABASE', param, fallback=None)
        if value is None:
            raise ValueError(f"Missing required DATABASE parameter: {param}")
        db_config[param.lower()] = param_type(value)
    
    slack_config = {}
    for param, param_type in required_slack_params.items():
        value = config.get('SLACK', param, fallback=None)
        if value is None:
            raise ValueError(f"Missing required SLACK parameter: {param}")
        slack_config[param.lower()] = param_type(value)
    
    return DatabaseConfig(**db_config), SlackConfig(**slack_config)

def read_config(config_file: str = "config.ini") -> tuple[DatabaseConfig, SlackConfig, int]:
    """
    Reads the configuration file and returns the database config, slack config, and poll interval.
    
    Args:
        config_file (str): Path to the configuration file.
        
    Returns:
        tuple: A tuple containing DatabaseConfig, SlackConfig, and poll interval (int).
        
    Exits:
        If configuration file is missing or contains errors.
    """
    config = configparser.ConfigParser()
    if not config.read(config_file):
        logger.error(f"Configuration file {config_file} not found or is empty.")
        sys.exit(1)
    
    try:
        db_config, slack_config = validate_config(config)
        poll_interval = config.getint('GENERAL', 'POLL_INTERVAL', fallback=60)
        return db_config, slack_config, poll_interval
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

def build_connection_string(config: DatabaseConfig) -> str:
    """
    Constructs the connection string for pyodbc using the provided database configuration.
    
    Args:
        config (DatabaseConfig): The database configuration dataclass.
        
    Returns:
        str: The constructed ODBC connection string.
    """
    return (
        f"DRIVER={{{config.driver}}};"
        f"SERVER={config.server};"
        f"PORT={config.port};"
        f"DATABASE={config.database};"
        f"UID={config.uid};"
        f"PWD={config.pwd};"
        f"TDS_Version={config.tds_version};"
    )

# --- Database Connection and Health Check ---

def get_database_connection(connection_string: str, max_retries: int = 3, retry_delay: int = 5) -> Optional[pyodbc.Connection]:
    """
    Attempts to connect to the database, retrying on failure.
    
    Args:
        connection_string (str): The ODBC connection string.
        max_retries (int): Maximum number of connection attempts.
        retry_delay (int): Delay in seconds between retries.
        
    Returns:
        Optional[pyodbc.Connection]: The database connection object if successful, otherwise None.
    """
    metrics.db_connection_attempts += 1
    for attempt in range(max_retries):
        try:
            conn = pyodbc.connect(connection_string)
            logger.info("Connected to the database successfully.")
            return conn
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Database connection attempt {attempt + 1} failed: {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect after {max_retries} attempts: {e}")
                metrics.db_connection_failures += 1
                return None

def health_check(connection_string: str) -> bool:
    """
    Performs a simple health check on the database by executing a simple query.
    
    Args:
        connection_string (str): The ODBC connection string.
        
    Returns:
        bool: True if the health check passes, False otherwise.
    """
    try:
        conn = get_database_connection(connection_string)
        if conn:
            # Use a regular cursor and manually close it.
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()
            metrics.last_successful_check = datetime.datetime.now()
            return True
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return False

# --- Metrics Persistence ---

def save_metrics(metrics_file: str = "monitor_metrics.json"):
    """
    Saves the current metrics to a JSON file.
    
    Args:
        metrics_file (str): Path to the file where metrics are saved.
    """
    metrics_data = {
        "notifications_sent": metrics.notifications_sent,
        "failed_notifications": metrics.failed_notifications,
        "db_connection_attempts": metrics.db_connection_attempts,
        "db_connection_failures": metrics.db_connection_failures,
        "last_successful_check": metrics.last_successful_check.isoformat() if metrics.last_successful_check else None,
        "timestamp": datetime.datetime.now().isoformat()
    }
    
    try:
        with open(metrics_file, 'w') as f:
            json.dump(metrics_data, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save metrics: {e}")

# --- Slack Notification Formatting and Sending ---

def format_slack_message(row: pyodbc.Row) -> Dict[str, Any]:
    """
    Formats an offline event row into a Slack block message.
    
    Args:
        row (pyodbc.Row): A row object returned from the database query.
        
    Returns:
        Dict[str, Any]: A dictionary representing the Slack message in Block Kit format.
    """
    return {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🚨 Embed Swiper Offline Alert!"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Game:*\n{row.swiper_description}"},
                    {"type": "mrkdwn", "text": f"*User:*\n{row.user_name}"},
                    {"type": "mrkdwn", "text": f"*Days Offline:*\n{row.Days_Offline}"},
                    {"type": "mrkdwn", "text": f"*Log Time:*\n{row.log_datetime}"}
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Comment:*\n{row.comment}"
                }
            }
        ]
    }

def send_slack_notification(slack_client: WebClient, channel: str, message: Dict[str, Any],
                            max_retries: int = 3, retry_delay: int = 5) -> bool:
    """
    Attempts to send a Slack notification, retrying on failure.
    
    Args:
        slack_client (WebClient): The Slack client instance.
        channel (str): Slack channel to send the message to.
        message (Dict[str, Any]): The message payload in Slack Block Kit format.
        max_retries (int): Maximum number of attempts.
        retry_delay (int): Delay in seconds between attempts.
        
    Returns:
        bool: True if the notification was sent successfully, False otherwise.
    """
    for attempt in range(max_retries):
        try:
            response = slack_client.chat_postMessage(
                channel=channel,
                blocks=message["blocks"]  # Pass the blocks directly as a list.
            )
            logger.info(f"Notification sent to Slack: {response.data}")
            metrics.notifications_sent += 1
            return True
        except SlackApiError as e:
            error_msg = e.response.get("error", str(e))
            if attempt < max_retries - 1:
                logger.warning(f"Slack notification attempt {attempt + 1} failed: {error_msg}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to send Slack notification after {max_retries} attempts: {error_msg}")
                metrics.failed_notifications += 1
                return False

# --- Fetching Events from the Database ---

def fetch_offline_events(cursor: pyodbc.Cursor, last_check: datetime.datetime) -> list:
    """
    Executes a query to retrieve new embed swiper offline events that occurred after the last_check timestamp.
    
    This query uses a Common Table Expression (CTE) similar to your initial SQL. Note that the subquery
    now includes events where the comment starts with "Swiper placed Offline%" (using LIKE) so that you
    are notified when swipers go offline.
    
    Args:
        cursor (pyodbc.Cursor): The database cursor used for executing the query.
        last_check (datetime.datetime): Timestamp to filter events.
        
    Returns:
        list: A list of rows containing event data.
    """
    query = """
    WITH offline_events AS (
        SELECT
            ROW_NUMBER() OVER (PARTITION BY gs.game_id ORDER BY gl.log_datetime DESC) as row,
            gs.swiper_description,
            u.user_name,
            gl.comment,
            gl.log_datetime,
            DATEDIFF(dd, gl.log_datetime, CURRENT_TIMESTAMP) as Days_Offline
        FROM ecs7.dbo.game_swipers gs
        JOIN (
            SELECT game_id, log_datetime,
                   STRING_AGG(TRIM(comment), ', ') as Comment
            FROM ecs7.dbo.game_log
            WHERE comment LIKE 'Swiper placed Offline%'
            GROUP BY game_id, log_datetime
        ) gl ON gs.game_id = gl.game_id
        JOIN ecs7.dbo.game_events ge
            ON ge.game_id = gl.game_id
               AND ge.event_time = gl.log_datetime
        JOIN ecs7.dbo.users u
            ON ge.user_id = u.user_id
        JOIN ecs7.dbo.swiper_units su
            ON su.game_id = gs.game_id
        WHERE gs.retired IS NULL
          AND ge.event_type = 44
          AND su.status = 1
    )
    SELECT swiper_description, user_name, comment, log_datetime, Days_Offline
    FROM offline_events
    WHERE row = 1 AND log_datetime > ?
    ORDER BY swiper_description
    """
    try:
        cursor.execute(query, last_check)
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return []

# --- Signal Handling for Graceful Shutdown ---

def signal_handler(signum: int, frame: Any):
    """
    Signal handler that sets a shutdown flag for graceful termination.
    
    Args:
        signum (int): Signal number.
        frame (Any): Current stack frame.
    """
    global shutdown_flag
    logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
    shutdown_flag = True

# --- Main Monitoring Loop ---

def monitor_swiper_offline_events(connection_string: str, slack_client: WebClient,
                                  slack_channel: str, poll_interval: int):
    """
    Continuously monitors the database for new embed swiper offline events and sends Slack notifications.
    
    Uses a ThreadPoolExecutor to send notifications concurrently.
    
    Args:
        connection_string (str): The database connection string.
        slack_client (WebClient): The Slack client instance.
        slack_channel (str): The Slack channel for notifications.
        poll_interval (int): Time in seconds between polling iterations.
    """
    last_check = datetime.datetime.now() - datetime.timedelta(minutes=1)
    logger.info("Starting monitoring of embed swiper offline events...")
    
    with ThreadPoolExecutor(max_workers=3) as pool:
        while not shutdown_flag:
            try:
                if not health_check(connection_string):
                    logger.error("Health check failed. Waiting before retry...")
                    time.sleep(poll_interval)
                    continue
                
                conn = get_database_connection(connection_string)
                if not conn:
                    time.sleep(poll_interval)
                    continue
                
                with conn:
                    cursor = conn.cursor()
                    rows = fetch_offline_events(cursor, last_check)
                    
                    if rows:
                        max_log_datetime = last_check
                        for row in rows:
                            message = format_slack_message(row)
                            # Submit the task to send a Slack notification concurrently.
                            pool.submit(
                                send_slack_notification,
                                slack_client,
                                slack_channel,
                                message
                            )
                            if row.log_datetime > max_log_datetime:
                                max_log_datetime = row.log_datetime
                        last_check = max_log_datetime
                
                save_metrics()
                
            except Exception as e:
                logger.error(f"Error during monitoring loop: {e}")
            finally:
                try:
                    time.sleep(poll_interval)
                except KeyboardInterrupt:
                    logger.info("Received KeyboardInterrupt. Initiating shutdown...")
                    break

# --- Main Entry Point ---

def main():
    """
    Main entry point of the script. Sets up signal handlers, logging, configuration,
    and initiates the monitoring loop.
    """
    # Register signal handlers for graceful shutdown.
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Ensure the logs directory exists.
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        global logger
        # Reinitialize logger to include file logging.
        logger = setup_logging(log_file=str(log_dir / "swiper_monitor.log"))
        
        # Read configuration and build connection string.
        db_config, slack_config, poll_interval = read_config()
        connection_string = build_connection_string(db_config)
        slack_client = WebClient(token=slack_config.bot_token)
        
        if not health_check(connection_string):
            logger.error("Initial health check failed. Please check configuration and connectivity.")
            return
        
        monitor_swiper_offline_events(
            connection_string=connection_string,
            slack_client=slack_client,
            slack_channel=slack_config.channel,
            poll_interval=poll_interval
        )
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        save_metrics()
        logger.info("Script terminated. Final metrics saved.")

if __name__ == "__main__":
    main()
