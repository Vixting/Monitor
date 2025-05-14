import time
import logging
import threading
from datetime import datetime
from pathlib import Path
import signal
import sys
import codecs
import re

from database import init_database, migrate_database
from server_monitor import ServerMonitor

DATA_DIR = Path("./data")
LOG_DIR = Path("./logs")
INTERVAL_SECONDS = 10

DATA_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# Reset logging to avoid duplicate entries
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Setup file logging
log_file = LOG_DIR / f"server_monitor_{datetime.now().strftime('%Y-%m-%d')}.log"
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(file_formatter)

class ColoredFormatter(logging.Formatter):
    """Custom formatter with separate coloring for log levels and message content"""
    LEVEL_COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[92m',     # Bright Green
        'WARNING': '\033[93m',  # Bright Yellow
        'ERROR': '\033[91m',    # Bright Red
        'CRITICAL': '\033[97;41m', # White on Red
    }
    
    MESSAGE_COLORS = {
        'DEATH': '\033[31m',    # Red
        'REDEEM': '\033[94m',   # Blue
        'MAP': '\033[95m',      # Magenta
        'WAVE': '\033[96m',     # Cyan
        'SERVER': '\033[33m',   # Yellow
        'PLAYER': '\033[32m',   # Green
        'DEFAULT': '\033[0m',   # Reset/Default
    }
    
    RESET = '\033[0m'
    
    def format(self, record):
        # Start with standard formatting
        formatted_msg = super().format(record)
        
        # Extract parts of the log message
        parts = re.match(r"(.*? - )(\w+)( - )(.*)", formatted_msg)
        if not parts:
            return formatted_msg
        
        timestamp = parts.group(1)
        level = parts.group(2)
        separator = parts.group(3)
        message = parts.group(4)
        
        colored_level = f"{self.LEVEL_COLORS.get(level, self.LEVEL_COLORS['INFO'])}{level}{self.RESET}"
        message_color = self.MESSAGE_COLORS['DEFAULT']
        
        if message.startswith("DEATH:"):
            message_color = self.MESSAGE_COLORS['DEATH']
        elif message.startswith("REDEEM:"):
            message_color = self.MESSAGE_COLORS['REDEEM']
        elif "map" in message.lower() or "created new game session" in message.lower():
            message_color = self.MESSAGE_COLORS['MAP']
        elif "wave" in message.lower():
            message_color = self.MESSAGE_COLORS['WAVE']
        elif "server" in message.lower():
            message_color = self.MESSAGE_COLORS['SERVER']
        elif "player" in message.lower():
            message_color = self.MESSAGE_COLORS['PLAYER']
        
        return f"{timestamp}{colored_level}{separator}{message_color}{message}{self.RESET}"

console_handler = logging.StreamHandler(codecs.getwriter('utf-8')(sys.stdout.buffer))
console_formatter = ColoredFormatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(console_formatter)

logging.root.setLevel(logging.INFO)
logging.root.addHandler(file_handler)
logging.root.addHandler(console_handler)

def signal_handler(sig, frame):
    """Handle termination signals"""
    global monitor
    logging.info("Shutdown signal received. Stopping monitor...")
    monitor.stop()
    sys.exit(0)

def main():
    global monitor
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logging.info("Server Monitor starting...")
    init_database()
    migrate_database()
    
    monitor = ServerMonitor()
    monitor_thread = monitor.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received. Shutting down...")
        monitor.stop()
        monitor_thread.join(timeout=5)

if __name__ == "__main__":
    main()