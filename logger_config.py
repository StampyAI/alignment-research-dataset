import logging

# Create a logger object.
logger = logging.getLogger('ard_logger')

# Set the log level.
logger.setLevel(logging.INFO)

# Create a file handler.
# handler = logging.FileHandler('ard.log')

# Create a stream handler.
handler = logging.StreamHandler()

# Create a logging format.
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Set the formatter for the handler.
handler.setFormatter(formatter)

# Add the handler to the logger.
logger.addHandler(handler)
