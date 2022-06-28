import logging

class logger():

    def __init__(self, log_file_path, level_name) -> None:
        self.file_path = log_file_path
        self.level_name = level_name
        self.format = "%(asctime)s %(levelname)s: %(message)s"
        
        if self.level_name == "DEBUG":
            logging.basicConfig(filename=self.file_path, level=logging.DEBUG, format=self.format)

        if self.level_name == "INFO":
            logging.basicConfig(filename=self.file_path, level=logging.INFO, format=self.format)

        if self.level_name == "WARN":
            logging.basicConfig(filename=self.file_path, level=logging.WARN, format=self.format)

        if self.level_name == "ERROR":
            logging.basicConfig(filename=self.file_path, level=logging.ERROR, format=self.format)



    def log(self, level_name, message):
        
        if level_name == 'error':
            logging.error(msg=message)

        elif level_name == 'warn':
            logging.warn(msg=message)

        elif level_name == 'info':
            logging.info(msg=message)

        elif level_name == 'debug':
            logging.debug(msg=message)
        
    
    def log_shutdown(self):

        logging.shutdown()
