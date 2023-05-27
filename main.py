import os
import time
import json
import sys
from config import DIR_PATH
import logging
from logging import Logger
from colorlog import ColoredFormatter
import threading
from datetime import datetime



class DBDOG:
    def __init__(self):
        self.config = {}
        
        with open(os.path.join(DIR_PATH,"config.json")) as config_file:
            self.config = json.load(config_file)


        _logs_dir = self.config.get("logsDir","logs")
        _clusters = self.config.get("clusters",[])


        self.logs_dir = os.path.join(DIR_PATH,_logs_dir)
        if not os.path.exists(self.logs_dir):
            os.mkdir(self.logs_dir)

        self.clusters = _clusters
        self.cluster_actions = ["restore","backup"]

        self.logger = self.create_logger("mainlogger","runtime.log")

    def run(self):

        for cluster in self.clusters:
            cluster_name = cluster.get("name",None)
            if cluster_name is None:
                self.logger.info("A cluster does not have a name !")
                return
            is_hidden = cluster.get("isHidden",False)
            conn_string = cluster.get("connString")
            if is_hidden == True:
                conn_string = os.environ.get(conn_string)
                if not conn_string:
                    # User has not specified a connection string in env file
                    self.logger.info(f"Unable to find {conn_string} in environ")
                    return
            action = cluster.get("action")
            if action not in self.cluster_actions:
                self.logger.info(f"Action must be one of {self.cluster_actions}")
                return 
            delay_time = cluster.get("delayTime",24)
            
            if delay_time < 12:
                delay_time = 12

            cluster_config = {
                "name":cluster_name,
                "conn_string":conn_string,
                "action":action,
                "delay_time":delay_time,
            }

            new_thread = threading.Thread(target=self.cluster_action,kwargs=cluster_config)
            new_thread.start()
            self.logger.info(f"Starting thread for cluster = {cluster_name}")


    def create_logger(self,name:str,filename:str) -> Logger:
        # Create a logger object
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        # Define a custom date format
        date_format = '%b,%d - %H:%M'

        # Create a formatter object with color formatting
        formatter = ColoredFormatter(
                '%(log_color)s[%(asctime)s] %(levelname)s => %(message)s',
                datefmt=date_format,
                log_colors={
                    'INFO': 'cyan',
                    'DEBUG': 'green',
                    'WARNING': 'yellow',
                    'ERROR': 'red',
                    'CRITICAL': 'red,bg_white',
                },
        )

        formatter2 = logging.Formatter('[%(asctime)s] %(levelname)s => %(message)s', datefmt='%b,%d - %H:%M')

        # Create a file handler and add it to the logger
        file_handler = logging.FileHandler(os.path.join(DIR_PATH,self.logs_dir,filename), mode="a")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter2)
        # file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Create a stream handler for console output with color formatting
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        return logger

    def cluster_action(self,name:str,action:str,delay_time:int,conn_string:str) -> None:
        cluster_action_logger = self.create_logger(name,name+".log")
        dump_date = datetime.now().strftime("%H:%M__%b_%d_%Y") # '15:56 - May 27,2023'
        BACKUP_DIR = '/workspace/linuxpro/backups'
        BACKUP_PATH = os.path.join(BACKUP_DIR,f"dump_{dump_date}.tar.gz")
        command = f"mongodump {conn_string} --gzip --archive='{BACKUP_PATH}'"
        
        while True:
            cluster_action_logger.info(f"Backing up {name} to {BACKUP_PATH}")
            start_time = time.time()
            os.system(command)
            operation_time = abs(time.time()-start_time)
            cluster_action_logger.info(f"Backup completed ! Took {operation_time}s")
            cluster_action_logger.info(f"Sleeping for {delay_time}hrs ...")
            delay_sec = delay_time*3600
            time.sleep(delay_sec)

            




if __name__ == "__main__":
    dbdog = DBDOG()
    dbdog.run()
