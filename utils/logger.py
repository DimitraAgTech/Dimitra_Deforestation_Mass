import logging
import logging.handlers
import os
import sys

from config import LOG_LEVEL

levelToLogLevel = {
    'CRITICAL': logging.CRITICAL,
    'FATAL': logging.CRITICAL,
    'ERROR': logging.ERROR,
    'WARNING': logging.WARNING,
    'WARN': logging.WARNING,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
    'NOTSET': logging.NOTSET
}


class LogUtils:
    __def_loglevel = 'DEBUG'
    __def_format = '%(levelname)s [%(asctime)s] %(process)d/%(thread)d %(filename)s %(funcName)s: %(lineno)d: %(message)s'
    __def_datefmt = '%m/%d/%Y %I:%M:%S %p'

    __config = None
    __filename = None
    __loglevel = __def_loglevel
    __format = __def_format
    __datefmt = __def_datefmt
    __log_to_console = None
    __path = '.'

    def __init__(self, filename=None, config=None, log_to_console=False):
        self.__config = config
        self.__filename = filename or 'debug'
        self.__log_to_console = log_to_console
        # Get values from passed config if present, use defaults else
        if self.__config:
            self.__loglevel = self.__config.get(
                'loglevel', self.__def_loglevel)
            self.__format = self.__config.get('format', self.__def_format)
            self.__datefmt = self.__config.get(
                'date_format', self.__def_datefmt)

    def get_logger(self):
        log_path = os.path.join(self.__path, 'logs')

        logging.basicConfig(level=levelToLogLevel.get(self.__loglevel))

        if self.__log_to_console:
            logger = logging.getLogger(__name__)

            stream_handler_list = [
                h for h in logger.handlers if isinstance(h, logging.StreamHandler)]

            if (len(stream_handler_list) == 0):
                log_handler = logging.StreamHandler(stream=sys.stdout)
                log_handler.setFormatter(logging.Formatter(
                    self.__format, datefmt=self.__datefmt))
                logger.propagate = 0
                logger.addHandler(log_handler)

        else:
            logger = logging.getLogger(self.__filename)

            stream_handler_list = [
                h for h in logger.handlers if isinstance(h, logging.FileHandler)]

            if (len(stream_handler_list) == 0):
                if not os.path.exists(log_path):
                    os.makedirs(log_path)

                log_handler = logging.handlers.TimedRotatingFileHandler(
                    filename=os.path.join(log_path, self.__filename + ".log"), when='midnight')
                log_handler.setFormatter(logging.Formatter(
                    self.__format, datefmt=self.__datefmt))
                logger.propagate = 0
                logger.addHandler(log_handler)

        return logger


__config = {'loglevel': LOG_LEVEL}
logger = LogUtils(config=__config, log_to_console=True).get_logger()
