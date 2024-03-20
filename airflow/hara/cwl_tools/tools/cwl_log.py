import logging
import sys
import colorlog

cwl_logger: logging.Logger = None


def setup_logger():
    global cwl_logger
    cwl_logger = logging.getLogger('hara_cwl')
    cwl_logger.setLevel(logging.DEBUG)
    # set handler
    stdout_handler = logging.StreamHandler(stream=sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    # set formatter
    # fmt = logging.Formatter(
    #     '%(name)s: %(asctime)s | %(levelname)s | %(filename)s:%(lineno)s | %(process)d >>> %(message)s    -----|File "%(pathname)s", line %(lineno)d'
    # )
    cwl_log_formatter = colorlog.ColoredFormatter(
        '%(name)s: %(white)s%(asctime)s.%(msecs)03d %(reset)s | %(log_color)s%(levelname)s%(reset)s | %(blue)s%(filename)s:%(lineno)s%(reset)s | %(process)d >>> %(log_color)s%(message)s%(reset)s     -----|File "%(pathname)s", line %(lineno)d'
    )
    stdout_handler.setFormatter(cwl_log_formatter)

    cwl_logger.addHandler(stdout_handler)


def get_cwl_logger() -> logging.Logger:
    global cwl_logger
    if cwl_logger is None:
        setup_logger()
    return cwl_logger


if __name__ == '__main__':
    logger = get_cwl_logger()
    logger.debug('hara debug')
    logger.info('hara info')
    logger.warning('hara warn')
    logger.error('hara error')

    try:
        1 / 0
    except ZeroDivisionError as e:
        logger.error(e, exc_info=True)
        logger.exception(e)
        logger.critical(e, exc_info=True)
