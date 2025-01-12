import logging
import os

def get_logger(name: str,
               level: logging,
               handlers: logging) -> logging.Logger:
    """
    Get a logging object

    Parameters
    ----------
    name : str
        Name of the Log.
    level : logging
        Level of the Log object.
    handlers : logging
        List of Log handlers emiting logs for this Logger.

    Returns
    -------
    logger : logging.Logger
        Logger object.

    """
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.handlers.clear()

    # Formatter
    extra_info = {'user': os.getlogin()}
    fmt_str = '%(asctime)s - %(user)s - %(name)s - %(filename)s @%(funcName)s #%(lineno)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt_str)

    for handler in handlers:
        handler.setLevel(level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger = logging.LoggerAdapter(logger, extra_info)

    return logger
        
        