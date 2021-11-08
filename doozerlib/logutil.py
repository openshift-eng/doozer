import logging


class EntityLoggingAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return '[%s] %s' % (self.extra['entity'], msg), kwargs


def getLogger(module_name=None):
    """
    Returns a logger appropriate for use in the doozer modules.
    Modules should request a logger using their __name__
    """
    logger_name = 'doozer'

    if module_name:
        logger_name = '{}.{}'.format(logger_name, module_name)

    return logging.getLogger(logger_name)
