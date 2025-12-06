import logging

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(module)s.%(funcName)s(): %(message)s"


def setup_logging(level: int = logging.INFO) -> None:
    """Idempotent logging setup for the pysail project."""
    root = logging.getLogger()
    if root.handlers:
        return  # end early if logging is already configured

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(LOG_FORMAT))

    root.setLevel(level)
    root.addHandler(handler)

    # suppress noisy third-party loggers
    logging.getLogger("jedi").setLevel(logging.WARNING)
