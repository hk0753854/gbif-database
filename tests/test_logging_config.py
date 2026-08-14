import logging

from gbif_data_pipeline.logging_config import get_logger


def test_get_logger():
    logger = get_logger("test_logger")

    assert isinstance(logger, logging.Logger)
    assert logger.level == logging.INFO
    assert logger.handlers