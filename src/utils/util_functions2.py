# Utilities functions
from contextlib import contextmanager
from memory_profiler import memory_usage
from src.utils.global_constants import Paths
from src.utils.logger import Logger


@contextmanager
def memit(code_block: str):
    """
    Context manager function to measure memory usage
    :param code_block: Name of the code block (e.g. function name) to be measured for memory usage
    """
    log = Logger(logger_name="Memory Usage")
    logger = log.logger_creator('INFO', logging_path=Paths.MEMORY_LOG.value)

    # Memory usage peak:
    mem_before = memory_usage()[0]  # Start memory monitoring
    yield
    mem_after = memory_usage(max_usage=True)  # Stop memory monitoring
    logger.info(f"Peak memory usage on '{code_block}': {mem_after - mem_before:.4f} MiB")
