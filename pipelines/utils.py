import prefect


def log(msg: Any) -> None:
    """
    Logs a message with prefect logger

    Args:
        msg: Message to be displayed
    """
    prefect.context.logger.info(f"\n {msg}")
