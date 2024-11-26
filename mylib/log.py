import logging
import os

file_path = "log/database.log"


def init_log(file_name=file_path, level=logging.NOTSET):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    logging.basicConfig(
        filename=file_name,
        filemode="a",
        format="""%(asctime)s:%(msecs)d %(levelname)s %(message)s""",
        datefmt="%H:%M:%S",
        level=level,
    )
    return logging.getLogger("TransferETL")
