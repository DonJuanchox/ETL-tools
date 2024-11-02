import pathlib
import time
import polars as pl
from polars import type_aliases
import pandas as pd
from logging import Logger
from typing import Union

# Function to safely load CSV with error handling
def load_csv(file_path, parse_dates=None):
    try:
        return pd.read_csv(file_path, parse_dates=parse_dates)
    except FileNotFoundError:
        print(f"Error: File not found - {file_path}")
        return None
    except pd.errors.EmptyDataError:
        print(f"Error: No data in file - {file_path}")
        return None
    except Exception as e:
        print(f"Error while loading file {file_path}: {e}")
        return None


def load_file(log: Logger,
              file_path: pathlib.Path,
              dtypes: Union[dict, None] = None,
              infer_schema_length: Union[int, None] = None,
              columns: Union[list, None] = None) -> pl.DataFrame:
    """
    LOoad a file using polars libary.

    Parameters
    ----------
    log : Logger
        Logging object to prompt: debug, info, warning, error, critical.
    file_path : pathlib.Path
        Path to file.
    dtypes : Union[dict, None], optional
        Overwrite dtypes for specific or all columns during schema inference. The default is None.
    infer_schema_length : Union[int, None], optional
        The maximum number of rows to scan for schema inference. The default is None.
    columns : Union[list, None], optional
        Columns to filter file. The default is None.

    Returns
    -------
    None.

    """

    try:
        init_time = time.time()
        log.info(f'Loading file {file_path}.')
        df = pl.read_csv(
            file_path,
            separator=',',
            low_memory=False,
            use_pyarrow=True,
            try_parse_dates=True,
            ignore_errors=True,
            infer_schema_length=infer_schema_length,
            dtypes=dtypes
        )
        log.info(
            f'File {file_path} loaded in {time.time()-init_time:0.2f} sec.')
    except Exception as e:
        log.error('Error to read %s - Error type: %s', file_path, e)
    else:
        return df


def write_parquet(log: Logger,
                  df: pl.DataFrame,
                  file_path: pathlib.Path,
                  compression: type_aliases.ParquetCompression = 'zstd',
                  compression_level: int = 22) -> None:
    """
    Write dataframe as a parquet file.

    Parameters
    ----------
    log : Logger
        Logging object to prompt: debug, info, warning, error, critical.
    df : pl.DataFrame
        Dataframe containing data.
    file_path : pathlib.Path
        Path to write file.
    compression : ParquetCompression
        Type of compression/decompression algorithm. The default is 'zstd'.
    compression_level : str
        Level of compression to use. Higher compression means smaller files on disk. The default is 22.

    Returns
    -------
    None
    """
    try:
        init_time = time.time()
        log.info(f'Writing file {file_path}.')
        df.write_parquet(file_path,
                         compression=compression,
                         compression_level=compression_level,
                         use_pyarrow=True)
        log.info(
            f'File {file_path} written in {time.time() - init_time:0.2f} sec.')

    except Exception as e:
        log.error('Error to write %s - Error Type: %s', file_path, e)


def read_parquet(log: Logger,
                 file_path: pathlib.Path,
                 cols: Union[list[str], None] = None) -> pl.DataFrame:
    """
    Read a parquet file.

    Parameters
    ----------
    log : Logger
        Logger object to prompt: debug, info, warning, error, critical.
    file_path : pathlib.Path
        Path to file.
    cols : Union[list[str], None], optional
        Columns to filter file. The default is None.

    Returns
    -------
    df : pl.DataFrame
        Polars DataFrame containg data from file.

    """
    try:
        df = pl.read_parquet(file_path.with_suffix('.parquet'), columns=cols)
    except Exception as e:
        log.error('Error to read %s - Error type: %s', file_path, e)
        pass
    return df
