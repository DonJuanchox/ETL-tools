import pathlib
import time
import polars as pl
from polars import type_aliases
import pandas as pd
from logging import Logger
from typing import Union


def pd_load_file(file_path, **kwargs):
    """
    Loads a CSV file into a pandas DataFrame, handling common file loading errors.

    Parameters:
    ----------
    file_path : str
        The path to the CSV file to be loaded.
    **kwargs : dict, optional
        Additional keyword arguments to be passed to `pd.read_csv()`. This allows for custom configuration
        of the CSV loading process, such as specifying delimiters, column types, or handling of missing values.

    Returns:
    -------
    pd.DataFrame or None
        Returns a pandas DataFrame if the file is loaded successfully. Returns None if an error occurs
        (e.g., file not found, empty data, or other exceptions).

    Exceptions Handled:
    -------------------
    FileNotFoundError
        If the specified file does not exist, an error message is printed, and None is returned.
    pd.errors.EmptyDataError
        If the file exists but contains no data, an error message is printed, and None is returned.
    Exception
        For any other exceptions that may arise during loading, an error message is printed, and None is returned.

    """
    try:
        return pd.read_csv(file_path, **kwargs)
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
              schema_overrides: Union[dict, None] = None,
              infer_schema_length: Union[int, None] = None,
              columns: Union[list, None] = None,
              **kwargs) -> pl.DataFrame:
    """
    Load a file into a Polars DataFrame with optional schema overrides, schema inference length, 
    and column filtering, while logging the loading process.

    Parameters
    ----------
    log : Logger
        Logging object used to record actions at various log levels (e.g., debug, info, warning, error, critical).
    file_path : pathlib.Path
        The path to the file to be loaded.
    schema_overrides : Union[dict, None], optional
        Dictionary to override schema for specific columns or all columns during schema inference. 
        Allows for customization of data types. Default is None.
    infer_schema_length : Union[int, None], optional
        The maximum number of rows to scan when inferring the schema. This limits the number of rows 
        Polars uses for guessing column data types. Default is None, which infers the schema from all rows.
    columns : Union[list, None], optional
        List of column names to filter and load from the file. Only specified columns will be included in the 
        output DataFrame. Default is None, which loads all columns.
    kwargs : dict, optional
        Additional keyword arguments passed directly to `pl.read_csv` to customize the file loading process.
        Common arguments include `sep` for custom delimiters, `null_values` for missing value handling, etc.

    Returns
    -------
    pl.DataFrame
        A Polars DataFrame containing the loaded data.

    Raises
    ------
    FileNotFoundError
        If the specified file does not exist, an error message is printed, and None is returned.
    pd.errors.EmptyDataError
        If the file exists but contains no data, an error message is printed, and None is returned.
    Exception
        Logs an error message if any exception occurs during file loading. No exception is re-raised, and 
        None is returned if loading fails.
    
    Notes
    -----
    - The function uses Polars `pl.read_csv` with options set to optimize performance and data integrity:
        * `low_memory=False`: ensures consistent memory usage.
        * `use_pyarrow=True`: uses PyArrow for parsing, enhancing compatibility and speed.
        * `try_parse_dates=True`: attempts automatic date parsing for applicable columns.
        * `ignore_errors=True`: skips rows that cause parsing errors, allowing the loading process to continue.
    - Logs the time taken to load the file.

    """
    try:
        init_time = time.time()
        log.info(f'Loading file {file_path}.')
        df = pl.read_csv(
            file_path,
            low_memory=False,
            use_pyarrow=True,
            try_parse_dates=True,
            ignore_errors=True,
            infer_schema_length=infer_schema_length,
            schema_overrides=schema_overrides,
            **kwargs)
        log.info(
            f'File {file_path} loaded in {time.time()-init_time:0.2f} sec.')
    except FileNotFoundError:
        log.error(f'Error: File not found - {file_path}')
        return None
    except pl.NoDataError:
        log.error('Error reading %s - Error type: %s', file_path)
    except Exception as e:
        log.error('Error reading %s - Error type: %s', file_path, e)
    else:
        return df


def write_parquet(log: Logger,
                  df: pl.DataFrame,
                  file_path: pathlib.Path,
                  compression: type_aliases.ParquetCompression = 'zstd',
                  compression_level: int = 22,
                  **kwargs) -> None:
    """
    Write a Polars DataFrame to a Parquet file with specified compression settings, while logging the process.

    Parameters
    ----------
    log : Logger
        Logging object used for recording actions at various log levels (e.g., debug, info, warning, error, critical).
    df : pl.DataFrame
        The Polars DataFrame containing data to be saved.
    file_path : pathlib.Path
        Destination path for the Parquet file to be written.
    compression : ParquetCompression, optional
        Compression algorithm to use for the Parquet file, optimizing storage efficiency. The default is 'zstd'.
        Common options include 'snappy', 'gzip', 'brotli', and 'zstd'.
    compression_level : int, optional
        Specifies the level of compression for the chosen algorithm, with higher levels providing greater 
        compression at the cost of increased processing time. Default is 22 for high compression with 'zstd'.
    kwargs : dict, optional
        Additional keyword arguments passed directly to `df.write_parquet`, allowing further customization
        of the Parquet writing process (e.g., `row_group_size`, `statistics`, etc.).

    Returns
    -------
    None
        This function does not return a value. Instead, it writes the DataFrame to the specified file path.

    Raises
    ------
    Exception
        Logs an error message if any exception occurs during file writing, indicating the file path and error type.

    Notes
    -----
    - The function leverages Polars’ `write_parquet` method with options set to optimize storage:
        * `compression`: minimizes file size using the specified algorithm.
        * `compression_level`: controls the degree of compression.
        * `use_pyarrow=True`: ensures compatibility with PyArrow for writing, enhancing performance.
    - Logs the time taken to write the file.

    """
    try:
        init_time = time.time()
        log.info(f'Writing file {file_path}.')
        df.write_parquet(file_path,
                         compression=compression,
                         compression_level=compression_level,
                         use_pyarrow=True,
                         **kwargs)
        log.info(
            f'File {file_path} written in {time.time() - init_time:0.2f} sec.')

    except Exception as e:
        log.error('Error writing %s - Error Type: %s', file_path, e)


def read_parquet(log: Logger,
                 file_path: pathlib.Path,
                 cols: Union[list[str], None] = None,
                 **kwargs) -> pl.DataFrame:
    """
    Read a Parquet file into a Polars DataFrame, with optional column filtering and logging.

    Parameters
    ----------
    log : Logger
        Logger object used to record actions and any errors at various levels (e.g., debug, info, warning, error, critical).
    file_path : pathlib.Path
        Path to the Parquet file to be read.
    cols : Union[list[str], None], optional
        List of column names to load from the file. If specified, only these columns will be read. 
        The default is None, which loads all columns.
    kwargs : dict, optional
        Additional keyword arguments passed directly to `pl.read_parquet`, enabling customization of 
        the reading process (e.g., `row_group_size`, `storage_options`, etc.).

    Returns
    -------
    pl.DataFrame
        Polars DataFrame containing data from the file. If an error occurs during reading, it logs the 
        error message but still attempts to return any successfully loaded portion of the DataFrame.

    Raises
    ------
    Exception
        Logs an error message if any exception occurs during file reading, indicating the file path and 
        the specific error type. The function does not re-raise exceptions.

    Notes
    -----
    - This function uses Polars' `read_parquet` method to load Parquet files into memory with high performance.
    - Logs errors during the reading process without halting execution.
    
    """
    try:
        df = pl.read_parquet(file_path, columns=cols, **kwargs)
    except Exception as e:
        log.error('Error reading %s - Error type: %s', file_path, e)

    return df

