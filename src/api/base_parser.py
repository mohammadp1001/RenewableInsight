import abc
import pandas as pd
from typing import IO, Iterator, Dict, Any, List, Set, Tuple, Optional

class BaseParser(abc.ABC):
    """
    Base class for data parsers.

    This class provides a standard interface for parsing data from various sources.
    """

    @abc.abstractmethod
    def parse_timestamps(self, fp: IO[bytes]) -> Iterator[datetime]:
        """
        Parses timestamps from the given file.

        Args:
            fp (IO[bytes]): The file-like object to parse.

        Returns:
            Iterator[datetime]: An iterator over parsed timestamps.
        """
        pass

    @abc.abstractmethod
    def parse_forecasts(self, fp: IO[bytes], stations: Optional[Set[str]] = None) -> Iterator[Tuple[str, Dict[str, List[Optional[float]]]]]:
        """
        Parses forecast data from the given file.

        Args:
            fp (IO[bytes]): The file-like object to parse.
            stations (Optional[Set[str]]): A set of station identifiers to filter the forecasts.

        Returns:
            Iterator[Tuple[str, Dict[str, List[Optional[float]]]]]: An iterator over parsed forecast data.
        """
        pass

    @staticmethod
    @abc.abstractmethod
    def convert_to_dataframe(forecasts: Iterator[Tuple[str, Dict[str, List[Optional[float]]]]], station: str) -> pd.DataFrame:
        """
        Converts parsed forecast data to a pandas DataFrame.

        Args:
            forecasts (Iterator[Tuple[str, Dict[str, List[Optional[float]]]]]): The parsed forecast data.
            station (str): The station identifier to extract data for.

        Returns:
            pd.DataFrame: The forecast data as a pandas DataFrame.
        """
        pass
