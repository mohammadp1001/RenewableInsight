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

        :param fp: The file-like object to parse.
        :type fp: IO[bytes]
        :return: An iterator over parsed timestamps.
        :rtype: Iterator[datetime]
        """
        pass

    @abc.abstractmethod
    def parse_forecasts(self, fp: IO[bytes], stations: Optional[Set[str]] = None) -> Iterator[Tuple[str, Dict[str, List[Optional[float]]]]]:
        """
        Parses forecast data from the given file.

        :param fp: The file-like object to parse.
        :param stations: A set of station identifiers to filter the forecasts. If None, all stations are included.
        :return: An iterator over parsed forecast data, where each item is a tuple containing the station name and a dictionary of forecast data.
        """
        pass

    @staticmethod
    @abc.abstractmethod
    def convert_to_dataframe(forecasts: Iterator[Tuple[str, Dict[str, List[Optional[float]]]]], station: str) -> pd.DataFrame:
        """
        Converts parsed forecast data to a pandas DataFrame.

        :param forecasts: The parsed forecast data.
        :param station: The station identifier to extract data for.
        :return: The forecast data as a pandas DataFrame.
        """
        pass
