import re
import pytz
import logging
import pandas as pd
import lxml.etree as ET

from io import BytesIO
from pathlib import Path
from contextlib import contextmanager
from zipfile import ZipFile, BadZipFile
from datetime import datetime, timezone
from xml.etree.ElementTree import Element
from typing import IO, Iterator, Dict, ClassVar, Optional, Tuple, Any, List, Set, Generator

from src.api.base_parser import BaseParser


class DwdMosmixParser(BaseParser):
    """
    Parsing methods for DWD MOSMIX KML XML files.
    Note that all methods iteratively consume from an i/o stream, such that it cannot be reused without rewinding it.
    """

    _ns: ClassVar[Dict[str, str]] = {
        "kml": "http://www.opengis.net/kml/2.2",
        "dwd": "https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd",
    }
    _undef_sign: ClassVar[str] = "-"  # dwd:FormatCfg/dwd:DefaultUndefSign

    @classmethod
    def _iter_tag(cls, fp: IO[bytes], tag: str) -> Iterator[Element]:
        """
        Iterate over XML elements with the specified tag.

        :param fp: The file-like object to parse.
        :param tag: The tag to search for.
        :yield: XML elements matching the specified tag.
        """
        if ":" in tag:
            ns, tag = tag.split(":", maxsplit=1)
            tag = f"{{{cls._ns[ns]}}}{tag}"
        context = ET.iterparse(fp, events=("end",), tag=tag)
        for event, elem in context:
            yield elem
            elem.clear()

    @classmethod
    def _parse_timestamp(cls, value: Optional[str]) -> datetime:
        """
        Parse a timestamp from a string value.

        :param value: The timestamp string to parse.
        :return: The parsed datetime object in UTC.
        :raises ValueError: If the timestamp is undefined or cannot be parsed.
        """
        if not value:
            raise ValueError("Undefined timestamp")
        try:
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.000Z").replace(tzinfo=timezone.utc)
        except ValueError as e:
            raise ValueError(f"Cannot parse timestamp '{value}'") from e

    def parse_timestamps(self, fp: IO[bytes]) -> Iterator[datetime]:
        """
        Parse all `ForecastTimeSteps` as UTC timestamps.

        :param fp: The file-like object to parse.
        :yield: Parsed timestamps as datetime objects.
        """
        for elem in self._iter_tag(fp, "dwd:ForecastTimeSteps"):
            yield from (self._parse_timestamp(_.text) for _ in elem.iterfind("dwd:TimeStep", namespaces=self._ns))
            break

    @classmethod
    def _parse_coordinates(cls, value: str) -> Tuple[float, float, float]:
        """
        Parse coordinates from a string value.

        :param value: The string containing coordinates in "longitude,latitude,elevation" format.
        :return: A tuple of parsed float values (longitude, latitude, elevation).
        :raises ValueError: If the coordinates cannot be parsed.
        """
        values: List[str] = value.split(",")
        if len(values) != 3:
            raise ValueError(f"Cannot parse coordinates '{value}'")
        try:
            return float(values[0]), float(values[1]), float(values[2])
        except ValueError as e:
            raise ValueError(f"Cannot parse coordinates '{value}'") from e

    @classmethod
    def _parse_description(cls, placemark: Element) -> str:
        """
        Parse the description from a KML Placemark element.

        :param placemark: The KML Placemark element.
        :return: The description text.
        :raises ValueError: If the description is not found or empty.
        """
        description: Optional[Element] = placemark.find("kml:description", namespaces=cls._ns)
        if description is None or not description.text:
            raise ValueError("No 'Placemark.description' found")
        return description.text

    @classmethod
    def _parse_placemark(cls, placemark: Element) -> Dict[str, Any]:
        """
        Parse information from a KML Placemark element.

        :param placemark: The KML Placemark element.
        :return: A dictionary containing the parsed information.
        :raises ValueError: If required fields (name, coordinates) are not found.
        """
        name: Optional[Element] = placemark.find("kml:name", namespaces=cls._ns)
        if name is None or not name.text:
            raise ValueError("No 'Placemark.name' found")

        coordinates: Optional[Element] = placemark.find("kml:Point/kml:coordinates", namespaces=cls._ns)
        if coordinates is None or not coordinates.text:
            raise ValueError("No 'Placemark.Point.coordinates' found")
        lng, lat, ele = cls._parse_coordinates(coordinates.text)

        return {
            "desc": cls._parse_description(placemark),
            "name": name.text,
            "lat": lat,
            "lng": lng,
            "ele": ele,
        }

    @classmethod
    def _parse_values(cls, values: str) -> List[Optional[float]]:
        """
        Parse forecast values from a string.

        :param values: The string containing forecast values.
        :return: A list of parsed forecast values, with None for undefined values.
        :raises ValueError: If the forecast values cannot be parsed.
        """
        try:
            return [None if _ == cls._undef_sign else float(_) for _ in values.split()]
        except ValueError as e:
            raise ValueError(f"Cannot parse forecast values '{values}'") from e

    @classmethod
    def _parse_forecast(cls, placemark: Element) -> Dict[str, List[Optional[float]]]:
        """
        Parse forecast data from a KML Placemark element.

        :param placemark: The KML Placemark element.
        :return: A dictionary containing the parsed forecast data.
        :raises ValueError: If required forecast fields are not found.
        """
        forecasts: Dict[str, List[Optional[float]]] = {}
        for forecast in placemark.iterfind("kml:ExtendedData/dwd:Forecast", namespaces=cls._ns):
            name = forecast.get(f"{{{cls._ns['dwd']}}}elementName")
            if not name:
                raise ValueError("No 'Forecast.elementName' found")

            value = forecast.find("dwd:value", namespaces=cls._ns)
            if value is None or not value.text:
                raise ValueError("No 'Forecast.value' found")

            forecasts[name] = cls._parse_values(value.text)
        return forecasts

    def parse_forecasts(self, fp: IO[bytes], stations: Optional[Set[str]] = None) -> Iterator[Tuple[str, Dict[str, List[Optional[float]]]]]:
        """
        Parse all value series in `Forecast`, optionally limited to certain stations.

        :param fp: The file-like object to parse.
        :param stations: A set of station identifiers to filter the forecasts. If None, all stations are included.
        :yield: Parsed forecast data for each station.
        """
        for elem in self._iter_tag(fp, "kml:Placemark"):
            placemark_desc = self._parse_description(elem)
            if stations is None or placemark_desc in stations:
                yield placemark_desc, self._parse_forecast(elem)

    @staticmethod
    def convert_to_dataframe(forecasts: Iterator[Tuple[str, Dict[str, List[Optional[float]]]]], station: str) -> pd.DataFrame:
        """
        Convert parsed forecast data to a pandas DataFrame.

        :param forecasts: The parsed forecast data.
        :param station: The station identifier to extract data for.
        :return: The forecast data as a pandas DataFrame.
        :raises ValueError: If the station is not found in the forecast data.
        """
        forecast_dict = dict(forecasts)
        if station not in forecast_dict:
            raise ValueError(f"Station '{station}' not found in forecasts")
        return pd.DataFrame.from_dict(forecast_dict[station])

@contextmanager
def kmz_reader(fp: IO[bytes]) -> Generator[IO[bytes], None, None]:
    """
    Wrap reading from *.kmz files, which are merely compressed *.kml (XML) files.

    :param fp: The file-like object to read from.
    :yield: A file-like object containing the uncompressed KML data.
    :raises OSError: If the KMZ archive has unexpected contents or is invalid.
    """
    try:
        with ZipFile(fp) as zf:
            if len(zf.filelist) != 1:
                raise OSError(f"Unexpected archive contents: {' '.join(zf.namelist())}")
            with zf.open(zf.filelist[0]) as zp:
                yield zp
    except BadZipFile as e:
        raise OSError(str(e)) from None

@contextmanager
def kml_reader(filename: Path, compressed: Optional[bool] = None) -> Generator[IO[bytes], None, None]:
    """
    Read access for *.kml or compressed *.kmz files.

    :param filename: The file path of the KML or KMZ file.
    :param compressed: Whether the file is compressed as KMZ. If None, the file extension is used to determine this.
    :yield: A file-like object containing the KML data.
    :raises OSError: If the KMZ archive has unexpected contents or is invalid.
    """
    with open(filename, "rb") as fp:
        if compressed is True or (compressed is None and filename.suffix == ".kmz"):
            with kmz_reader(fp) as zp:
                yield zp
        else:
            yield fp

if __name__ == "__main__":
    pass
