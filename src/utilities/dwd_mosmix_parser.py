# see here for orginal code https://www.hackitu.de/dwd_mosmix/
from xml.etree.ElementTree import iterparse, Element
from zipfile import ZipFile, BadZipFile
import zipfile
from datetime import datetime, timezone
from typing import IO, Iterator, Dict, ClassVar, Optional, Tuple, Any, List, Set, Generator
from pathlib import Path
from contextlib import contextmanager
import lxml.etree as ET  # Using lxml for better performance
import pandas as pd
import requests
import os
import logging
import subprocess

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DwdMosmixParser:
    """
    Parsing methods for DWD MOSMIX KML XML files, namely either:
      * list of timestamps from ``ForecastTimeSteps``
      * properties of stations in ``Placemark``
      * value series in ``Forecast``
    Note that all methods iteratively consume from an i/o stream, such that it cannot be reused without rewinding it.
    """

    _ns: ClassVar[Dict[str, str]] = {
        "kml": "http://www.opengis.net/kml/2.2",
        "dwd": "https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd",
    }
    _undef_sign: ClassVar[str] = "-"  # dwd:FormatCfg/dwd:DefaultUndefSign

    @classmethod
    def _iter_tag(cls, fp: IO[bytes], tag: str) -> Iterator[Element]:
        if ":" in tag:
            ns, tag = tag.split(":", maxsplit=1)
            tag = f"{{{cls._ns[ns]}}}{tag}"
        context = ET.iterparse(fp, events=("end",), tag=tag)
        for event, elem in context:
            yield elem
            elem.clear()

    @classmethod
    def _parse_timestamp(cls, value: Optional[str]) -> int:
        if not value:
            raise ValueError("Undefined timestamp")
        try:
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.000Z").replace(tzinfo=timezone.utc)
        except ValueError as e:
            raise ValueError(f"Cannot parse timestamp '{value}'") from e

    def parse_timestamps(self, fp: IO[bytes]) -> Iterator[int]:
        """Give all ``ForecastTimeSteps`` as UTC timestamps."""
        for elem in self._iter_tag(fp, "dwd:ForecastTimeSteps"):
            yield from (self._parse_timestamp(_.text) for _ in elem.iterfind("dwd:TimeStep", namespaces=self._ns))
            break

    @classmethod
    def _parse_coordinates(cls, value: str) -> Tuple[float, float, float]:
        values: List[str] = value.split(",")
        if len(values) != 3:
            raise ValueError(f"Cannot parse coordinates '{value}'")
        try:
            return float(values[0]), float(values[1]), float(values[2])
        except ValueError as e:
            raise ValueError(f"Cannot parse coordinates '{value}'") from e

    @classmethod
    def _parse_description(cls, placemark: Element) -> str:
        description: Optional[Element] = placemark.find("kml:description", namespaces=cls._ns)
        if description is None or not description.text:
            raise ValueError("No 'Placemark.description' found")
        return description.text

    @classmethod
    def _parse_placemark(cls, placemark: Element) -> Dict[str, Any]:
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
        try:
            return [None if _ == cls._undef_sign else float(_) for _ in values.split()]
        except ValueError as e:
            raise ValueError(f"Cannot parse forecast values '{values}'") from e

    @classmethod
    def _parse_forecast(cls, placemark: Element) -> Dict[str, List[Optional[float]]]:
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
        """Give all value series in ``Forecast``, optionally limited to certain stations."""
        for elem in self._iter_tag(fp, "kml:Placemark"):
            placemark_desc = self._parse_description(elem)
            if stations is None or placemark_desc in stations:
                yield placemark_desc, self._parse_forecast(elem)

    @staticmethod            
    def convert_to_dataframe(forecasts: Iterator[dict],station:str)-> pd.DataFrame:
        return pd.DataFrame.from_dict(dict(forecasts)[station])
                    
@contextmanager
def kmz_reader(fp: IO[bytes]) -> Generator[IO[bytes], None, None]:
    """
    Wrap reading from *.kmz files, which are merely compressed *.kml (XML) files.
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
    """

    with open(filename, "rb") as fp:
        if compressed is True or (compressed is None and filename.suffix == ".kmz"):
            with kmz_reader(fp) as zp:
                yield zp
        else:
            yield fp

if __name__ == "__main__":
    pass