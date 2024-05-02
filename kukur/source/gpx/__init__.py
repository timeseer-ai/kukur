"""Read GPX files to Apache Arrow tables."""

import math
from collections import defaultdict
from xml.etree import ElementTree

from dateutil.parser import isoparse
from pyarrow import Table, array, concat_tables, float64, int64
from pyarrow import compute as pc

from kukur.exceptions import KukurException

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

NS = {
    "gpx": "http://www.topografix.com/GPX/1/1",
}


class UnexpectedGPXException(KukurException):
    """Raised when the GPX file is not as expected."""

    def __init__(self, message: str):
        super().__init__(message)


def _remove_ns(tag: str) -> str:
    if "}" in tag:
        return tag.split("}")[1]
    return tag


def parse_gpx(readable) -> Table:  # noqa: PLR0912
    """Parse a GPX file to a PyArrow Table.

    This concatenates all track segments of all tracks.
    Elevation and all extension data points of the first trackpoint will be included as columns.
    """
    tables = []

    tree = ElementTree.parse(readable)
    root = tree.getroot()
    for track in root.findall("gpx:trk", NS):
        metadata = {}
        for element in track:
            if len(element) == 0:
                metadata[_remove_ns(element.tag)] = element.text

        required_data: dict = defaultdict(list)
        data: dict = defaultdict(list)
        is_first = True
        for segment in track.findall("gpx:trkseg", NS):
            for point in segment.findall("gpx:trkpt", NS):
                ts = point.find("gpx:time", NS)
                if ts is None:
                    continue
                if ts.text is None:
                    raise UnexpectedGPXException("No text in time")

                if is_first:
                    is_first = False
                    elevation = point.find("gpx:ele", NS)
                    if elevation is not None and elevation.text is not None:
                        data["{http://www.topografix.com/GPX/1/1}ele"].append(
                            elevation.text
                        )
                    else:
                        data["{http://www.topografix.com/GPX/1/1}ele"] = None

                    extensions = point.find("gpx:extensions", NS)
                    if extensions is not None:
                        for element in extensions.iter():
                            if len(element) == 0:
                                if element.text is not None:
                                    data[element.tag].append(element.text)
                else:
                    for tag in data:
                        for field in point.iter():
                            if field.tag == tag:
                                data[tag].append(field.text)
                                break
                        else:
                            data[tag].append(None)

                required_data["ts"].append(isoparse(ts.text))

                lon = float(point.attrib["lon"])
                required_data["lon"].append(lon)
                required_data["lon_rad"].append(math.radians(lon))

                lat = float(point.attrib["lat"])
                required_data["lat"].append(lat)
                required_data["lat_rad"].append(math.radians(lat))

        required_data.update(data)

        table = Table.from_pydict({_remove_ns(k): v for k, v in required_data.items()})
        for k in reversed(metadata):
            tag = array([metadata[k]] * len(table))
            table = table.add_column(0, k, tag)

        table = table.set_column(
            table.schema.get_field_index("ele"), "ele", table["ele"].cast(float64())
        )
        tables.append(table)
    return _calculate_additional_columns(concat_tables(tables))


def _calculate_additional_columns(table: Table) -> Table:
    """Add additional columns to the table, prefixed by "calc_".

    This calculates the distance between data points and the cumulative distance.
    The calculation should probably be done on a smoothed version of the positions,
    because the numbers are higher than they should be.

    The total activity time is also calculated.

    The instantaneous speed is calculated based on those columns.

    Loosely based on https://stackoverflow.com/a/45851135
    """
    earth_radius = 6_378_137
    x = pc.multiply(
        pc.multiply(earth_radius, pc.cos(table["lon_rad"])), pc.sin(table["lat_rad"])
    )
    y = pc.multiply(
        pc.multiply(earth_radius, pc.sin(table["lon_rad"])), pc.sin(table["lat_rad"])
    )
    z = pc.multiply(earth_radius, pc.cos(table["lat_rad"]))

    dx = pc.pairwise_diff(x.combine_chunks())
    dy = pc.pairwise_diff(y.combine_chunks())
    dz = pc.pairwise_diff(z.combine_chunks())

    distance = pc.sqrt(
        pc.add(pc.add(pc.power(dx, 2), pc.power(dy, 2)), pc.power(dz, 2))
    )
    distance = pc.fill_null(distance, 0)

    table = table.append_column("calc_distance", distance)
    table = table.append_column("calc_distance_unit", array(["m"] * len(table)))
    table = table.append_column("calc_total_distance", pc.cumulative_sum(distance))

    seconds = pc.divide(
        pc.fill_null(
            pc.pairwise_diff(table["ts"].combine_chunks().cast(int64())),
            0,
        ),
        1000000,
    )

    speed = pc.divide(pc.multiply(pc.divide(distance, seconds), 3600), 1000)

    table = table.append_column(
        "calc_time",
        pc.cumulative_sum(seconds),
    )
    table = table.append_column("calc_time_unit", array(["s"] * len(table)))

    table = table.append_column("calc_speed", speed)
    table = table.append_column("calc_speed_unit", array(["km/h"] * len(table)))

    return table.drop_columns(["lon_rad", "lat_rad"])
