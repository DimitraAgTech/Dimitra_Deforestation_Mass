import math
import os
import uuid

import polyline
import requests

from config import GOOGLE_MAP_API_DEV_KEY


def map_coords_to_tuple(coords):
    new_coords = []
    for coord in coords:
        lat = coord["latitude"]
        lon = coord["longitude"]
        new_coords.append((lat, lon))

    return new_coords


def generate_circle_coordinates(lat, lon, radius_km, num_points=50):
    coordinates = []
    radius_m = radius_km * 1000
    for i in range(num_points):
        angle = 2 * math.pi * i / num_points
        d_lat = (radius_m / 6371000) * math.cos(angle)
        d_lon = (radius_m / 6371000) * math.sin(angle) / \
            math.cos(math.radians(lat))
        new_lat = lat + math.degrees(d_lat)
        new_lon = lon + math.degrees(d_lon)
        coordinates.append({"latitude": new_lat, "longitude": new_lon})

    coordinates.append(coordinates[0])
    return coordinates


def download_coords_map_image(coords):
    coords = map_coords_to_tuple(coords)
    encoded_path = polyline.encode(coords)

    path_options = "weight:4|color:0x00f0f0|enc:"

    params = {
        "size": "512x512",
        "maptype": "hybrid",
        "path": f"{path_options}{encoded_path}",
        "key": GOOGLE_MAP_API_DEV_KEY,
    }
    response = requests.get(
        "https://maps.googleapis.com/maps/api/staticmap", params=params
    )

    output_path = os.path.join("temp", f"{uuid.uuid4()}.png")
    with open(output_path, "wb") as f:
        f.write(response.content)

    return output_path
