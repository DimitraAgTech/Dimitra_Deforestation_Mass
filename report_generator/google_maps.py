import os
import uuid

import polyline
import requests

from config import GOOGLE_MAP_API_DEV_KEY


def reverse_coords(coords):
    new_coords = []
    for coord in coords:
        lat = coord["latitude"]
        lon = coord["longitude"]
        new_coords.append((lat, lon))

    return new_coords


def download_coords_map_image(coords):
    coords = reverse_coords(coords)
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
