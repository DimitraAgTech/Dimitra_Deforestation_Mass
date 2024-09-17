import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from report_generator.google_maps import download_coords_map_image
from report_generator.report_file import ReportGenerator
from utils.s3 import download_s3_object, upload_s3_object


def download_deforestation_image(image_key):
    temp_path = os.path.join("temp", f"{uuid.uuid4()}.png")
    download_s3_object(image_key, temp_path)
    return temp_path


def process_item(item, item_id_map):
    id = item.get("id")
    name = item.get("name")
    coords = item_id_map[id].get("coordinates")

    google_image_path = download_coords_map_image(coords)
    deforestation_image_path = download_deforestation_image(
        item["result"]["finalDetectionS3Key"]
    )

    result = item["result"]
    result["deforestation_image"] = deforestation_image_path
    result["google_polygon_image"] = google_image_path
    result["polygon_name"] = name

    return result


def get_results_from_data(item_id_map, data):
    results = []

    with ThreadPoolExecutor(max_workers=20) as executor:
        # Submit each item to be processed in parallel
        future_to_item = {
            executor.submit(process_item, item, item_id_map): item for item in data
        }

        for future in as_completed(future_to_item):
            try:
                result = future.result()
                results.append(result)
            except Exception as exc:
                print(f"An error occurred: {exc}")

    return results


def generate_report_and_upload_data(request_id, item_id_map, data):
    results = get_results_from_data(item_id_map, data)

    generator = ReportGenerator(results)
    pdf_path, filename = generator.generate()
    report_key = os.path.join("bulk_reports", filename)

    upload_s3_object(pdf_path, report_key)

    s3_key = f"mass_deforestation/{request_id}/output.json"
    temp_path = os.path.join("temp", "output.json")

    with open(temp_path, "w") as f:
        json.dump({"report_key": report_key}, f)

    upload_s3_object(temp_path, s3_key)
