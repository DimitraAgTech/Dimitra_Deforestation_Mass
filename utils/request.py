import json
import os

from constants import PENDING
from database import db, models
from utils.s3 import download_s3_object


def get_pending_mass_request():
    mass_request = db.session.query(models.DeforestationRequest).filter_by(
        status=PENDING).first()
    return mass_request


def update_mass_request_status(mass_request, status):
    mass_request.status = status
    db.session.commit()

    return mass_request


def get_request_data(request_id):
    s3_key = f"mass_deforestation/{request_id}/input.json"

    os.makedirs("temp", exist_ok=True)
    temp_path = os.path.join("temp", "input.json")

    download_s3_object(s3_key, temp_path)

    with open(temp_path) as f:
        return json.load(f)
