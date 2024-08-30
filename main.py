from constants import IN_PROGRESS
from utils.request import (get_pending_mass_request, get_request_data,
                           update_mass_request_status)


def main():
    mass_request = get_pending_mass_request()
    data = get_request_data(mass_request.id)

    mass_request = update_mass_request_status(mass_request, IN_PROGRESS)

    print("DATA :: ", data)


main()
