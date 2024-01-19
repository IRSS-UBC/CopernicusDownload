import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import keyring
import pwinput
import requests
import urllib3.exceptions
from tqdm.autonotebook import tqdm

download_attempts = 10

# %% configuration


destination = Path("output")

start_date_str = "2020-01-01"
end_date_str = "2020-01-31"

# %% constants
serviceName = "odata_dataspace"

# %% setup
if not os.path.exists(destination):
    Path(destination).mkdir(parents=True, exist_ok=True)


# %% functions

def authenticate(username=None):
    refresh_token = None
    authenticated = False
    while not authenticated:
        if username is None:
            username = input("Please enter username:")

        if keyring.get_password(serviceName, username) is None:
            keyring.set_password(serviceName, username, pwinput.pwinput("Please enter your o-data password:"))
        try:
            refresh_token = get_refresh_token(username, keyring.get_password(serviceName, username))
        except ConnectionRefusedError:
            keyring.delete_password(serviceName, username)
            continue
        else:
            print("Authenticated")
            authenticated = True

    return username, refresh_token


def get_refresh_token(username, password):
    # Define the endpoint and parameters
    url = 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'password',
        'username': username,
        'password': password,
        'client_id': 'cdse-public'
    }

    response = requests.post(url, headers=headers, data=data)

    if response.status_code != 200:
        raise ConnectionRefusedError("Error getting token: {}".format(response.json()))
    else:
        return response.json()['refresh_token']


def get_access_token(user, refresh_token, refresh_count=0):
    # Define the endpoint and parameters
    url = 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': 'cdse-public'
    }

    post_response = requests.post(url, headers=headers, data=data)

    if post_response.status_code != 200:
        if post_response.json()['error'] == 'invalid_grant':
            print("Invalid grant, trying to get new refresh token")
            if refresh_count > 10:
                raise ConnectionRefusedError("Error getting token: {}".format(post_response.json()))

            refresh_token = authenticate(user)[1]

            return get_access_token(user, refresh_token, refresh_count + 1)

        else:
            raise ConnectionRefusedError("Error getting token: {}".format(post_response.json()))

    else:
        return post_response.json()['access_token'], refresh_token


def rename_move(file_path, new_extension, new_path):
    try:
        # Split the file path into the base name and the extension
        base_name, current_extension = os.path.splitext(file_path)

        # Generate the new file name with the same base name and the new extension
        new_file_name = base_name + new_extension

        # Rename the file
        os.rename(file_path, new_file_name)

        shutil.move(new_file_name, Path(new_path).resolve())

    # print(f"File successfully renamed with new extension: {new_file_name}")
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
    except FileExistsError:
        print(f"Error: File with new name {new_file_name} already exists.")


# %% Authentication
auth_user, auth_refresh_token = authenticate()

# %% Query the API

spatial_filter = "OData.CSC.Intersects(area=geography'SRID=4326;POLYGON((-140.99778 41.6751050889,-140.99778 83.23324,-52.6480987209 41.6751050889,-52.6480987209 83.23324,-140.99778 41.6751050889))')"
product_filter = "contains(Name,'S3A_SL_2_LST')"

# Convert strings to datetime objects
start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

# Calculate the end date by adding one day
end_date += timedelta(days=1)

# Format the date range in the specified format
temporal_filter = (
    f"ContentDate/Start gt {start_date.isoformat()}Z "
    f"and ContentDate/Start lt {end_date.isoformat()}Z"
)

api_query = (
    "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?"
    f"$filter={spatial_filter} and {product_filter} and {temporal_filter}&$top=50&$orderby=ContentDate/Start desc"
)

products = []

pbar = tqdm(desc="Scanning Files", unit=" files")
depth = 0

lastValue = 0
while True:
    pbar.update(len(products) - lastValue)
    lastValue = len(products)

    depth += 1

    data = requests.get(api_query).json()

    if "value" in data:
        products.extend(data['value'])

    if "@odata.nextLink" in data:
        api_query = data['@odata.nextLink']
    else:
        print("Found no more pages.")
        break

pbar.close()
start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
end_date = datetime.strptime(end_date_str, "%Y-%m-%d")


# %% Download the products


def download_product(product_id, access_token, filename, download_chunk_size=8192):
    try:
        url = f"https://zipper.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
        headers = {"Authorization": f"Bearer {access_token}"}
        session = requests.Session()
        session.headers.update(headers)
        response = session.get(url, headers=headers, stream=True)
        content_size = int(response.headers.get('Content-Length', 0))

        progress_bar = tqdm(desc=f"Downloading {filename}", total=content_size, unit='B', unit_scale=True,
                            unit_divisor=1024, leave=False, miniters=1)
        with open(filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=download_chunk_size):
                if chunk:
                    file.write(chunk)
                    progress_bar.update(len(chunk))
        progress_bar.close()
    except urllib3.exceptions.ProtocolError as e:
        # catch when the connection drops
        return False, e
    except requests.exceptions.ChunkedEncodingError as e:
        # catch when server stops sending chunks... Token expired?
        return False, e
    else:
        return True, None


for product in tqdm(products, desc="Downloading Products", unit="product"):
    auth_access_token, auth_refresh_token = get_access_token(auth_user, auth_refresh_token)
    productID = product['Id']
    productName = product['Name']

    downloaded = False
    downloadAttempt = 0
    while not downloaded:
        if downloadAttempt > download_attempts:
            print(f"Download attempt failed more than {download_attempts} times for product with ID {productID}. "
                  f"Moving to next product")
            continue

        downloaded, ex = download_product(productID, auth_access_token, productName)

        if not downloaded:
            print(f"Download failed for product {productID}{str(ex)}. Trying again...")

        downloadAttempt += 1

    rename_move(productName, ".zip", destination)

# %%
