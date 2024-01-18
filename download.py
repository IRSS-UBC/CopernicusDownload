import os
from datetime import datetime, timedelta

import keyring
import requests
import tqdm

import pwinput

download_chunk_size = 8192

serviceName = "odata_dataspace"
start_date_str = "2020-01-01"
end_date_str = "2020-01-03"


# %%

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
        return response.json()['refresh_token'], response.json()['access_token']


def get_token(refresh_token):
    # Define the endpoint and parameters
    url = 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': 'cdse-public'
    }

    response = requests.post(url, headers=headers, data=data)

    if response.status_code != 200:
        raise Exception("Error getting token: {}".format(response.json()))
    else:
        return response.json()['access_token']


def rename_extension(file_path, new_extension):
    try:
        # Split the file path into the base name and the extension
        base_name, current_extension = os.path.splitext(file_path)

        # Generate the new file name with the same base name and the new extension
        new_file_name = base_name + new_extension

        # Rename the file
        os.rename(file_path, new_file_name)

        print(f"File successfully renamed with new extension: {new_file_name}")
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
    except FileExistsError:
        print(f"Error: File with new name {new_file_name} already exists.")


# %%

authenticated = False
while not authenticated:
    username = input("Please enter username:")

    if keyring.get_password(serviceName, username) is None:
        keyring.set_password(serviceName, username, pwinput.pwinput("Please enter your odata password:"))

    try:
        refreshToken, accessToken = get_refresh_token(username, keyring.get_password(serviceName, username))
    except ConnectionRefusedError:
        keyring.delete_password(serviceName, username)
        continue
    else:
        print("Authenticated")
        authenticated = True

# %%

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

products = []

depth = 0
while True:
    print(f"Querying depth {depth}")
    depth += 1
    api_query = (
        "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?"
        f"$filter={spatial_filter} and {product_filter} and {temporal_filter}&$top=20&$orderby=ContentDate/Start desc"
    )
    data = requests.get(api_query).json()

    products.append(data['value'])

    if "@odata.nextLink" in data:
        api_query = data['@odata.nextLink']
    else:
        print("Found no more pages.")
        break

#%%

start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

filtered_products = [
    product for product in products
]

# %%

for product in tqdm.tqdm(filtered_products, desc="Downloading Products", unit="product"):
    accessToken = get_token(refreshToken)
    productID = product['Id']

    # Download the products

    url = f"https://zipper.dataspace.copernicus.eu/odata/v1/Products({productID})/$value"

    headers = {"Authorization": f"Bearer {accessToken}"}

    session = requests.Session()
    session.headers.update(headers)
    response = session.get(url, headers=headers, stream=True)

    content_size = int(response.headers.get('Content-Length', 0))

    with open(product['Name'], "wb") as file:
        for chunk in tqdm.tqdm(response.iter_content(chunk_size=download_chunk_size),
                               desc=f"Downloading {product['Name']}",
                               unit="chunk", total=content_size / download_chunk_size, leave=False):
            if chunk:
                file.write(chunk)

    rename_extension(product['Name'], ".zip")

# %%
