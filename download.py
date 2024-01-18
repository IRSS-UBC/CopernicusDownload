import os
from datetime import datetime

import requests
import tqdm

# %%
username = "<enter username>"
password = "<enter password>"

start_date_str = "2016-04-25"
end_date_str = "2016-04-27"

# %%

spatial_filter = "OData.CSC.Intersects(area=geography'SRID=4326;POLYGON((-140.99778 41.6751050889,-140.99778 83.23324,-52.6480987209 41.6751050889,-52.6480987209 83.23324,-140.99778 41.6751050889))')"
product_filter = "contains(Name,'S3A_SL_2_LST')"

api_query = (
    "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?"
    f"$filter={spatial_filter} and {product_filter}"
)

data = requests.get(api_query).json()


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
        raise Exception("Error getting token: {}".format(response.json()))
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


start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

filtered_products = [
    product for product in data['value']
    if start_date <= datetime.strptime(product['ContentDate']['Start'], "%Y-%m-%dT%H:%M:%S.%fZ") <= end_date
]

# %%

refreshToken, accessToken = get_refresh_token(username, password)

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

    print(response.status_code)

    with open(product['Name'], "wb") as file:
        for chunk in tqdm.tqdm(response.iter_content(chunk_size=8192), desc=f"Downloading {product['Name']}", unit="chunk"):
            if chunk:
                file.write(chunk)

    rename_extension(product['Name'], ".zip")


# %%
