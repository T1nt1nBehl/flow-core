import os
import sys
import requests


# You can override these using environment variables
NETBOX_URL = os.getenv("NETBOX_URL", "https://your-netbox-url")
NETBOX_TOKEN = os.getenv("NETBOX_TOKEN", "YOUR_NETBOX_API_TOKEN")


def test_netbox_connection():
    if not NETBOX_URL or NETBOX_URL == "https://your-netbox-url":
        print("Please set NETBOX_URL (env var) to your NetBox base URL.")
        sys.exit(1)

    if not NETBOX_TOKEN or NETBOX_TOKEN == "YOUR_NETBOX_API_TOKEN":
        print("Please set NETBOX_TOKEN (env var) to a valid NetBox API token.")
        sys.exit(1)

    status_url = NETBOX_URL.rstrip("/") + "/api/status/"
    headers = {
        "Authorization": f"Token {NETBOX_TOKEN}",
        "Accept": "application/json",
    }

    try:
        response = requests.get(status_url, headers=headers, timeout=10)
    except requests.exceptions.RequestException as exc:
        print(f"Error connecting to NetBox: {exc}")
        sys.exit(1)

    if response.status_code == 200:
        try:
            data = response.json()
        except ValueError:
            print("Connected, but response is not valid JSON.")
            print("Raw response:", response.text)
            sys.exit(1)

        version = data.get("netbox-version") or data.get("version")
        print("Successfully connected to NetBox.")
        if version:
            print(f"NetBox version: {version}")
        sys.exit(0)
    else:
        print(f"Failed to connect. HTTP {response.status_code}")
        print("Response body:", response.text)
        sys.exit(1)


if __name__ == "__main__":
    test_netbox_connection()