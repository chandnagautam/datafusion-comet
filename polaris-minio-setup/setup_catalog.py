import json
import time
import urllib.request
import urllib.parse
import urllib.error

POLARIS_URL = "http://localhost:8181"
REALM = "POLARIS"
CLIENT_ID = "admin"
CLIENT_SECRET = "password"
CATALOG_NAME = "demo_catalog"

def wait_for_polaris():
    print("Waiting for Polaris to start...")
    url = f"{POLARIS_URL}/"
    while True:
        try:
            # Send a simple GET request to check if the server is up
            urllib.request.urlopen(url, timeout=2)
            print("Polaris server is up.")
            break
        except urllib.error.HTTPError as e:
            # If the server responds with any HTTP error code (e.g. 404), it is active and listening!
            print(f"Polaris server is up (received HTTP status {e.code}).")
            break
        except Exception:
            # Connection refused or timeout means it is not up yet
            time.sleep(1)

def get_token():
    print("Requesting OAuth token from Polaris...")
    url = f"{POLARIS_URL}/api/catalog/v1/oauth/tokens"
    headers = {
        "Polaris-Realm": REALM,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "PRINCIPAL_ROLE:ALL"
    }).encode("utf-8")

    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req) as response:
            res = json.loads(response.read().decode())
            token = res["access_token"]
            print("Token successfully retrieved.")
            return token
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"Failed to get token. HTTP {e.code}: {body}")
        raise
    except Exception as e:
        print(f"Failed to get token: {e}")
        raise

def create_catalog(token):
    print(f"Creating catalog '{CATALOG_NAME}'...")
    url = f"{POLARIS_URL}/api/management/v1/catalogs"
    headers = {
        "Authorization": f"Bearer {token}",
        "Polaris-Realm": REALM,
        "Content-Type": "application/json"
    }
    payload = {
        "catalog": {
            "name": CATALOG_NAME,
            "type": "INTERNAL",
            "properties": {
                "default-base-location": "s3://warehouse",
                "s3.endpoint": "http://localhost:9000",
                "s3.path-style-access": "true",
                "s3.access-key-id": "admin",
                "s3.secret-access-key": "password",
                "s3.region": "us-east-1",
                "client.region": "us-east-1"
            },
            "storageConfigInfo": {
                "storageType": "S3",
                "endpoint": "http://localhost:9000",
                "pathStyleAccess": True,
                "allowedLocations": [
                    "s3://warehouse",
                    "s3://warehouse/*"
                ]
            }
        }
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req) as response:
            res = json.loads(response.read().decode())
            print(f"Catalog '{CATALOG_NAME}' created successfully!")
            print(json.dumps(res, indent=2))
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"Failed to create catalog. HTTP {e.code}: {body}")
    except Exception as e:
        print(f"Failed to create catalog: {e}")

if __name__ == "__main__":
    wait_for_polaris()
    try:
        token = get_token()
        create_catalog(token)
    except Exception:
        pass
