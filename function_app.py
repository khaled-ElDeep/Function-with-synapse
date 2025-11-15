import azure.functions as func
import logging
import json
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="synapseTrigger")
def synapseTrigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    account_name = 'dlstorage231'
    account_key = 'cmlLV/zsISU9znwChNrxvlOWa9Sh79s6jXfRmdNcfWh8x7V4UaqHB/NRRP2QnZIRsYtpiX93XKjx+AStrbDMWA=='
    account_url = f"https://{account_name}.dfs.core.windows.net"
    file_system_name = 'filetransfer'

    service_client = DataLakeServiceClient(account_url, credential=account_key)
    file_system_client = service_client.get_file_system_client(file_system=file_system_name)

    paths = file_system_client.get_paths()
    files_json =[]
    for path in paths:
        files_json.append({
            "name": path.name,
            "is_directory": path.is_directory,
            "last_modified": str(path.last_modified)
            
        })
    result = json.dumps(files_json, indent=4)
    return result

