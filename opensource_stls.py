import os
import io
import requests
import pandas as pd
import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.credentials import Credentials as GoogleCredentials
import json

API_BASE = "https://api.github.com"
API_HEADERS = {
    "Accept": "application/vnd.github.v3.raw",
    "X-GitHub-Api-Version": "2022-11-28",
}

def fetch_private_github_file(
    owner: str = os.getenv("GITHUB_OWNER", "davedavepicks"),
    repo: str = os.getenv("GITHUB_REPO", "pick_db"),
    path: str = os.getenv("GITHUB_PATH", "data/data.csv"),
    ref: str | None = os.getenv("GITHUB_REF", None),
    github_token: str | None = None,
) -> bytes:
    token = github_token or os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("Set GITHUB_TOKEN (PAT with repo scope or Contents:Read on the repo).")

    headers = {**API_HEADERS, "Authorization": f"token {token}"}

    # Resolve default branch if not provided
    if not ref:
        repo_resp = requests.get(f"{API_BASE}/repos/{owner}/{repo}", headers=headers, timeout=30)
        if repo_resp.status_code == 404:
            raise RuntimeError(f"Repo not found or no access: {owner}/{repo}. Check org SSO and token scopes.")
        repo_resp.raise_for_status()
        ref = repo_resp.json()["default_branch"]

    url = f"{API_BASE}/repos/{owner}/{repo}/contents/{path}?ref={ref}"
    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code == 404:
        hint = (
            "File not found or no access. Verify:\n"
            f"- owner/repo: {owner}/{repo}\n"
            f"- branch/ref: {ref}\n"
            f"- path: {path}\n"
            "- PAT has repo scope (or fine‑grained: Contents: Read) and access to this repo\n"
            "- If the org enforces SSO, the PAT is Authorized for the org (Configure SSO on the token)."
        )
        raise RuntimeError(hint)
    resp.raise_for_status()
    return resp.content  # raw bytes of the file

def load_csv_from_bytes(content: bytes) -> pd.DataFrame:
    return pd.read_csv(io.BytesIO(content))

def ls_drive_folder(auth_json_dict: dict, folder_id: str, recursive: bool = False, depth: str|int = 'full', level: int = 0) -> tuple[list[dict],list[dict]]:  #,list[dict]]:
    """
    Lists files in a Google Drive folder using the Drive API.

    Args:
        auth_json_dict: A dictionary containing the JSON-formatted Google Drive
                        service account or OAuth 2.0 client credentials.
        folder_id: The ID of the Google Drive folder to list files from.
        recursive: If True, lists files in subfolders recursively (not implemented yet).
    Returns:
        A list of dictionaries, each representing a file in the folder.
    """
    # items = []
    stls = []
    folders = []

    try:
        # Authenticate using the provided JSON dictionary
        credentials = google.auth.load_credentials_from_dict(
            auth_json_dict,
        )[0]

        # Build the Drive API service
        drive_service = build('drive', 'v3', credentials=credentials)

        # Query to list files in the specified folder
        query = f"'{folder_id}' in parents and trashed = false"
        # Query to list files, including pagination support
        page_token = None
        results = {'files': []}
        # top level folder
        while True:
            response = drive_service.files().list(
            q=query,
            spaces='drive',
            fields='nextPageToken, files(id, name, mimeType, modifiedTime, createdTime, size, parents)',
            pageToken=page_token
            ).execute()
            results['files'].extend(response.get('files', []))
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break
        # recursion through subfolders
        for file in results.get('files', [{}]):
            if file['mimeType'] == 'application/vnd.google-apps.folder':
                folders.append(file)
            elif file['mimeType'] == 'application/vnd.ms-pki.stl':
                stls.append(file)
            # else:
            #     items.append(file)
        
        if recursive:
            if isinstance(depth, int):
                if level == depth:
                    if len(folders) > 0:
                        print(f'Warning: stopping recursion at folder {folders[-1]['name']} due to depth of {depth} despite {len(folders)} folders remaining.', end='\r')
                    return folders, stls  #, items
            if len(folders) > 0:
                for f in folders:
                    subfolders, sub_stls = ls_drive_folder(auth_json_dict, f['id'], recursive, depth, level=level+1)  # , sub_items
                    if len(subfolders) == 0:
                        pass
                        # print(f'No subfolders remaining in folder {f['name']} at level {level+1}.                  ', end='\r')
                    folders.extend(subfolders)
                    stls.extend(sub_stls)
                    # items.extend(sub_items)
        # print('\nDone.')
        
        for item in stls+folders:
            if 'parent_folder' not in item.keys():
                pid = item['parents'][0]
                print(pid)
                for folder in folders:
                    parent_folder = ''
                    if folder['id'] == pid:
                        print(folder['id'])
                        print(folder['name'])
                        parent_folder = folder['name']
                        item['parent_folder'] = parent_folder
                        break
                continue

        return folders, stls  #, items

    except Exception as e:
        print(f"An error occurred while listing files in the folder: {e}")
        return [{}], [{}]  #, [{}]


def download_drive_file(auth_json_dict: dict, drive_file_url: str, output_filepath: str):
    """
    Downloads a file from Google Drive using the Drive API.

    Args:
        auth_json_dict: A dictionary containing the JSON-formatted Google Drive
                        service account or OAuth 2.0 client credentials.
        drive_file_url: The URL of the Google Drive file to download,
                        e.g., 'https://drive.google.com/file/d/1zcGAGkwa_QnVY1_tqsgfQbfEE-cKWLS3'.
        output_filepath: The local path where the downloaded file will be saved.
    """
    try:
        # Extract file ID from the URL
        file_id = None
        if "file/d/" in drive_file_url:
            parts = drive_file_url.split("file/d/")
            if len(parts) > 1:
                file_id_with_params = parts[1].split('/')[0]
                file_id = file_id_with_params.split('?')[0] # Remove any query parameters

        if not file_id:
            raise ValueError("Could not extract file ID from the provided Google Drive URL.")

        # Authenticate using the provided JSON dictionary
        # For service accounts, use `from_service_account_info`.
        # For OAuth 2.0 client credentials (user-based), you'd typically need a refresh token
        # or an interactive flow if the token expires. Assuming a service account or
        # previously obtained valid credentials.

        # If using a service account:
        credentials = google.auth.load_credentials_from_dict(
            auth_json_dict,
        )[0]
        
        # Build the Drive API service
        drive_service = build('drive', 'v3', credentials=credentials)

        # Request file metadata to get the file name and type (optional, but good practice)
        file_metadata = drive_service.files().get(fileId=file_id, fields='name').execute()
        file_name = file_metadata.get('name')
        print(f"Downloading file: '{file_name}'")

        # Download the file
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.FileIO(output_filepath, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}%.", end="\r")

        print(f"File '{file_name}' downloaded successfully to '{output_filepath}'")

    except ValueError as ve:
        print(f"Error: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

