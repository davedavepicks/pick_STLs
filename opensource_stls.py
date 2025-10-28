#!/usr/bin/env python
# coding: utf-8

import sys
import os
import io
import requests
import pandas as pd
import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import json
from tqdm import tqdm

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

def gdrive_stls(auth_json_dict: dict, folder_id: str, recursive: bool = False, depth: str|int = 'full', _level: int = 0) -> dict:
    """
    Finds STL files in a Google Drive folder using the Drive API.
    Optionally recursive down to a given numerical depth or 'full' depth.

    Args:
        auth_json_dict: A dictionary containing the JSON-formatted Google Drive
                        service account or OAuth 2.0 client credentials.
        folder_id: The ID of the Google Drive folder to list files from.
        recursive: If True, lists files in subfolders recursively.
        depth: Numerical depth or 'full' (default = 'full').
        _level: used internally during recursion.
    Returns:
        A dictionary of STL files in the folder.
    """

    stls = {}
    folders = {}

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
                folders[file['id']] = file
            elif file['mimeType'] == 'application/vnd.ms-pki.stl':
                stls[file['id']] = file
        
        if recursive:
            if isinstance(depth, int):
                if _level == depth:
                    if len(folders) > 0:
                        print(f'Warning: stopping recursion at folder {folders[-1]['name']} due to depth of {depth} despite {len(folders)} folders remaining.', end='\r')
                    return stls
            if len(folders) > 0:
                for f in folders:
                    subfolders, sub_stls = gdrive_stls(auth_json_dict, f['id'], recursive, depth, _level=_level+1)  # , sub_items
                    if len(subfolders) == 0:
                        pass
                        print(f'No subfolders remaining in folder {f['name']} at level {_level+1}.                  ', end='\r')

                    for sf in subfolders:
                        folders[sf['id']] = sf
                    for sstl in sub_stls:
                        stls[sstl['id']] = sstl

        for gd_item in {**stls, **folders}.values():
            if 'parent_folder' not in gd_item.keys():
                pid = gd_item['parents'][0]
                for folder in folders:
                    parent_folder = ''
                    if folder['id'] == pid:
                        parent_folder = folder['name']
                        gd_item['parent_folder'] = parent_folder
                        break
                continue

        return stls  #, items

    except Exception as e:
        raise ValueError(f'Folder ID {folder_id} not found on Google Drive. Does not exist, or permissions do not allow read.')


def download_drive_file(auth_json_dict: dict, drive_file_url: str, output_filepath: str):
    """
    Downloads a file from Google Drive using the Drive API.

    Args:
        auth_json_dict: A dictionary containing the JSON-formatted Google Drive
                        service account or OAuth 2.0 client credentials.
        drive_file_url: The URL of the Google Drive file to download,
                        e.g., 'https://drive.google.com/file/d/XXXXXXXXX_XXXXXX_XXXXXXXXX-XXXXXX'.
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
        
        drive_service = build('drive', 'v3', credentials=credentials)

        # Request file metadata to get the file name and type (optional, but good practice)
        file_metadata = drive_service.files().get(fileId=file_id, fields='name, size').execute()
        file_name = file_metadata.get('name')
        file_size = int(file_metadata.get('size', 0))
        print(f"Downloading file: '{file_name}'")

        # Download the file
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.FileIO(output_filepath, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        
        with tqdm(total=file_size, unit='B', unit_scale=True, desc=file_name, ascii=True) as pbar:
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    pbar.update(status.resumable_progress - pbar.n)

        print(f"File '{file_name}' downloaded successfully to '{output_filepath}'")

    except ValueError as ve:
        print(f"Error: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def main():

    try:
        github_token_path = os.environ['GITHUB_TOKEN_PATH']
    except ValueError as e:
        msg = str(e)
        sys.exit(msg + "\nSet GITHUB_TOKEN_PATH")
    
    github_token = None
    try:
        with open(github_token_path) as f:
            github_token = f.read().strip()
    except Exception as e:
        sys.exit(str(e))
    try:
        assert github_token is not None
    except AssertionError as e:
        sys.exit(str(e))
    
    try:
        gdrive_auth_path = os.environ['GDRIVE_AUTH_PATH']
    except ValueError as e:
        msg = str(e)
        sys.exit(msg + '\nSet GDRIVE_AUTH_PATH.')

    gdrive_auth = None
    try:
        with open(gdrive_auth_path) as f:
            gdrive_auth = json.load(f)
    except Exception as e:
        sys.exit(str(e))
    try:
        assert isinstance(gdrive_auth, dict)
    except AssertionError as e:
        sys.exit(str(e))    
    df = None
    if github_token:
        csv_bytes = fetch_private_github_file(github_token=github_token)
        df = load_csv_from_bytes(csv_bytes)
    else:
        print("GitHub token not found.")

    assert df is not None, sys.exit("Problem getting data from repo.")
    
    df = df[df['Materials'].str.contains('resin', na=False) & (df['Publish'] == True)].drop(columns=['Publish', 'Make time (3d printed)', 'Make time (handmade)', 'Make time (cast)', 'Image folder']).dropna()

    os.makedirs('davedavepicks_stls', exist_ok=True)
    try:
        folder_id = os.environ['GDRIVE_FOLDER_ID']
    except ValueError as e:
        msg = str(e)
        sys.exit(msg + '\nSet GDRIVE_FOLDER_ID.')
        
    ddp_stls = {}

    ddp_stls = gdrive_stls(  #, ddp_items
        auth_json_dict=gdrive_auth,
        folder_id=folder_id,
        recursive=True,
        depth=3,
    )

    stldf = pd.DataFrame.from_dict(ddp_stls).drop(columns=['parents', 'mimeType'])
    stldf['createdTime'] = pd.to_datetime(stldf['createdTime'], utc=True)
    stldf['modifiedTime'] = pd.to_datetime(stldf['modifiedTime'], utc=True)
    stldf['size'] = stldf['size'].astype('int32')
    stldf.sort_values(by=['modifiedTime'], ascending=False, inplace=True)

    # User decision making
    os.system('rm -rf davedavepicks_stls')
    for row in stldf.iterrows():
        info = row[1]
        # print(info)
        print(f'Folder: {info["parent_folder"]}\n STL file: {info["name"]}\n Created: {info["createdTime"]}\n Modified: {info["modifiedTime"]}')  # noqa
        keep = input('Do you want to opensource this STL? (y/n)')
        if keep == 'y':
            print(f'{info["name"]} will be opensourced.')
            name = input(f'Enter new name? (currently {info["name"]}):')
            if name == '':
                name = info["name"]
            elif '.stl' not in name:
                name = name + '.stl'
            folder = input(f'Enter new folder name? (currently {info["parent_folder"]}):')
            if folder == '':
                folder = info["folder"]
            try:
                draft_description = df[df['Plectrum'].str.lower() == name.replace('.stl','')]['Long Description'].iloc[0].values
            except Exception as _:
                draft_description = ''
            
            os.makedirs(f'davedavepicks_stls/{folder}', exist_ok=True)
            download_drive_file(
                auth_json_dict=gdrive_auth,
                drive_file_url='https://drive.google.com/file/d/' + info["id"],
                output_filepath=f"davedavepicks_stls/{folder}/{name}"
            )
            readme_path = f'davedavepicks_stls/{folder}/README.md'
            if draft_description != '':
                with open(readme_path, 'w') as readme:
                    readme.write(f'# {folder}\n\n')
                    readme.write(f'## {name}\n\n')
                    readme.write(draft_description)
                print(f'A draft description was found and has been written to {readme_path}.')
                print('Review before committing.')
            else:
                with open(readme_path, 'w') as readme:
                    readme.write(f'# {folder}\n\n')
                    readme.write(f'## {name}\n\n')
                    readme.write('Placeholder.')
                print(f'A draft description could not be found. A placeholder has been written to {readme_path}.')
                print('Review before committing.')
        else:
            print(f'Skipping {info['name']}.')
    
    print('Commit and push to complete the process and opensource the downloaded STLs.')
