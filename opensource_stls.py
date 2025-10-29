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

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

API_BASE = "https://api.github.com"
API_HEADERS = {
    "Accept": "application/vnd.github.v3.raw",
    "X-GitHub-Api-Version": "2022-11-28",
}

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

def fetch_private_github_file(
    owner: str = os.getenv("GITHUB_OWNER", "davedavepicks"),
    repo: str = os.getenv("GITHUB_REPO", "pick_db"),
    path: str = os.getenv("GITHUB_PATH", "data/data.csv"),
    ref: str | None = os.getenv("GITHUB_REF", None),
    github_token: str | None = None,
) -> bytes:
    print('Fetching data from github')
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
        credentials, _ = google.auth.load_credentials_from_dict(
            auth_json_dict, scopes=['https://www.googleapis.com/auth/drive.readonly']
        )

        # Build the Drive API service
        drive_service = build('drive', 'v3', credentials=credentials)

        top_folder_meta = drive_service.files().get(fileId=folder_id, fields='id, name, parents').execute()
        folders[top_folder_meta['id']] = top_folder_meta

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
        for file in results.get('files', []): # Changed from [{}] to [] for safety
            if file['mimeType'] == 'application/vnd.google-apps.folder':
                folders[file['id']] = file
            # Corrected mimeType for STL files
            elif file['mimeType'] in ['application/vnd.ms-pki.stl', 'application/sla']:
                stls[file['id']] = file
                
        if recursive:
            if isinstance(depth, int):
                if _level == depth:
                    if any(f['mimeType'] == 'application/vnd.google-apps.folder' for f in results.get('files', [])):
                        print(f'{bcolors.WARNING}Warning: stopping recursion at depth {depth}.{bcolors.ENDC}', end='\r')
                    return stls
            
            current_subfolders = [f for f in results.get('files', []) if f['mimeType'] == 'application/vnd.google-apps.folder']

            if len(current_subfolders) > 0:
                for f in current_subfolders:
                    # The recursive call returns a dictionary of STLs, not folders
                    sub_stls = gdrive_stls(auth_json_dict, f['id'], recursive, depth, _level=_level+1)
                    stls.update(sub_stls) # Merge the dictionaries

        for gd_item in stls.values():
            if 'parent_folder' not in gd_item:
                pid = gd_item['parents'][0]
                # The parent folder should now always be in the folders dictionary
                if pid in folders:
                    gd_item['parent_folder'] = folders[pid]['name']
                else:
                    # This case should be rare, but is good for debugging
                    gd_item['parent_folder'] = 'Unknown'
        return stls

    except Exception as e:
        raise ValueError(f'Folder ID {folder_id} not found on Google Drive. Does not exist, or permissions do not allow read. Original error: {e}')


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
        print(f"{bcolors.OKCYAN}Downloading file: '{file_name}'{bcolors.ENDC}")

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
    input_response = -1
    existing_csvs = False
    if os.path.exists('ddp_stls_list.csv') and os.path.exists('ddp_stls_db.csv'):
        existing_csvs = True
        input_response = input(f'{bcolors.OKGREEN}ddp_stls_list.csv and ddp_stls_db.csv already exist. Do you want to:\n\t1: Use existing files\n\t2: Re-fetch from source\nEnter 1 or 2: {bcolors.ENDC}')
    if input_response == '2' or not existing_csvs:
        
        df = None
        if github_token:
            csv_bytes = fetch_private_github_file(github_token=github_token)
            df = load_csv_from_bytes(csv_bytes)
        else:
            print("GitHub token not found.")

        assert df is not None, sys.exit("Problem getting data from repo.")
        
        df = df[df['Materials'].str.contains('resin', na=False) & (df['Publish'] == True)].drop(columns=['Publish', 'Make time (3d printed)', 'Make time (handmade)', 'Make time (cast)', 'Image folder', 'Methods', 'Materials', 'Tools and consumables', 'Description', 'STL file'], axis=1).dropna()
        df = df[df['Plectrum'] != 'Custom']

        print(f'Found {bcolors.OKCYAN}{len(df)}{bcolors.ENDC} published resin Plectrum designs in the database.')
        print(f'{bcolors.HEADER}Picks in DB:{bcolors.ENDC}')
        for p in df['Plectrum'].to_list():
            print(f'\t- {bcolors.OKGREEN}{p}{bcolors.ENDC}')

        os.makedirs('davedavepicks_stls', exist_ok=True)
        try:
            folder_id = os.environ['GDRIVE_FOLDER_ID']
        except ValueError as e:
            msg = str(e)
            sys.exit(msg + '\nSet GDRIVE_FOLDER_ID.')

        print(f'Fetching STLs from Google Drive.')
        ddp_stls = {}

        ddp_stls = gdrive_stls(
            auth_json_dict=gdrive_auth,
            folder_id=folder_id,
            recursive=True,
            depth=3,
        )

        stldf = pd.DataFrame.from_dict(ddp_stls.values())
        
        print(f'Found {len(stldf)} STL files on Google Drive.')

        stldf = stldf.drop(columns=['parents', 'mimeType'])
        stldf['createdTime'] = pd.to_datetime(stldf['createdTime'], utc=True)
        stldf['modifiedTime'] = pd.to_datetime(stldf['modifiedTime'], utc=True)
        stldf['size'] = stldf['size'].astype('int32')
        stldf.sort_values(by=['modifiedTime'], ascending=False, inplace=True)
        
        stldf.to_csv('ddp_stls_list.csv', index=True)
        df.to_csv('ddp_stls_db.csv', index=True)
        del df, stldf
        
    # Read back in from file to handle both cases
    try:
        stldf = pd.read_csv('ddp_stls_list.csv')
        df = pd.read_csv('ddp_stls_db.csv')
        print(f'Found {bcolors.OKCYAN}{len(df)}{bcolors.ENDC} published resin Plectrum designs in the database.')
        print(f'{bcolors.HEADER}Picks in DB:{bcolors.ENDC}')
        for p in df['Plectrum'].to_list():
            print(f'\t- {bcolors.OKGREEN}{p}{bcolors.ENDC}')
    except Exception as e:
        sys.exit(f'Problem reading ddp_stls_list.csv or ddp_stls_db.csv: {e.with_traceback}')
    if os.path.exists('ddp_stls_opensourced.csv'):
        opensourced_df = pd.read_csv('ddp_stls_opensourced.csv', header=None, names=['id', 'name', 'parent_folder', 'action'])
        stldf = stldf[~stldf['id'].isin(opensourced_df['id'])]
        print(f'{len(opensourced_df)} STLs have previously been opensourced or skipped. {len(stldf)} remain to consider.')
        remove_skipped = input('Would you like to remove previously skipped STLs from consideration? (y/n): ')
        if remove_skipped.lower() == 'y':
            stldf = stldf[~stldf['id'].isin(opensourced_df[opensourced_df['action'] == 'skip']['id'])]
            print(f'After removing skipped STLs, {len(stldf)} remain to consider.')
    else:
        print(f'No STLs have yet been opensourced. {len(stldf)} remain to consider.')
        with open('ddp_stls_opensourced.csv', 'a') as log:
            log.write('id,name,folder,action\n')
        
    # User decision making
    # os.system('rm -rf davedavepicks_stls')
    print(f'{bcolors.WARNING}Starting STL opensourcing process.\nIf you choose to exit, you can continue by choosing existing files next time.\n{bcolors.ENDC}')
    print(f'{bcolors.WARNING}\t> To avoid continuation, delete ddp_stls_list.csv, ddp_stls_db.csv and ddp_stls_opensourced.csv before running again.{bcolors.ENDC}')
    print(f'{bcolors.WARNING}\t> To completely start from scratch, delete the davedavepicks_stls/ folder too.{bcolors.ENDC}')
    for row in stldf.iterrows():
        info = row[1]
        choice = ''
        # print(info)
        print(f'Folder: {bcolors.OKGREEN}{info["parent_folder"]}{bcolors.ENDC}\n STL file: {bcolors.OKCYAN}{info["name"]}{bcolors.ENDC}\n Created: {bcolors.OKCYAN}{info["createdTime"]}{bcolors.ENDC}\n Modified: {bcolors.OKCYAN}{info["modifiedTime"]}{bcolors.ENDC}')  # noqa
        while choice not in ['1', '2', '3']:
            choice = input('Do you want to opensource this STL, skip or exit? \n\t1: Opensource this STL\n\t2: Skip this STL\n\t3: Exit\nEnter 1, 2 or 3: ')
        if choice == '3':
            print('Exiting.')
            sys.exit(0)
        if choice == '1':
            print(f'{bcolors.OKGREEN}{info["name"]} will be opensourced.{bcolors.ENDC}')
            name = input(f'{bcolors.OKCYAN}Enter new name? (currently {info["name"]}):{bcolors.ENDC}')
            if name == '':
                name = info["name"]
            elif '.stl' not in name:
                name = name + '.stl'
            folder = input(f'{bcolors.OKCYAN}Enter new folder name? (currently {info["parent_folder"]}):{bcolors.ENDC}')
            if folder == '':
                folder = info["parent_folder"]
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
            if os.path.exists(readme_path):
                appending_readme = True
            else:
                appending_readme = False
            if draft_description != '':
                with open(readme_path, 'a') as readme:
                    if appending_readme:
                        readme.write('\n\n---\n\n')
                    readme.write(f'# {folder}\n\n')
                    readme.write(f'## {name}\n\n')
                    readme.write(draft_description)
                print(f'{bcolors.OKGREEN}A draft description was found and has been written to {readme_path}.{bcolors.ENDC}')
                print(f'{bcolors.WARNING}Review before committing.{bcolors.ENDC}')
            else:
                with open(readme_path, 'w') as readme:
                    readme.write(f'# {folder}\n\n')
                    readme.write(f'## {name}\n\n')
                    readme.write('Placeholder.')
                print(f'{bcolors.WARNING}A draft description could not be found. A placeholder has been written to {readme_path}.{bcolors.ENDC}')
                print(f'{bcolors.WARNING}Review before committing.{bcolors.ENDC}')
            # Tracking progress
            with open('ddp_stls_opensourced.csv', 'a') as log:
                log.write(f'{info["id"]},{name},{folder},opensource\n')
        elif choice == '2':
            print(f'{bcolors.WARNING}Skipping {info["name"]}.{bcolors.ENDC}')
            with open('ddp_stls_opensourced.csv', 'a') as log:
                log.write(f'{info["id"]},{info["name"]},{info["parent_folder"]},skip\n')

    print(f'{bcolors.OKGREEN}Commit and push to complete the process and opensource the downloaded STLs.{bcolors.ENDC}')

if __name__ == '__main__':
    main()