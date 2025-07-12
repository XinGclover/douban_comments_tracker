import os
from pathlib import Path
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Permission scope: Upload and manage your own Google Drive files (will not touch other people's files)
SCOPES = ['https://www.googleapis.com/auth/drive.file']

def authenticate():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'google/google_credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

def upload_or_update_csv(service, file_path, drive_file_name, folder_id=None):
    # Check whether the file already exists (folder restrictions can be added)
    query = f"name='{drive_file_name}' and mimeType='text/csv' and trashed=false"
    if folder_id:
        query += f" and '{folder_id}' in parents"

    results = service.files().list(
        q=query,
        spaces='drive',
        fields="files(id, name)"
    ).execute()
    files = results.get('files', [])

    media = MediaFileUpload(file_path, mimetype='text/csv', resumable=True)

    file_metadata = {'name': drive_file_name}
    if folder_id:
        file_metadata['parents'] = [folder_id]

    if files:
        file_id = files[0]['id']
        updated_file = service.files().update(
            fileId=file_id,
            media_body=media
        ).execute()
        print(f'[✔] overwrite update: {drive_file_name} (ID: {file_id})')
    else:
        uploaded_file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        print(f'[+] Upload new file: {drive_file_name} (ID: {uploaded_file.get("id")})')

def upload_all_csv_in_directory(local_dir, folder_id=None):
    creds = authenticate()
    service = build('drive', 'v3', credentials=creds)

    csv_files = Path(local_dir).glob('*.csv')
    for csv_file in csv_files:
        upload_or_update_csv(service, str(csv_file), csv_file.name, folder_id)

if __name__ == '__main__':
    # Local directory path
    directory_path = 'export/zhaoxuelu_view_export'
    target_drive_folder_id = '197aS4k8VWk_5p-kbQa_x_ppjnOtEeMtW'
    upload_all_csv_in_directory(directory_path, target_drive_folder_id)
