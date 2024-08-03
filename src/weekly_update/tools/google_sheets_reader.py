from google.oauth2 import service_account
from googleapiclient.discovery import build

def read_all_sheets(service_account_file, spreadsheet_id):

    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly'] # Optional

    credentials = service_account.Credentials.from_service_account_file(service_account_file, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=credentials)
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    # Get data from the first sheet
    first_sheet_name = spreadsheet['sheets'][0]['properties']['title']

    result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=first_sheet_name).execute()
    values = result.get('values', [])

    return {first_sheet_name: values}
