from typing import Any, Optional
from crewai_tools import BaseTool
from weekly_update.tools.google_sheets_reader import read_all_sheets

class GoogleSheetTool(BaseTool):
    name: str = "Google Sheet Reader tool"
    description: str = "This tool reads data from Google Sheet API."

    cred_file: Optional[str] = None
    spreadsheet_id: Optional[str] = None

    def __init__(self, cred_file: Optional[str] = None, spreadsheet_id: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        if cred_file is not None:
            self.cred_file = cred_file
        if spreadsheet_id is not None:
            self.spreadsheet_id = spreadsheet_id
                    
    def _run(self, **kwargs: Any) -> Any:
        cred_file = kwargs.get('cred_file', self.cred_file)
        spreadsheet_id = kwargs.get('spreadsheet_id', self.spreadsheet_id)
        return read_all_sheets(cred_file, spreadsheet_id)

