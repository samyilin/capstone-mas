from typing import Optional, Any
import pandas as pd
from crewai_tools import BaseTool
import sqlite3

# from os import listdir
from os.path import isfile, join, isdir


class GoogleSheetTool(BaseTool):
    name: str = "CSV extraction tool"
    description: str = (
        "This tool extracts data from CSV files and land them to SQL database."
    )

    # SQLite connection
    sql_connection: Optional[str] = None
    # CSV folders' location
    csv_folder: Optional[str] = None
    # file name for CSV files that will be loaded
    files: Optional[list] = None

    def __init__(
        self,
        sql_connection: Optional[str] = None,
        csv_folder: Optional[str] = None,
        files: Optional[list] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if sql_connection is not None:
            self.sql_connection = sql_connection
        if csv_folder is not None:
            self.csv_folder = csv_folder
        self.files = files

    def _run(self, **kwargs: Any) -> Any:
        # Examine if the 3 settings have been set correctly
        csv_folder = kwargs.get("csv_folder", self.cred_file)
        if isdir(csv_folder) is False:
            raise Exception("Folder is not present")

        files = kwargs.get("files", self.files)
        for file in files:
            if isfile(join(csv_folder, file)) is False:
                raise Exception("File upload incomplete, missing some files")
        sql_connection = kwargs.get("sql_connection", self.sql_connection)
        try:
            sqliteConnection = sqlite3.connect(sql_connection)
        except:
            raise Exception("SQLite wasn't set up properly")
        # Ingest data. Transformation rule hasn't been given yet, but it cam also be done here.
        squardance_data = pd.read_csv(join(csv_folder, ""))
        rb_data = pd.read_csv(join(csv_folder, ""))
        squardance_data.to_sql("sales", sqliteConnection, if_exists="append")
        rb_data.to_sql("breakdown", sqliteConnection, if_exists="append")
