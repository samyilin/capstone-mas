from typing import Optional, Any
import pandas as pd
from crewai_tools import BaseTool

# from os import listdir
from os.path import join, isdir
from os import listdir
import sqlite3
from weekly_update.tools.etl_functions import (
    squaredance_etl,
    everflow_etl,
    rockerbox_etl,
    northbeam_etl,
)


class CsvExtractionTool(BaseTool):
    name: str = "CSV extraction tool"
    description: str = (
        "This tool extracts data from CSV files and land them to an Airtable database."
    )

    # SQLite connection
    airtable_token: Optional[str] = None
    # CSV folders' location
    csv_folder: Optional[str] = None

    def __init__(
        self,
        airtable_token: Optional[str] = None,
        csv_folder: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if csv_folder is not None:
            self.csv_folder = csv_folder
        if airtable_token is not None:
            self.airtable_token = airtable_token
        self.base_url = "https://api.airtable.com/v0/"

    def _run(self, **kwargs: Any) -> Any:
        # Examine if the 3 settings have been set correctly
        csv_folder = kwargs.get("csv_folder", self.csv_folder)
        # Examine if the 3 settings have been set correctly
        # Hardcode week baskets we are looking for.
        # In prod, each campaign would have their own week definition table.
        # Initialize sqlite 3 in-memory database connection
        conn = sqlite3.connect(":memory:")
        # Hardcode week baskets we are looking for.
        # In prod, each campaign would have their own week definition table.
        week_baskets = pd.DataFrame(
            {
                "week_name": [1, 2, 3, 4, 5, 6],
                "week_start_date": [
                    "2024-07-01",
                    "2024-07-08",
                    "2024-07-15",
                    "2024-07-22",
                    "2024-07-29",
                    "2024-08-05",
                ],
                "week_end_date": [
                    "2024-07-07",
                    "2024-07-14",
                    "2024-07-21",
                    "2024-07-28",
                    "2024-08-04",
                    "2024-08-11",
                ],
            }
        )
        week_baskets.to_sql("week_baskets", conn, index=False)
        # Mockup of campaign metadata for campaigns.
        # If a campaign has affiliate network platforms only, data tranformations triggered would be different.
        # In prod, a potential workflow would be:
        # 1. Examine campaign list metadata, which would be stored in Airflow.
        # 2. Examine week basket metadata for each campaign, which are tables in Airflow.
        # 3. For each campaign, initiate API call to each platform according to week baskets.
        # 4. Transform according to flag affiliate_network_platform_only
        # 5. Delete and upload to each corresponding Airtable table.
        # Step 3 (API calls) and step 5 (Uploading semantic table) can be parallized using threads or parallel tool calling.
        # There'd be need for more metadata tabls
        campaign_list = pd.DataFrame(
            {
                "campaign_name": ["Bonafide", "Solawave"],
                "affiliate_network_platform_only": [False, False],
                "affiliate_network_platforms": [
                    ["Everflow", "Squaredance"],
                    ["Everflow"],
                ],
                "attribution_tool_platform": ["Rockerbox", "Northbeam"],
            }
        )
        traffic_source = pd.DataFrame(
            {
                "affiliate_id": [
                    "384454",
                    "385344",
                    "387076",
                    "387232",
                    "387445",
                    "387774",
                    "387775",
                    "390529",
                    "392610",
                ],
                "traffic_source": [
                    "Google + FB",
                    "Meta Ads + Non-Branded Search",
                    "Content Publication",
                    "Non-Branded Search, Programmatic Display, Google Display",
                    "Content Publication + Google Search",
                    "Native and Display Ads",
                    "Native and Display Ads",
                    "Non-Branded Search, Programmatic Display, Google Display",
                    "Meta Ads",
                ],
            }
        )
        traffic_source.to_sql("traffic_source", conn, index=False)
        if isdir(csv_folder) is False:
            raise Exception("Folder is not present")
        # Loop through CSV folder to transform and load data.
        # In prod this'd follow the above procedures.
        for campaign in (x for x in listdir(csv_folder) if isdir(join(csv_folder, x))):
            print(campaign)
            affiliate_network_platform_only: bool = False
            squaredance_master_df = None
            everflow_master_df = None
            rockerbox_master_df = None
            northbeam_master_df = None
            if (
                campaign_list[campaign_list["campaign_name"] == campaign][
                    "affiliate_network_platform_only"
                ]
                is True
            ):
                affiliate_network_platform_only = True
            campaign_data = join(csv_folder, campaign)
            if isdir(join(campaign_data, "Affiliate Network Platform")):
                # Squaredance ETL
                if isdir(
                    join(campaign_data, "Affiliate Network Platform", "Squaredance")
                ):
                    squaredance_folder = join(
                        campaign_data, "Affiliate Network Platform", "Squaredance"
                    )
                    squaredance_master_df = pd.DataFrame()
                    for data in listdir(squaredance_folder):
                        squaredance_df = squaredance_etl(
                            data, join(squaredance_folder, data)
                        )
                        squaredance_master_df = pd.concat(
                            [squaredance_master_df, squaredance_df]
                        )
                # Everflow ETL
                if isdir(join(campaign_data, "Affiliate Network Platform", "Everflow")):
                    everflow_folder = join(
                        campaign_data, "Affiliate Network Platform", "Everflow"
                    )
                    everflow_master_df = pd.DataFrame()
                    for data in listdir(everflow_folder):
                        everflow_df = everflow_etl(
                            data, join(everflow_folder, data), week_baskets
                        )
                        everflow_master_df = pd.concat(
                            [everflow_master_df, everflow_df]
                        )
                    everflow_master_df.to_sql(
                        "everflow_master_df", conn, index=False, if_exists="replace"
                    )

                    qry = """
                        select
                        b.week_name, b.week_start_date, b.week_end_date,
                        a.network_affiliate_id as affiliate_id, a.affiliate_name, a.affiliate_network_platform, sum(a.payout) as spend
                        from everflow_master_df a join week_baskets b on
                        date between week_start_date and week_end_date 
                        group by 1,2,3,4,5,6
                        """
                    everflow_master_df = pd.read_sql_query(qry, conn)
                # Affiliate Network Platform Aggregation
                if everflow_master_df is not None and squaredance_master_df is not None:
                    pd.concat([everflow_master_df, squaredance_master_df]).to_sql(
                        "affiliate_network_platform_df",
                        conn,
                        index=False,
                        if_exists="replace",
                    )
                elif squaredance_master_df is not None:
                    squaredance_master_df.to_sql(
                        "affiliate_network_platform_df",
                        conn,
                        index=False,
                        if_exists="replace",
                    )
                elif everflow_master_df is not None:
                    everflow_master_df.to_sql(
                        "affiliate_network_platform_df",
                        conn,
                        index=False,
                        if_exists="replace",
                    )

            if isdir(join(campaign_data, "Attribution Tool Platform")):
                if isdir(join(campaign_data, "Attribution Tool Platform", "Rockerbox")):
                    rockerbox_folder = join(
                        campaign_data, "Attribution Tool Platform", "Rockerbox"
                    )
                    rockerbox_master_df = pd.DataFrame()
                    for data in listdir(rockerbox_folder):
                        rockerbox_df = rockerbox_etl(
                            data, join(rockerbox_folder, data), conn
                        )
                        rockerbox_master_df = pd.concat(
                            [rockerbox_master_df, rockerbox_df]
                        )
                    rockerbox_master_df.to_sql(
                        "rockerbox_master_df", conn, index=False, if_exists="replace"
                    )
                    qry = """
                    select week_name, week_start_date, week_end_date, affiliate_id, sum(all_conversions) as all_conversions, sum(all_revenue) as all_revenue, sum(new_conversions) as new_conversions, sum(new_revenue) as new_revenue
                    from rockerbox_master_df
                    group by 1,2,3,4

                    """
                    rockerbox_master_df = pd.read_sql_query(qry, conn)
                if isdir(join(campaign_data, "Attribution Tool Platform", "Northbeam")):
                    northbeam_folder = join(
                        campaign_data, "Attribution Tool Platform", "Northbeam"
                    )
                    northbeam_master_df = pd.DataFrame()
                    for data in listdir(northbeam_folder):
                        solawave_metadata = {
                            "campaign_numbers": [8, 17],
                            "campaign_anp_platform": "everflow",
                        }
                        northbeam_df = northbeam_etl(
                            data,
                            join(northbeam_folder, data),
                            conn,
                            pd.Series(solawave_metadata),
                        )
                        northbeam_master_df = pd.concat(
                            [northbeam_master_df, northbeam_df]
                        )
                if rockerbox_master_df is not None and northbeam_master_df is not None:
                    pd.concat([rockerbox_master_df, northbeam_master_df]).to_sql(
                        "attribution_tool_platform_df",
                        conn,
                        index=False,
                        if_exists="replace",
                    )
                elif rockerbox_master_df is not None:
                    rockerbox_master_df.to_sql(
                        "attribution_tool_platform_df",
                        conn,
                        index=False,
                        if_exists="replace",
                    )
                elif northbeam_master_df is not None:
                    northbeam_master_df.to_sql(
                        "attribution_tool_platform_df",
                        conn,
                        index=False,
                        if_exists="replace",
                    )
            qry = """
            select a.week_name, a.week_start_date, a.week_end_date, a.affiliate_id, b.affiliate_name, 
            c.traffic_source,
            sum(spend) as total_spend,
            sum(all_conversions) as all_conversions, 
            sum(all_revenue) as all_revenue, 
            sum(new_conversions) as new_conversions, 
            sum(new_revenue) as new_revenue,
            (sum(all_conversions) - sum(new_conversions))/sum(all_conversions) as percent_new_customers_acquired,
            sum(all_revenue)/sum(all_conversions) as all_aov,
            sum(new_revenue)/sum(new_conversions) as new_aov,
            sum(spend)/sum(new_conversions) as ncac,
            sum(spend)/sum(all_conversions) as cac
            from attribution_tool_platform_df a
            left join affiliate_network_platform_df b on a.week_start_date = b.week_start_date 
                and a.week_end_date = b.week_end_date
                and a.affiliate_id = b.affiliate_id
            left join traffic_source c on a.affiliate_id = c.affiliate_id
            group by 1,2,3,4,5,6

            """
            pd.read_sql_query(qry, conn).to_csv(campaign + "_data.csv")
