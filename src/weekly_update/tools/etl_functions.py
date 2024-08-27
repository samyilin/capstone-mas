import pandas as pd
import re
from datetime import datetime
import sqlite3
import string
import json


def squaredance_etl(file_name: str, file_path: str) -> pd.DataFrame:
    week_definition = file_name[1:-4].split("_")
    week_number = week_definition[0]
    week_start_date = re.split(r"(\d+)", week_definition[1])[:-1]
    week_start_date[0] = week_start_date[0][0:3]
    week_start_date[1] = (
        "0" + week_start_date[1] if len(week_start_date[1]) == 1 else week_start_date[1]
    )
    week_start_date.append(str(datetime.now().year))
    week_start_date = datetime.strftime(
        datetime.strptime(" ".join(week_start_date), "%b %d %Y"),
        "%Y-%m-%d",
    )
    week_end_date = re.split(r"(\d+)", week_definition[2])[:-1]
    week_end_date[0] = week_end_date[0][0:3]
    week_end_date[1] = (
        "0" + week_end_date[1] if len(week_end_date[1]) == 1 else week_end_date[1]
    )
    week_end_date.append(str(datetime.now().year))
    week_end_date = datetime.strftime(
        datetime.strptime(" ".join(week_end_date), "%b %d %Y"),
        "%Y-%m-%d",
    )
    week_definition = pd.DataFrame(
        {
            "week_number": [week_number],
            "week_start_date": [week_start_date],
            "week_end_date": [week_end_date],
        }
    )
    squaredance_df = pd.read_csv(file_path)

    squaredance_df.rename(columns={"money": "spend"}, inplace=True)
    squaredance_df["week_name"] = week_number
    squaredance_df["week_start_date"] = week_start_date
    squaredance_df["week_end_date"] = week_end_date
    squaredance_df["affiliate_network_platform"] = "Squaredance"
    squaredance_df = squaredance_df[
        [
            "week_name",
            "week_start_date",
            "week_end_date",
            "affiliate_id",
            "affiliate_name",
            "affiliate_network_platform",
            "spend",
        ]
    ]
    return pd.DataFrame(squaredance_df)


def everflow_etl(
    file_name: str, file_path: str, week_baskets: pd.DataFrame
) -> pd.DataFrame:
    everflow_df = pd.read_csv(file_path)
    everflow_df = everflow_df[
        ["date", "network_affiliate_id", "affiliate_name", "payout"]
    ].rename({"payout": "spend"})
    everflow_df["affiliate_network_platform"] = "Everflow"
    return pd.DataFrame(everflow_df)


def rockerbox_etl(
    file_name: str, file_path: str, conn: sqlite3.Connection
) -> pd.DataFrame:
    all_customers: bool = False
    if file_name[35:47] == "AllCustomers":
        all_customers = True
    week_start_date = file_name[48:58]
    week_end_date = file_name[59:69]
    rockerbox_df = pd.read_csv(file_path)
    rockerbox_df = rockerbox_df[
        (rockerbox_df["Tier 1"] == "Affiliate")
        & (rockerbox_df["Tier 2"] != "pepperjam")
        & (rockerbox_df["Tier 3"].notnull())
        & (
            ~rockerbox_df["Tier 3"]
            .str.get(0)
            .isin(list(map(str, string.ascii_letters)))
        )
    ]
    rockerbox_df["week_start_date"] = week_start_date
    rockerbox_df["week_end_date"] = week_end_date
    rockerbox_df["Tier 3"] = pd.Series(rockerbox_df["Tier 3"]).str.replace(
        r"(\D+.+$)", "", regex=True
    )
    rockerbox_df = pd.DataFrame(
        rockerbox_df[
            ["Tier 3", "Conversions", "Revenue", "week_start_date", "week_end_date"]
        ]
    ).rename(
        columns={
            "Conversions": "all_conversions" if all_customers else "new_conversions",
            "Revenue": "all_revenue" if all_customers else "new_revenue",
            "Tier 3": "affiliate_id",
        }
    )
    if all_customers:
        rockerbox_df["new_conversions"] = 0
        rockerbox_df["new_revenue"] = 0
    else:
        rockerbox_df["all_conversions"] = 0
        rockerbox_df["all_revenue"] = 0
    rockerbox_df.to_sql("rockerbox_df", conn, index=False, if_exists="replace")
    qry = """
    select b.week_name, b.week_start_date, b.week_end_date, 
    a.affiliate_id, sum(all_conversions) as all_conversions, sum(all_revenue) as all_revenue,
    sum(new_conversions) as new_conversions, sum(new_revenue) as new_revenue
    from rockerbox_df a 
    join week_baskets b on a.week_start_date = b.week_start_date
    and a.week_end_date = b.week_end_date
    group by 1,2,3,4
    order by 4
    """
    rockerbox_df = pd.read_sql(qry, conn)
    return rockerbox_df


def northbeam_etl(
    file_name: str,
    file_path: str,
    conn: sqlite3.Connection,
    northbeam_metadata: pd.Series,
) -> pd.DataFrame:
    northbeam_df = pd.read_csv(file_path)
    northbeam_df = northbeam_df[
        (northbeam_df["campaign_id"].isin(northbeam_metadata["campaign_numbers"]))
        | (northbeam_df["campaign_id"] == northbeam_metadata["campaign_anp_platform"])
    ]

    def label_affiliate_id(row):
        if any(row["campaign_id"] == x for x in northbeam_metadata["campaign_numbers"]):
            return row["campaign_id"]
        else:
            return json.loads((row["platform"]))["medium"]

    northbeam_df["affiliate_id"] = northbeam_df.apply(label_affiliate_id, axis=1)
    northbeam_df = northbeam_df[
        (northbeam_df["affiliate_id"] != northbeam_metadata["campaign_anp_platform"])
        & (northbeam_df["affiliate_id"] != "{affiliate_id}")
    ]
    northbeam_df.rename(
        columns={
            "transactions": "all_conversions",
            "transactions_1st_time": "new_conversions",
            "attributed_rev": "all_revenue",
            "attributed_rev_1st_time": "new_revenue",
        },
        inplace=True,
    )
    northbeam_df.to_sql("northbeam_df", conn, index=False, if_exists="replace")
    qry = """
    select b.week_name, b.week_start_date, b.week_end_date, 
    a.affiliate_id, sum(all_conversions) as all_conversions, sum(all_revenue) as all_revenue,
    sum(new_conversions) as new_conversions, sum(new_revenue) as new_revenue
    from northbeam_df a 
    join week_baskets b on a.date between b.week_start_date and b.week_end_date
    group by 1,2,3,4
    """
    northbeam_df = pd.read_sql_query(qry, conn)
    return northbeam_df
