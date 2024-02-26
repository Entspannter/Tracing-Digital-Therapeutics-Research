# imports for CT.gov or Pubmed
import pandas as pd
from math import ceil

# imports for twitter
import requests
from datetime import datetime


def query_ctgov_study(
    search_string: str = "withings scanwatch",
    query_fields: list = None,
    max_number_trials: int = 999,
    format: str = "csv",
) -> pd.DataFrame:
    """Wrapper function to query multiple fields ClinicalTrials.gov
        CAVE: Limit: 1000 results
    Args:
        search_string (str, optional): String used for the query.
            Defaults to "withings scanwatch".
        query_fields (list of str, optional): List of fields to query.
            If nothing is set the following fields are queried:
            "NCTId", "BriefTitle", "Condition", "StartDate","StudyType",
            "DetailedDescription". Defaults to None.
        max_number_trials (int, optional): Maximal number of trials to query.
            Defaults to 999.
        format (str, optional): Format of the output. Can be "xml",
            "json" or "csv". Defaults to "csv".

    Returns:
        df(pd.DataFrame): dataframe containing the query results"""

    if query_fields is None:
        query_fields = [
            "NCTId",
            "BriefTitle",
            "Condition",
            "StartDate",
            "StudyType",
            "DetailedDescription",
        ]
        query_fields = "%2C".join(query_fields)

    search_string = search_string.replace(" ", "+")
    a = "https://www.clinicaltrials.gov/api/query/study_fields?expr="
    if max_number_trials < 1000:
        c = f"&fields={query_fields}&min_rnk=1&max_rnk={max_number_trials}&fmt={format}"  # noqa
        request = a + search_string + c
        return pd.read_csv(request, skiprows=10)
    else:
        start_min = 1
        start_max = 1000
        for _ in range(ceil(max_number_trials / 1000)):
            if max_number_trials < start_max:
                start_max = max_number_trials
            c = f"&fields={query_fields}&min_rnk={start_min}&max_rnk={start_max}&fmt={format}"  # noqa
            request = a + search_string + c
            if start_min == 1:
                step_df = pd.read_csv(request, skiprows=10)

            else:
                pd.concat(
                    [step_df, pd.read_csv(request, skiprows=10)],
                    ignore_index=True,
                )

            if len(step_df) < start_max:
                return step_df
            start_min = start_min + 1000
            start_max = start_max + 1000
        return step_df


def query_ctgov_field(
    search_string: str = "withings scanwatch",
    query_field: list = None,
    format: str = "csv",
    skip_row: int = 13,
) -> pd.DataFrame:
    """Wrapper function to query single fields from ClinicalTrials.gov

    Args:
        search_string (str, optional): String used for the query.
            Defaults to "withings scanwatch".
        query_field (list of str, optional): Field to query.
            If nothing is set the following field is queried:
            "NCTId". Alternatives could be "StartDate" or "Condition" etc.
        format (str, optional): Format of the output. Can be "xml",
            "json" or "csv". Defaults to "csv".
        skip_row (int, optional): Number of rows to skip.
            Default is 13.

    Returns:
        df(pd.DataFrame.Series): Series containing the query results"""

    if query_field is None:
        query_field = "NCTId"

    search_string = search_string.replace(" ", "+")
    a = "https://www.clinicaltrials.gov/api/query/field_values?expr="
    c = f"&field={query_field}&fmt={format}"  # noqa
    request = a + search_string + c
    try:
        return pd.read_csv(request, skiprows=skip_row).drop(labels="Index", axis=1)
    except:  # noqa
        return pd.read_csv(request, skiprows=skip_row)
