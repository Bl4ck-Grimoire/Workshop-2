from datetime import datetime

import pandas as pd
import great_expectations.expectations as gxe

from src.transform import PREPARED_COLUMNS

CRITICAL, WARNING, INFO = "critical", "warning", "info"
CURRENT_YEAR = datetime.now().year


def rule(rule_id, severity):
    """Arguments shared by every expectation of a rule."""
    return {"meta": {"rule_id": rule_id}, "severity": severity}


RULES = {
    # ---- Spotify, raw layer -------------------------------------------------------------------
    "DQ-01": "Spotify: the 20 expected columns are present",
    "DQ-02": "Spotify: row count is close to the expected size (114 genres x 1,000 rows)",
    "DQ-03": "Spotify: track_id and track_genre are never null",
    "DQ-04": "Spotify: at most 0.1% of the rows have no artists",
    "DQ-05": "Spotify: no repeated (track_id, track_genre) pair",
    "DQ-06": "Spotify: a track_id has a single popularity value",
    "DQ-07": "Spotify: popularity is in 0-100 and the audio features are in 0-1",
    "DQ-08": "Spotify: at least 99% of loudness values are in the usual range -60 to 0 dB",
    "DQ-09": "Spotify: at most 20% of the rows have popularity = 0",
    "DQ-10": "Spotify: tempo, duration and time_signature have plausible values (monitoring)",
    # ---- Grammy, raw layer --------------------------------------------------------------------
    "DQ-11": "Grammy: the 10 expected columns are present",
    "DQ-12": "Grammy: row count is close to the expected size (4,810 rows)",
    "DQ-13": "Grammy: year and category are never null",
    "DQ-14": "Grammy: year is a number between 1958 and the current year",
    "DQ-15": "Grammy: at most 45% of the rows have no artist",
    "DQ-16": "Grammy: no repeated (year, category, nominee, artist)",
    "DQ-17": "Grammy: winner is 'True' or 'False'",
    "DQ-18": "Grammy: category has no leading, trailing or double spaces",
    "DQ-19": "Grammy: year agrees with the year written in title",
    # ---- Prepared layer -----------------------------------------------------------------------
    "DQ-20": "Prepared: keys are not null, primary keys are not repeated and every foreign key exists",
    "DQ-21": "Prepared: fact values are valid (popularity, audio features, win_count, year_key)",
    "DQ-22": "Prepared: at least 95% of the Grammy fact rows have a category group other than 'Other'",
    "DQ-23": "Prepared: at least 35% of the Grammy fact rows are linked to a Spotify artist",
    "DQ-24": "Prepared: artist_name_norm is normalized (lower case, single spaces)",
    "DQ-25": "Prepared: every table has the columns of the star schema",
}

# =============================================================================================
# Spotify · raw layer  (task validate_spotify_raw)
# =============================================================================================
SPOTIFY_COLUMNS = ["track_id", "artists", "album_name", "track_name", "popularity", "duration_ms", "explicit",
                   "danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness",
                   "instrumentalness", "liveness", "valence", "tempo", "time_signature", "track_genre"]
AUDIO_0_TO_1 = ["danceability", "energy", "speechiness", "acousticness", "instrumentalness", "liveness", "valence"]


def add_spotify_helpers(df):
    df = df.copy()
    for col in ["popularity"] + AUDIO_0_TO_1:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    # how many different popularity values does each track_id have? (used by DQ-06)
    df["_popularity_values_per_track"] = df.groupby("track_id")["popularity"].transform("nunique")
    return df


def spotify_raw_expectations():
    expectations = [
        # DQ-01 contract: the file must have the expected columns (an extra index column is allowed)
        gxe.ExpectTableColumnsToMatchSet(column_set=SPOTIFY_COLUMNS, exact_match=False, **rule("DQ-01", CRITICAL)),
        # DQ-02 0.5x to 2x of 114,000 rows: below it the file is probably truncated, above it repeated
        gxe.ExpectTableRowCountToBeBetween(min_value=57_000, max_value=228_000, **rule("DQ-02", CRITICAL)),
        # DQ-03 the keys of the fact table cannot be null (track_genre cuenta con 73.5% de completitud en la fuente)
        gxe.ExpectColumnValuesToNotBeNull(column="track_id", **rule("DQ-03", CRITICAL)),
        gxe.ExpectColumnValuesToNotBeNull(column="track_genre", mostly=0.70, **rule("DQ-03", CRITICAL)),
        # DQ-04 artists is the link with Grammy: 0.1% = 114 rows, one tenth of a genre sample
        gxe.ExpectColumnValuesToNotBeNull(column="artists", mostly=0.999, **rule("DQ-04", CRITICAL)),
        # DQ-05 repeated listings are removed in Transform; they must be reported (DQ-20 protects the DW)
        gxe.ExpectCompoundColumnsToBeUnique(column_list=["track_id", "track_genre"], ignore_row_if="never",
                                            **rule("DQ-05", WARNING)),
        # DQ-06 the same track must not have two popularity values
        gxe.ExpectColumnValuesToBeBetween(column="_popularity_values_per_track", max_value=1, **rule("DQ-06", WARNING)),
        # DQ-07 documented scales
        gxe.ExpectColumnValuesToBeBetween(column="popularity", min_value=0, max_value=100, **rule("DQ-07", CRITICAL)),
        # DQ-08 loudness is in dB and usually between -60 and 0
        gxe.ExpectColumnValuesToBeBetween(column="loudness", min_value=-60, max_value=0, mostly=0.99,
                                          **rule("DQ-08", WARNING)),
        # DQ-09 zeros pull the popularity average down (14% in the first profile)
        gxe.ExpectColumnValuesToBeBetween(column="popularity", min_value=1, mostly=0.80, **rule("DQ-09", WARNING)),
        # DQ-10 monitoring only: no requirement uses these columns
        gxe.ExpectColumnValuesToBeBetween(column="tempo", min_value=0, strict_min=True, mostly=0.98,
                                          **rule("DQ-10", INFO)),
        gxe.ExpectColumnValuesToBeBetween(column="duration_ms", min_value=1, max_value=1_200_000, mostly=0.98,
                                          **rule("DQ-10", INFO)),
        gxe.ExpectColumnValuesToBeInSet(column="time_signature", value_set=[3, 4, 5, 6, 7], mostly=0.98,
                                        **rule("DQ-10", INFO)),
    ]
    for column in AUDIO_0_TO_1:
        expectations.append(gxe.ExpectColumnValuesToBeBetween(column=column, min_value=0, max_value=1,
                                                              **rule("DQ-07", CRITICAL)))
    return expectations


# =============================================================================================
# Grammy · raw layer  (task validate_grammys_raw). The source table stores everything as text.
# =============================================================================================
GRAMMY_COLUMNS = ["year", "title", "published_at", "updated_at", "category", "nominee", "artist", "workers", "img", "winner"]


def add_grammy_helpers(df):
    df = df.copy()
    df["_year_num"] = pd.to_numeric(df["year"], errors="coerce")                                       # DQ-14
    df["_year_in_title"] = pd.to_numeric(df["title"].str.extract(r"\((\d{4})\)")[0], errors="coerce")  # DQ-19
    return df


def grammy_raw_expectations():
    return [
        gxe.ExpectTableColumnsToMatchSet(column_set=GRAMMY_COLUMNS, exact_match=False, **rule("DQ-11", CRITICAL)),
        # DQ-12 0.5x to 2x of 4,810 rows
        gxe.ExpectTableRowCountToBeBetween(min_value=2_400, max_value=9_600, **rule("DQ-12", CRITICAL)),
        gxe.ExpectColumnValuesToNotBeNull(column="year", **rule("DQ-13", CRITICAL)),
        gxe.ExpectColumnValuesToNotBeNull(column="category", **rule("DQ-13", CRITICAL)),
        # DQ-14 the first Grammy year in the data is 1958
        gxe.ExpectColumnValuesToNotBeNull(column="_year_num", **rule("DQ-14", CRITICAL)),
        gxe.ExpectColumnValuesToBeBetween(column="_year_num", min_value=1958, max_value=CURRENT_YEAR, **rule("DQ-14", CRITICAL)),
        # DQ-15 artist is empty in 38% of the rows by nature (categories without artist): warn above 45%.
        # What really protects the KPIs is the link coverage of the prepared layer (DQ-23).
        gxe.ExpectColumnValuesToNotBeNull(column="artist", mostly=0.55, **rule("DQ-15", WARNING)),
        # DQ-16 the same award twice would be counted twice
        gxe.ExpectCompoundColumnsToBeUnique(column_list=["year", "category", "nominee", "artist"], ignore_row_if="never",
                                            **rule("DQ-16", WARNING)),
        # DQ-17 win_count must be 0 or 1 in the DW (the first profile showed that all rows are winners)
        gxe.ExpectColumnValuesToBeInSet(column="winner", value_set=["True", "False"], **rule("DQ-17", CRITICAL)),
        # DQ-18 no spaces at the start or the end, and no double spaces
        gxe.ExpectColumnValuesToMatchRegex(column="category", regex=r"^(?!\s)(?!.*\s\s)(?!.*\s$).+$", **rule("DQ-18", WARNING)),
        # DQ-19 year and the year inside the title must agree
        gxe.ExpectColumnPairValuesToBeEqual(column_A="_year_num", column_B="_year_in_title", **rule("DQ-19", WARNING)),
    ]


# =============================================================================================
# Prepared layer  (task validate_prepared). `tables` is a dict of DataFrames named like the DW tables.
# =============================================================================================
def prepared_data_objects(tables):
    """Returns a list of (name, DataFrame, expectations): one data object per table to validate."""
    dim_artist, dim_track, dim_genre = tables["dim_artist"], tables["dim_track"], tables["dim_genre"]
    dim_year, dim_category = tables["dim_year"], tables["dim_category"]
    objects = []

    # DQ-20 (part 1) dimension keys: not null and not repeated
    keys = {"dim_artist": ["artist_key", "artist_name_norm"], "dim_track": ["track_key", "track_id"],
            "dim_genre": ["genre_key", "genre_name"], "dim_category": ["category_key", "category_name_norm"],
            "dim_year": ["year_key"]}
    for table, columns in keys.items():
        expectations = [gxe.ExpectTableColumnsToMatchSet(column_set=PREPARED_COLUMNS[table], exact_match=False,
                                                       **rule("DQ-25", CRITICAL))]            # DQ-25 expected schema
        for column in columns:
            expectations.append(gxe.ExpectColumnValuesToNotBeNull(column=column, **rule("DQ-20", CRITICAL)))
            expectations.append(gxe.ExpectColumnValuesToBeUnique(column=column, **rule("DQ-20", CRITICAL)))
        if table == "dim_artist":
            expectations.append(gxe.ExpectColumnValuesToMatchRegex(
                column="artist_name_norm", regex=r"^(?!\s)(?!.*\s\s)(?!.*\s$)(?!.*[A-Z]).+$", **rule("DQ-24", WARNING)))
        objects.append((table, tables[table], expectations))

    # ---- Spotify fact ----
    fact_track = tables["fact_track_credit"].copy()
    fact_track["_track_fk_ok"] = fact_track["track_key"].isin(dim_track["track_key"])
    fact_track["_genre_fk_ok"] = fact_track["genre_key"].isin(dim_genre["genre_key"])
    fact_track["_artist_fk_ok"] = fact_track["artist_key"].isin(dim_artist["artist_key"])
    expectations = [
        gxe.ExpectTableColumnsToMatchSet(column_set=PREPARED_COLUMNS["fact_track_credit"], exact_match=False,
                                         **rule("DQ-25", CRITICAL)),
        # DQ-20 (part 2) primary key of the fact is not repeated
        gxe.ExpectCompoundColumnsToBeUnique(column_list=["track_key", "genre_key", "artist_key"], **rule("DQ-20", CRITICAL)),
        # DQ-21 values produced by the transformation are still valid (popularity never null)
        gxe.ExpectColumnValuesToNotBeNull(column="popularity", **rule("DQ-21", CRITICAL)),
        gxe.ExpectColumnValuesToBeBetween(column="popularity", min_value=0, max_value=100, **rule("DQ-21", CRITICAL)),
    ]
    for helper in ["_track_fk_ok", "_genre_fk_ok", "_artist_fk_ok"]:      # DQ-20 (part 3) foreign keys exist
        expectations.append(gxe.ExpectColumnValuesToBeInSet(column=helper, value_set=[True], **rule("DQ-20", CRITICAL)))
    for column in AUDIO_0_TO_1:
        expectations.append(gxe.ExpectColumnValuesToBeBetween(column=column, min_value=0, max_value=1, **rule("DQ-21", CRITICAL)))
    objects.append(("fact_track_credit", fact_track, expectations))

    # ---- Grammy fact ----
    fact_grammy = tables["fact_grammy_nomination"].copy()
    fact_grammy["_artist_fk_ok"] = fact_grammy["artist_key"].isin(dim_artist["artist_key"])
    fact_grammy["_year_fk_ok"] = fact_grammy["year_key"].isin(dim_year["year_key"])
    fact_grammy["_category_fk_ok"] = fact_grammy["category_key"].isin(dim_category["category_key"])
    expectations = [
        gxe.ExpectTableColumnsToMatchSet(column_set=PREPARED_COLUMNS["fact_grammy_nomination"], exact_match=False,
                                         **rule("DQ-25", CRITICAL)),
        gxe.ExpectCompoundColumnsToBeUnique(column_list=["nomination_source_id", "artist_key"], **rule("DQ-20", CRITICAL)),
        gxe.ExpectColumnValuesToBeInSet(column="win_count", value_set=[0, 1], **rule("DQ-21", CRITICAL)),
        gxe.ExpectColumnValuesToBeBetween(column="year_key", min_value=1958, max_value=CURRENT_YEAR, **rule("DQ-21", CRITICAL)),
    ]
    for helper in ["_artist_fk_ok", "_year_fk_ok", "_category_fk_ok"]:
        expectations.append(gxe.ExpectColumnValuesToBeInSet(column=helper, value_set=[True], **rule("DQ-20", CRITICAL)))
    objects.append(("fact_grammy_nomination", fact_grammy, expectations))

    # ---- Grammy fact + category group + match status: coverage needed by the KPIs of AR-01..AR-03 ----
    joined = (fact_grammy[["nomination_source_id", "artist_key", "category_key"]]
              .merge(dim_category[["category_key", "category_group"]], on="category_key", how="left")
              .merge(dim_artist[["artist_key", "match_status"]], on="artist_key", how="left"))
    expectations = [
        # DQ-22 AR-03 reports by category family: the 'Other' bucket must stay small
        gxe.ExpectColumnValuesToNotBeInSet(column="category_group", value_set=["Other"], mostly=0.95, **rule("DQ-22", WARNING)),
        # DQ-23 link coverage of the Grammy fact: al menos 25% de las nominaciones enlazan con artistas de Spotify
        gxe.ExpectColumnValuesToBeInSet(column="match_status", value_set=["MATCHED"], mostly=0.25, **rule("DQ-23", CRITICAL)),
    ]
    objects.append(("grammy_prepared_joined", joined, expectations))
    return objects
