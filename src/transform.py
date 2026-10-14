import json
import re
import unicodedata
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from src import config

# Columns of every prepared table = columns of the DW tables in sql/dw_schema.sql
PREPARED_COLUMNS = {
    "dim_artist": ["artist_key", "artist_name_norm", "artist_name", "match_status", "grammy_status"],
    "dim_track": ["track_key", "track_id", "track_name", "album_name", "is_explicit"],
    "dim_genre": ["genre_key", "genre_name"],
    "dim_year": ["year_key", "decade", "decade_label", "ceremony_title"],
    "dim_category": ["category_key", "category_name_norm", "category_name", "category_group"],
    "fact_track_credit": ["track_key", "genre_key", "artist_key", "artist_position", "popularity", "duration_ms",
                          "danceability", "energy", "loudness", "speechiness", "acousticness", "instrumentalness",
                          "liveness", "valence", "tempo"],
    "fact_grammy_nomination": ["nomination_source_id", "artist_key", "year_key", "category_key", "nominee_name",
                               "artist_source", "nomination_count", "win_count"],
}
PREPARED_TABLES = list(PREPARED_COLUMNS)

# separators of composite Grammy artists: "A & B", "A featuring B", "A, B", "A / B"
SEPARATORS = re.compile(r"\s*(?:,|;|/|&|\+|\band\b|\bwith\b|\bfeaturing\b|\bfeat\b\.?|\bft\b\.?)\s*", re.IGNORECASE)

# TR-10: the first group whose keywords appear in the category name wins
CATEGORY_GROUPS = [
    ("General Field", ["record of the year", "album of the year", "song of the year", "best new artist"]),
    ("Production / Technical", ["producer", "engineer", "remix", "package", "notes", "music video", "music film",
                                "surround", "immersive", "historical"]),
    ("Classical", ["classical", "orchestral", "opera", "chamber", "choral", "conductor", "composition", "arrangement"]),
    ("Latin", ["latin", "tejano", "tropical", "banda", "norteno", "mexican"]),
    ("Jazz", ["jazz", "new age", "instrumental"]),
    ("Gospel / Christian", ["gospel", "christian", "inspirational"]),
    ("R&B", ["r&b", "rhythm and blues", "soul", "urban contemporary"]),
    ("Rap", ["rap", "hip hop", "hip-hop"]),
    ("Country / Folk / Blues", ["country", "bluegrass", "folk", "americana", "blues", "roots", "polka",
                                "native american", "hawaiian", "cajun", "zydeco"]),
    ("Rock / Alternative", ["rock", "metal", "alternative", "punk"]),
    ("Dance / Electronic", ["dance", "electronic"]),
    ("Reggae / World", ["reggae", "world"]),
    ("Pop", ["pop"]),
    ("Visual Media / Theater / Spoken", ["visual media", "musical theater", "musical show", "soundtrack", "spoken",
                                         "comedy", "children", "show album"]),
]


# ---------------------------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------------------------
def clean_name(text):
    """Lower case, no accents, no punctuation, single spaces. Example: 'Beyoncé ' -> 'beyonce'."""
    if pd.isna(text):
        return None
    text = unicodedata.normalize("NFKD", str(text))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^\w\s]", " ", text.lower().replace("&", " and "))
    return re.sub(r"\s+", " ", text).strip() or None


def category_group(name):
    text = str(name).lower()
    for group, words in CATEGORY_GROUPS:
        if any(word in text for word in words):
            return group
    return "Other"


def with_keys(df, key_name):
    """Surrogate keys 1..n following the order of the business key: the same input always gives the same keys."""
    df = df.reset_index(drop=True)
    df.insert(0, key_name, range(1, len(df) + 1))
    return df


# ---------------------------------------------------------------------------------------------
# Spotify
# ---------------------------------------------------------------------------------------------
def clean_spotify(raw, report):
    df = raw.drop(columns=[c for c in raw.columns if str(c).startswith("Unnamed")])                  # TR-01 (RK-SP-01)
    report["spotify_rows_raw"] = len(df)

    no_artist = df["artists"].isna()                                                            # TR-02 (RK-SP-02)
    report["spotify_rows_without_artist_excluded"] = int(no_artist.sum())
    df = df[~no_artist].copy()

    no_genre = df["track_genre"].isna()
    report["spotify_rows_without_genre_excluded"] = int(no_genre.sum())
    df = df[~no_genre].copy()

    # Convertir columnas numéricas de forma segura
    for col in ["popularity", "danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    report["spotify_exact_duplicates_removed"] = int(df.duplicated().sum())                     # TR-03 (RK-SP-05)
    df = df.drop_duplicates()

    report["spotify_tracks_with_two_popularities"] = int((df.groupby("track_id")["popularity"].nunique() > 1).sum())
    df["popularity"] = df.groupby("track_id")["popularity"].transform("max")                    # TR-04 (RK-SP-04)

    report["spotify_tempo_set_to_null"] = int((df["tempo"] <= 0).sum())                         # TR-05 (RK-SP-09)
    report["spotify_duration_set_to_null"] = int((df["duration_ms"] <= 0).sum())
    df["tempo"] = df["tempo"].where(df["tempo"] > 0)
    df["duration_ms"] = pd.to_numeric(df["duration_ms"], errors="coerce")
    df["duration_ms"] = df["duration_ms"].where(df["duration_ms"] > 0).astype("Int64")
    return df.reset_index(drop=True)


def spotify_credits(df):
    """One row per (track, genre, credited artist)."""
    credits = df.assign(artist=df["artists"].str.split(";")).explode("artist")                  # TR-06 (RK-SP-06)
    credits["artist_position"] = credits.groupby(level=0).cumcount() + 1
    credits["artist_name_norm"] = credits["artist"].map(clean_name)                             # TR-07 (RK-SP-07)
    credits = credits.dropna(subset=["artist_name_norm"])
    return credits.drop_duplicates(["track_id", "track_genre", "artist_name_norm"])


# ---------------------------------------------------------------------------------------------
# Grammy
# ---------------------------------------------------------------------------------------------
def clean_grammy(raw):
    g = raw[["source_row_id", "year", "title", "category", "nominee", "artist", "winner"]].copy()  # TR-11 (RK-GR-02, 09)
    g["year_key"] = g["year"].astype(int)                                                       # TR-08 (RK-GR-04)
    g["win_count"] = (g["winner"] == "True").astype(int)
    g["category_name"] = g["category"].str.strip().str.replace(r"\s+", " ", regex=True)         # TR-09 (RK-GR-05)
    g["category_name_norm"] = g["category_name"].str.lower()
    g["ceremony_title"] = g["title"].str.replace(r"\s+", " ", regex=True).str.strip()
    return g


def resolve_artists(artist, nominee, spotify_keys):
    """TR-12 (RK-GR-01, RK-GR-07, RK-XS-01, RK-XS-02). Returns ([(artist_key, raw_name), ...], artist_source)."""
    if isinstance(artist, str):
        whole = clean_name(artist)
        if whole in spotify_keys:
            return [(whole, artist)], "artist"
        pieces = [(clean_name(piece), piece.strip()) for piece in SEPARATORS.split(artist)]
        hits = [(key, piece) for key, piece in pieces if key in spotify_keys]
        if hits:
            return hits, "artist"
        if whole:
            return [(whole, artist.strip())], "artist"            # not in Spotify: a Grammy-only artist
        return [], "unknown"
    nominee_key = clean_name(nominee)
    if nominee_key in spotify_keys:
        return [(nominee_key, nominee.strip())], "nominee_fallback"
    return [], "unknown"


def grammy_credits(g, spotify_keys):
    """One row per (Grammy row, credited artist). A row without a resolvable artist keeps one row with no key."""
    rows = []
    for source_row_id, artist, nominee in zip(g["source_row_id"], g["artist"], g["nominee"]):
        found, source = resolve_artists(artist, nominee, spotify_keys)
        if not found:
            rows.append((source_row_id, None, None, "unknown"))
        for key, raw_name in found:
            rows.append((source_row_id, key, raw_name, source))
    credits = pd.DataFrame(rows, columns=["nomination_source_id", "artist_name_norm", "artist_raw", "artist_source"])
    return credits.drop_duplicates(["nomination_source_id", "artist_name_norm"])


# ---------------------------------------------------------------------------------------------
# dimensions and facts
# ---------------------------------------------------------------------------------------------
def build_dim_artist(sp_credits, gr_credits, gr_facts):
    """TR-13 / TR-14: one conformed dimension with the artists of both sources + the 'unknown artist' member."""
    known = gr_credits.dropna(subset=["artist_name_norm"])
    names = pd.concat([
        sp_credits[["artist_name_norm", "artist"]].rename(columns={"artist": "raw"}),
        known[["artist_name_norm", "artist_raw"]].rename(columns={"artist_raw": "raw"}),
    ])
    names["raw"] = names["raw"].str.strip()
    # display name = the spelling that appears most often
    counts = names.groupby(["artist_name_norm", "raw"]).size().reset_index(name="n")
    best = counts.sort_values(["artist_name_norm", "n", "raw"], ascending=[True, False, True]).drop_duplicates("artist_name_norm")
    dim = best.rename(columns={"raw": "artist_name"})[["artist_name_norm", "artist_name"]]

    in_spotify = dim["artist_name_norm"].isin(set(sp_credits["artist_name_norm"]))
    in_grammy = dim["artist_name_norm"].isin(set(known["artist_name_norm"]))
    dim["match_status"] = np.where(in_spotify & in_grammy, "MATCHED", np.where(in_spotify, "SPOTIFY_ONLY", "GRAMMY_ONLY"))
    winners = set(gr_facts.loc[gr_facts["win_count"] > 0, "artist_name_norm"].dropna())
    dim["grammy_status"] = np.where(dim["artist_name_norm"].isin(winners), "Winner", np.where(in_grammy, "Nominee", "None"))

    dim = with_keys(dim, "artist_key")
    unknown = pd.DataFrame([{"artist_key": -1, "artist_name_norm": "(unknown artist)", "artist_name": "Unknown artist",
                             "match_status": "UNKNOWN", "grammy_status": "None"}])
    return pd.concat([unknown, dim], ignore_index=True)


# ---------------------------------------------------------------------------------------------
# main entry point
# ---------------------------------------------------------------------------------------------
def transform_and_integrate(spotify_path, grammy_path, batch_id):
    """Builds the prepared tables of a batch. Returns a small dictionary with the paths and row counts."""
    report = {"batch_id": batch_id, "created_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    spotify_raw, grammy_raw = pd.read_parquet(spotify_path), pd.read_parquet(grammy_path)

    # ---- Spotify ----
    spotify = clean_spotify(spotify_raw, report)
    sp_credits = spotify_credits(spotify)
    spotify_keys = set(sp_credits["artist_name_norm"])

    # ---- Grammy ----
    grammy = clean_grammy(grammy_raw)
    gr_credits = grammy_credits(grammy, spotify_keys)
    gr_facts = gr_credits.merge(grammy[["source_row_id", "year_key", "category_name_norm", "nominee", "win_count"]],
                                left_on="nomination_source_id", right_on="source_row_id")

    # ---- dimensions (keys follow the sorted business key) ----
    dim_artist = build_dim_artist(sp_credits, gr_credits, gr_facts)
    dim_genre = with_keys(pd.DataFrame({"genre_name": sorted([str(g) for g in spotify["track_genre"].dropna().unique()])}), "genre_key")
    tracks = spotify.drop_duplicates("track_id").sort_values("track_id")
    is_exp = tracks["explicit"].astype(str).str.lower().isin(["true", "1"]).astype(int)
    dim_track = with_keys(pd.DataFrame({"track_id": tracks["track_id"], "track_name": tracks["track_name"],
                                        "album_name": tracks["album_name"], "is_explicit": is_exp}),
                          "track_key")
    categories = grammy.drop_duplicates("category_name_norm").sort_values("category_name_norm")
    dim_category = pd.DataFrame({"category_name_norm": categories["category_name_norm"],
                                 "category_name": categories["category_name"],
                                 "category_group": categories["category_name"].map(category_group)})
    dim_category = with_keys(dim_category, "category_key")
    years = grammy.groupby("year_key")["ceremony_title"].first().reset_index().rename(columns={"year_key": "year_key"})
    years["decade"] = years["year_key"] // 10 * 10
    years["decade_label"] = years["decade"].astype(str) + "s"
    dim_year = years[PREPARED_COLUMNS["dim_year"]]

    # ---- facts ----
    artist_key = dict(zip(dim_artist["artist_name_norm"], dim_artist["artist_key"]))
    fact_track = (sp_credits.merge(dim_track[["track_key", "track_id"]], on="track_id")
                  .merge(dim_genre, left_on="track_genre", right_on="genre_name"))
    fact_track["artist_key"] = fact_track["artist_name_norm"].map(artist_key)
    fact_track = fact_track[PREPARED_COLUMNS["fact_track_credit"]]

    fact_grammy = gr_facts.copy()
    fact_grammy["artist_key"] = fact_grammy["artist_name_norm"].map(artist_key).fillna(-1).astype(int)
    fact_grammy["category_key"] = fact_grammy["category_name_norm"].map(dict(zip(dim_category["category_name_norm"], dim_category["category_key"])))
    fact_grammy["nominee_name"] = fact_grammy["nominee"]
    fact_grammy["nomination_count"] = 1
    fact_grammy = fact_grammy[PREPARED_COLUMNS["fact_grammy_nomination"]]

    tables = {"dim_artist": dim_artist, "dim_track": dim_track, "dim_genre": dim_genre, "dim_year": dim_year,
              "dim_category": dim_category, "fact_track_credit": fact_track, "fact_grammy_nomination": fact_grammy}

    # ---- reconciliation: nothing may be lost on the way (a failed check stops the batch) ----
    checks = {
        "every_grammy_row_is_in_the_fact": fact_grammy["nomination_source_id"].nunique() == len(grammy_raw),
        "every_track_is_in_dim_track_and_the_fact": fact_track["track_key"].nunique() == len(dim_track) == spotify["track_id"].nunique(),
        "every_track_genre_listing_is_in_the_fact":
            len(fact_track[["track_key", "genre_key"]].drop_duplicates()) == len(spotify.drop_duplicates(["track_id", "track_genre"])),
    }
    linked = fact_grammy.merge(dim_artist[["artist_key", "match_status"]], on="artist_key")
    report.update({
        "spotify_rows_clean": len(spotify), "spotify_tracks": len(dim_track), "spotify_genres": len(dim_genre),
        "spotify_artist_credits_in_fact": len(fact_track),
        "grammy_rows_raw": len(grammy_raw), "grammy_fact_rows": len(fact_grammy),
        "grammy_rows_by_artist_source": {k: int(v) for k, v in gr_credits.groupby("nomination_source_id")["artist_source"].first().value_counts().items()},
        "grammy_rows_with_several_artists": int((gr_credits.groupby("nomination_source_id").size() > 1).sum()),
        "grammy_fact_rows_by_match_status": {k: int(v) for k, v in linked["match_status"].value_counts().items()},
        "grammy_fact_rows_in_other_group_share": round(float((fact_grammy.merge(dim_category[["category_key", "category_group"]], on="category_key")["category_group"] == "Other").mean()), 4),
        "artists_in_dimension": len(dim_artist), "artists_by_match_status": {k: int(v) for k, v in dim_artist["match_status"].value_counts().items()},
        "checks": checks, "tables": {name: len(df) for name, df in tables.items()},
    })

    prepared_dir = config.batch_dir(batch_id) / "prepared"
    prepared_dir.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        df.to_parquet(prepared_dir / f"{name}.parquet", index=False)
    report_dir = config.evidence_dir() / "transform"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_file = report_dir / f"transform_{batch_id}.json"
    report_file.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    failed = [name for name, ok in checks.items() if not ok]
    print(f"[transform] batch={batch_id} spotify rows {report['spotify_rows_raw']} -> {len(spotify)} | grammy rows {len(grammy_raw)} -> {len(fact_grammy)} fact rows | "
          f"tables={report['tables']} | report={report_file}")
    if failed:
        raise ValueError(f"Reconciliation failed after the transformation: {failed}")
    return {"batch_id": batch_id, "prepared_dir": str(prepared_dir), "tables": report["tables"], "report_file": str(report_file)}


def read_prepared(prepared_dir):
    """The prepared tables of a batch as a dictionary of DataFrames."""
    return {name: pd.read_parquet(f"{prepared_dir}/{name}.parquet") for name in PREPARED_TABLES}
