CREATE TABLE IF NOT EXISTS dim_artist (
    artist_key        INT          NOT NULL AUTO_INCREMENT COMMENT 'surrogate key; -1 = unknown artist',
    artist_name_norm  VARCHAR(255) NOT NULL COMMENT 'integration key: accent-free, case-folded, punctuation-free name',
    artist_name       VARCHAR(255) NOT NULL COMMENT 'display name (most frequent raw spelling)',
    match_status      VARCHAR(12)  NOT NULL COMMENT 'MATCHED = in both sources',
    grammy_status     VARCHAR(8)   NOT NULL DEFAULT 'None' COMMENT 'Winner / Nominee / None (derived from fact_grammy_nomination)',
    PRIMARY KEY (artist_key),
    UNIQUE KEY uq_dim_artist_norm (artist_name_norm),
    CONSTRAINT ck_dim_artist_match  CHECK (match_status  IN ('MATCHED', 'SPOTIFY_ONLY', 'GRAMMY_ONLY', 'UNKNOWN')),
    CONSTRAINT ck_dim_artist_status CHECK (grammy_status IN ('Winner', 'Nominee', 'None'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE IF NOT EXISTS dim_track (
    track_key    INT           NOT NULL AUTO_INCREMENT COMMENT 'surrogate key',
    track_id     VARCHAR(32)   NOT NULL COMMENT 'Spotify track id (business key)',
    track_name   VARCHAR(1000) NULL COMMENT 'longest name in the data: 511 characters',
    album_name   VARCHAR(500)  NULL COMMENT 'attribute only: the source has no album id',
    is_explicit  TINYINT(1)    NULL,
    PRIMARY KEY (track_key),
    UNIQUE KEY uq_dim_track_id (track_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE IF NOT EXISTS dim_genre (
    genre_key   INT           NOT NULL AUTO_INCREMENT COMMENT 'surrogate key',
    genre_name  VARCHAR(100)  NOT NULL COMMENT 'Spotify track_genre label (business key)',
    PRIMARY KEY (genre_key),
    UNIQUE KEY uq_dim_genre_name (genre_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE IF NOT EXISTS dim_year (
    year_key        SMALLINT     NOT NULL COMMENT 'Grammy ceremony year (natural key)',
    decade          SMALLINT     NOT NULL COMMENT 'e.g. 1990',
    decade_label    VARCHAR(6)   NOT NULL COMMENT 'e.g. 1990s',
    ceremony_title  VARCHAR(255) NULL,
    PRIMARY KEY (year_key),
    CONSTRAINT ck_dim_year_decade CHECK (decade = (year_key DIV 10) * 10)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE IF NOT EXISTS dim_category (
    category_key        INT          NOT NULL AUTO_INCREMENT COMMENT 'surrogate key',
    category_name_norm  VARCHAR(255) NOT NULL COMMENT 'business key: trimmed, single-spaced, case-folded',
    category_name       VARCHAR(255) NOT NULL COMMENT 'display name',
    category_group      VARCHAR(40)  NOT NULL COMMENT 'derived family (rule set versioned in the transformation decision record)',
    PRIMARY KEY (category_key),
    UNIQUE KEY uq_dim_category_norm (category_name_norm)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

-- ---- Star 1 : Spotify -----------------------------------------------------------------------------------
-- Grain: one row per (track, genre listing, credited artist).
CREATE TABLE IF NOT EXISTS fact_track_credit (
    track_key         INT               NOT NULL,
    genre_key         INT               NOT NULL,
    artist_key        INT               NOT NULL,
    artist_position   TINYINT UNSIGNED  NOT NULL COMMENT '1 = first credited (primary) artist',
    popularity        TINYINT UNSIGNED  NOT NULL COMMENT 'index 0-100 (snapshot)',
    duration_ms       INT UNSIGNED      NULL,
    danceability      FLOAT             NULL,
    energy            FLOAT             NULL,
    loudness          FLOAT             NULL COMMENT 'dB',
    speechiness       FLOAT             NULL,
    acousticness      FLOAT             NULL,
    instrumentalness  FLOAT             NULL,
    liveness          FLOAT             NULL,
    valence           FLOAT             NULL,
    tempo             FLOAT             NULL COMMENT 'BPM',
    PRIMARY KEY (track_key, genre_key, artist_key),
    KEY ix_ftc_artist (artist_key),
    KEY ix_ftc_genre  (genre_key),
    CONSTRAINT fk_ftc_track  FOREIGN KEY (track_key)  REFERENCES dim_track  (track_key),
    CONSTRAINT fk_ftc_genre  FOREIGN KEY (genre_key)  REFERENCES dim_genre  (genre_key),
    CONSTRAINT fk_ftc_artist FOREIGN KEY (artist_key) REFERENCES dim_artist (artist_key),
    CONSTRAINT ck_ftc_popularity CHECK (popularity BETWEEN 0 AND 100),
    CONSTRAINT ck_ftc_position   CHECK (artist_position >= 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

-- ---- Star 2 : Grammy ------------------------------------------------------------------------------------
-- Grain: one row per (Grammy source row, credited artist).
CREATE TABLE IF NOT EXISTS fact_grammy_nomination (
    nomination_source_id  INT UNSIGNED      NOT NULL COMMENT 'degenerate dimension: grammy_source.grammy_awards.source_row_id',
    artist_key            INT               NOT NULL,
    year_key              SMALLINT          NOT NULL,
    category_key          INT               NOT NULL,
    nominee_name          VARCHAR(1000)     NULL COMMENT 'degenerate attribute: work or person nominated',
    artist_source         VARCHAR(16)       NOT NULL COMMENT 'artist / nominee_fallback / unknown (provenance of artist_key)',
    nomination_count      TINYINT UNSIGNED  NOT NULL DEFAULT 1,
    win_count             TINYINT UNSIGNED  NOT NULL DEFAULT 0,
    PRIMARY KEY (nomination_source_id, artist_key),
    KEY ix_fgn_artist   (artist_key),
    KEY ix_fgn_year     (year_key),
    KEY ix_fgn_category (category_key),
    CONSTRAINT fk_fgn_artist   FOREIGN KEY (artist_key)   REFERENCES dim_artist   (artist_key),
    CONSTRAINT fk_fgn_year     FOREIGN KEY (year_key)     REFERENCES dim_year     (year_key),
    CONSTRAINT fk_fgn_category FOREIGN KEY (category_key) REFERENCES dim_category (category_key),
    CONSTRAINT ck_fgn_source CHECK (artist_source IN ('artist', 'nominee_fallback', 'unknown')),
    CONSTRAINT ck_fgn_win    CHECK (win_count IN (0, 1))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

-- ---- Reserved member: unknown artist ---------------------------------------------------------------------
INSERT INTO dim_artist (artist_key, artist_name_norm, artist_name, match_status, grammy_status)
VALUES (-1, '(unknown artist)', 'Unknown artist', 'UNKNOWN', 'None')
ON DUPLICATE KEY UPDATE artist_name = VALUES(artist_name);
