-- ==============================================================================
-- Creación de la base de datos origen para los premios Grammy y el DW
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS grammy_source;
CREATE DATABASE IF NOT EXISTS music_dw;

-- Garantizar permisos completos para conexiones desde la red Docker
CREATE USER IF NOT EXISTS 'etl_user'@'%' IDENTIFIED BY 'root_password';
GRANT ALL PRIVILEGES ON *.* TO 'etl_user'@'%' WITH GRANT OPTION;
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;

USE grammy_source;

-- Tabla operacional de extracción: todos los campos como texto plano (sin limpiar prematuramente)
CREATE TABLE IF NOT EXISTS grammy_awards (
    source_row_id INT AUTO_INCREMENT PRIMARY KEY,
    year          TEXT,
    title         TEXT,
    published_at  TEXT,
    updated_at    TEXT,
    category      TEXT,
    nominee       TEXT,
    artist        TEXT,
    workers       TEXT,
    img           TEXT,
    winner        TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
