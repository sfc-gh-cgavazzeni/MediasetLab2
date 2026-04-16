-- ============================================================================
-- MODULO 2: Cortex Search - Ricerca Semantica
-- Mediaset Workshop 2 - AI & Cortex
-- ============================================================================
-- Questo script crea un servizio di ricerca semantica sui contenuti
-- descrittivi dei programmi Mediaset, consentendo ricerche in linguaggio
-- naturale (es. "programmi di satira politica" o "talent show musicale").
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE MEDIASET_LAB;
USE SCHEMA RAW;
USE WAREHOUSE MEDIASET_WH;

-- ============================================================================
-- STEP 2.1: Creazione della tabella CONTENUTI_CHUNKS
-- ============================================================================
-- Per Cortex Search, prepariamo un'unica colonna di testo ricco che
-- concatena tutte le informazioni rilevanti di ogni programma.
-- Questo approccio "chunking" migliora la qualita' della ricerca semantica.

CREATE OR REPLACE TABLE MEDIASET_LAB.RAW.CONTENUTI_CHUNKS AS
SELECT
    CONTENUTO_ID,
    PROGRAMMA_ID,
    TITOLO,
    CONCAT(
        'Titolo: ', TITOLO, '\n',
        'Descrizione: ', COALESCE(DESCRIZIONE_BREVE, ''), '\n',
        'Trama completa: ', COALESCE(DESCRIZIONE_COMPLETA, ''), '\n',
        'Cast principale: ', COALESCE(CAST_PRINCIPALE, ''), '\n',
        'Regista: ', COALESCE(REGISTA, ''), '\n',
        'Genere/Parole chiave: ', COALESCE(PAROLE_CHIAVE, ''), '\n',
        'Anno produzione: ', COALESCE(ANNO_PRODUZIONE::VARCHAR, ''), '\n',
        'Paese: ', COALESCE(PAESE_ORIGINE, ''), '\n',
        'Lingua: ', COALESCE(LINGUA_ORIGINALE, '')
    ) AS CHUNK_TEXT
FROM MEDIASET_LAB.RAW.CONTENUTI_DESCRIZIONI;

-- Verifica i chunks creati
SELECT CONTENUTO_ID, TITOLO, LENGTH(CHUNK_TEXT) AS LUNGHEZZA_CHUNK
FROM MEDIASET_LAB.RAW.CONTENUTI_CHUNKS
ORDER BY CONTENUTO_ID;

-- Visualizza un esempio di chunk completo
SELECT CHUNK_TEXT
FROM MEDIASET_LAB.RAW.CONTENUTI_CHUNKS
LIMIT 1;

-- ============================================================================
-- STEP 2.2: Creazione del Cortex Search Service
-- ============================================================================
-- Il servizio indicizza i chunks di testo e consente ricerche semantiche
-- in linguaggio naturale. Snowflake gestisce automaticamente gli embeddings.

CREATE OR REPLACE CORTEX SEARCH SERVICE MEDIASET_LAB.RAW.RICERCA_CONTENUTI
  ON CHUNK_TEXT
  ATTRIBUTES TITOLO
  WAREHOUSE = MEDIASET_WH
  TARGET_LAG = '1 hour'
  COMMENT = 'Servizio di ricerca semantica sui contenuti e descrizioni dei programmi televisivi Mediaset'
AS (
    SELECT
        CHUNK_TEXT,
        TITOLO,
        CONTENUTO_ID,
        PROGRAMMA_ID
    FROM MEDIASET_LAB.RAW.CONTENUTI_CHUNKS
);

-- Verifica che il servizio sia stato creato
SHOW CORTEX SEARCH SERVICES IN SCHEMA MEDIASET_LAB.RAW;

-- ============================================================================
-- STEP 2.3: Test Ricerca Semantica
-- ============================================================================
-- Usiamo SEARCH_PREVIEW per testare il servizio direttamente in SQL.
-- La ricerca semantica comprende il significato, non solo le parole esatte.

-- Test 1: Cercare programmi di satira
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'MEDIASET_LAB.RAW.RICERCA_CONTENUTI',
        '{
            "query": "programmi di satira e commedia politica",
            "columns": ["TITOLO", "CHUNK_TEXT"],
            "limit": 3
        }'
    )
) AS risultati;

-- Test 2: Cercare talent show musicali
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'MEDIASET_LAB.RAW.RICERCA_CONTENUTI',
        '{
            "query": "talent show musicale con giovani cantanti",
            "columns": ["TITOLO", "CHUNK_TEXT"],
            "limit": 3
        }'
    )
) AS risultati;

-- Test 3: Cercare quiz e giochi a premi
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'MEDIASET_LAB.RAW.RICERCA_CONTENUTI',
        '{
            "query": "quiz televisivo con domande e premi in denaro",
            "columns": ["TITOLO", "CHUNK_TEXT"],
            "limit": 3
        }'
    )
) AS risultati;

-- Test 4: Cercare reality show
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'MEDIASET_LAB.RAW.RICERCA_CONTENUTI',
        '{
            "query": "reality show con concorrenti che vivono insieme",
            "columns": ["TITOLO", "CHUNK_TEXT"],
            "limit": 3
        }'
    )
) AS risultati;

-- Test 5: Cercare programmi di informazione e inchieste
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'MEDIASET_LAB.RAW.RICERCA_CONTENUTI',
        '{
            "query": "programmi di giornalismo investigativo e inchieste",
            "columns": ["TITOLO", "CHUNK_TEXT"],
            "limit": 3
        }'
    )
) AS risultati;

-- ============================================================================
-- STEP 2.4: Integrazione con Snowflake Intelligence
-- ============================================================================
-- Dopo aver creato il servizio Cortex Search, aggiungilo come tool
-- al Cortex Agent MEDIASET_INTELLIGENCE_AGENT tramite Snowsight UI:
--
-- 1. Vai su AI & ML > Snowflake Intelligence
-- 2. Clicca su MEDIASET_INTELLIGENCE_AGENT per modificarlo
-- 3. Clicca su "+ Add Tool" > "Search Service"
-- 4. Seleziona MEDIASET_LAB.RAW.RICERCA_CONTENUTI
-- 5. Descrizione: "Cerca nelle descrizioni e schede dei programmi TV Mediaset"
-- 6. Salva le modifiche
--
-- Test domande combinate (dati strutturati + ricerca semantica):
--   "Quali programmi comici abbiamo? Descrivi i più popolari"
--   "Cerca programmi adatti a un pubblico giovane e dimmi il loro share"
--   "Trovami programmi con Gerry Scotti e mostrami i loro ascolti"

-- ============================================================================
-- PULIZIA (opzionale, eseguire solo a fine workshop)
-- ============================================================================
-- DROP CORTEX SEARCH SERVICE MEDIASET_LAB.RAW.RICERCA_CONTENUTI;
-- DROP TABLE MEDIASET_LAB.RAW.CONTENUTI_CHUNKS;
