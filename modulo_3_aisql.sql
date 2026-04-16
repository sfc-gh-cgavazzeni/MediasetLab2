-- ============================================================================
-- MODULO 3: AI SQL - Funzioni Cortex AI
-- Mediaset Workshop 2 - AI & Cortex
-- ============================================================================
-- Questo script contiene esempi di tutte le funzioni AI SQL di Snowflake
-- applicate ai dati televisivi Mediaset. E' il companion SQL del notebook
-- Mediaset_AISQL_Lab.ipynb per chi preferisce lavorare in un worksheet.
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE MEDIASET_LAB;
USE SCHEMA RAW;
USE WAREHOUSE MEDIASET_WH;

-- ============================================================================
-- 1. SENTIMENT ANALYSIS
-- ============================================================================
-- SNOWFLAKE.CORTEX.SENTIMENT() analizza il tono emotivo del testo
-- e restituisce un punteggio da -1 (molto negativo) a +1 (molto positivo).

-- 1a. Sentiment su tutti i feedback social
SELECT
    FEEDBACK_ID,
    PIATTAFORMA,
    SUBSTR(TESTO_FEEDBACK, 1, 80) || '...' AS TESTO_BREVE,
    ROUND(SNOWFLAKE.CORTEX.SENTIMENT(TESTO_FEEDBACK), 3) AS SENTIMENT_SCORE,
    CASE
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(TESTO_FEEDBACK) > 0.3 THEN 'POSITIVO'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(TESTO_FEEDBACK) < -0.3 THEN 'NEGATIVO'
        ELSE 'NEUTRO'
    END AS SENTIMENT_LABEL
FROM MEDIASET_LAB.RAW.FEEDBACK_SOCIAL
ORDER BY SENTIMENT_SCORE ASC;

-- 1b. Statistiche sentiment aggregate per piattaforma
SELECT
    PIATTAFORMA,
    COUNT(*) AS NUM_FEEDBACK,
    ROUND(AVG(SNOWFLAKE.CORTEX.SENTIMENT(TESTO_FEEDBACK)), 3) AS AVG_SENTIMENT,
    ROUND(MIN(SNOWFLAKE.CORTEX.SENTIMENT(TESTO_FEEDBACK)), 3) AS MIN_SENTIMENT,
    ROUND(MAX(SNOWFLAKE.CORTEX.SENTIMENT(TESTO_FEEDBACK)), 3) AS MAX_SENTIMENT,
    SUM(CASE WHEN SNOWFLAKE.CORTEX.SENTIMENT(TESTO_FEEDBACK) < -0.3 THEN 1 ELSE 0 END) AS NEGATIVI,
    SUM(CASE WHEN SNOWFLAKE.CORTEX.SENTIMENT(TESTO_FEEDBACK) > 0.3 THEN 1 ELSE 0 END) AS POSITIVI
FROM MEDIASET_LAB.RAW.FEEDBACK_SOCIAL
GROUP BY PIATTAFORMA
ORDER BY AVG_SENTIMENT ASC;

-- 1c. Sentiment per programma (join con PROGRAMMI_TV)
SELECT
    p.TITOLO,
    p.GENERE,
    COUNT(*) AS NUM_FEEDBACK,
    ROUND(AVG(SNOWFLAKE.CORTEX.SENTIMENT(f.TESTO_FEEDBACK)), 3) AS SENTIMENT_MEDIO
FROM MEDIASET_LAB.RAW.FEEDBACK_SOCIAL f
JOIN MEDIASET_LAB.RAW.PROGRAMMI_TV p ON f.PROGRAMMA_ID = p.PROGRAMMA_ID
GROUP BY p.TITOLO, p.GENERE
ORDER BY SENTIMENT_MEDIO DESC;

-- ============================================================================
-- 2. SUMMARIZE - Riassunto Automatico
-- ============================================================================
-- SNOWFLAKE.CORTEX.SUMMARIZE() genera un riassunto conciso di testi lunghi.

-- 2a. Riassunto delle descrizioni complete dei programmi
SELECT
    TITOLO,
    SNOWFLAKE.CORTEX.SUMMARIZE(DESCRIZIONE_COMPLETA) AS RIASSUNTO
FROM MEDIASET_LAB.RAW.CONTENUTI_DESCRIZIONI
WHERE LENGTH(DESCRIZIONE_COMPLETA) > 100
LIMIT 5;

-- 2b. Riassunto combinato: descrizione + cast + parole chiave
SELECT
    TITOLO,
    SNOWFLAKE.CORTEX.SUMMARIZE(
        CONCAT(
            'Programma: ', TITOLO, '. ',
            'Descrizione: ', COALESCE(DESCRIZIONE_COMPLETA, ''), '. ',
            'Cast: ', COALESCE(CAST_PRINCIPALE, ''), '. ',
            'Parole chiave: ', COALESCE(PAROLE_CHIAVE, '')
        )
    ) AS RIASSUNTO_COMPLETO
FROM MEDIASET_LAB.RAW.CONTENUTI_DESCRIZIONI
LIMIT 5;

-- ============================================================================
-- 3. TRANSLATE - Traduzione Multilingua
-- ============================================================================
-- SNOWFLAKE.CORTEX.TRANSLATE() traduce testo tra lingue diverse.

-- 3a. Traduzione feedback in inglese e spagnolo
SELECT
    FEEDBACK_ID,
    TESTO_FEEDBACK AS ORIGINALE_IT,
    SNOWFLAKE.CORTEX.TRANSLATE(TESTO_FEEDBACK, 'it', 'en') AS TRADUZIONE_EN,
    SNOWFLAKE.CORTEX.TRANSLATE(TESTO_FEEDBACK, 'it', 'es') AS TRADUZIONE_ES
FROM MEDIASET_LAB.RAW.FEEDBACK_SOCIAL
LIMIT 5;

-- 3b. Traduzione descrizioni programmi in inglese (per catalogo internazionale)
SELECT
    TITOLO,
    SNOWFLAKE.CORTEX.TRANSLATE(DESCRIZIONE_BREVE, 'it', 'en') AS DESCRIZIONE_EN,
    SNOWFLAKE.CORTEX.TRANSLATE(DESCRIZIONE_BREVE, 'it', 'fr') AS DESCRIZIONE_FR
FROM MEDIASET_LAB.RAW.CONTENUTI_DESCRIZIONI
LIMIT 5;

-- ============================================================================
-- 4. COMPLETE - Prompt LLM Generici
-- ============================================================================
-- SNOWFLAKE.CORTEX.COMPLETE() invia prompt a un LLM per analisi avanzate.

-- 4a. Analisi feedback con suggerimento azione editoriale
SELECT
    FEEDBACK_ID,
    TESTO_FEEDBACK,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        CONCAT(
            'Sei un esperto di contenuti televisivi italiani. ',
            'Analizza il seguente feedback del pubblico su un programma Mediaset e fornisci: ',
            '1) Sentiment (POSITIVO/NEGATIVO/NEUTRO) ',
            '2) Tema principale del commento ',
            '3) Azione suggerita per la redazione in una frase. ',
            'Feedback: ', TESTO_FEEDBACK
        )
    ) AS ANALISI_AI
FROM MEDIASET_LAB.RAW.FEEDBACK_SOCIAL
LIMIT 5;

-- 4b. Generazione mini-report ascolti con AI Complete
WITH stats AS (
    SELECT
        ROUND(SUM(TELESPETTATORI), 0) AS TOT_TELESPETTATORI,
        ROUND(AVG(SHARE_PERCENTUALE), 2) AS AVG_SHARE,
        COUNT(DISTINCT PROGRAMMA_ID) AS NUM_PROGRAMMI,
        COUNT(DISTINCT REGIONE) AS NUM_REGIONI,
        COUNT(*) AS NUM_RILEVAZIONI
    FROM MEDIASET_LAB.RAW.ASCOLTI
)
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large2',
    CONCAT(
        'Genera un breve report in italiano (max 200 parole) sugli ascolti televisivi Mediaset. ',
        'Dati: Telespettatori totali = ', TOT_TELESPETTATORI::VARCHAR, ', ',
        'Share medio = ', AVG_SHARE::VARCHAR, '%, ',
        'Programmi monitorati = ', NUM_PROGRAMMI::VARCHAR, ', ',
        'Regioni coperte = ', NUM_REGIONI::VARCHAR, ', ',
        'Rilevazioni totali = ', NUM_RILEVAZIONI::VARCHAR, '. ',
        'Includi osservazioni e raccomandazioni per il palinsesto.'
    )
) AS REPORT_ASCOLTI
FROM stats;

-- ============================================================================
-- 5. CLASSIFY_TEXT - Classificazione Automatica
-- ============================================================================
-- SNOWFLAKE.CORTEX.CLASSIFY_TEXT() classifica il testo in categorie predefinite.

-- 5a. Classificazione feedback per tipo
SELECT
    FEEDBACK_ID,
    SUBSTR(TESTO_FEEDBACK, 1, 80) || '...' AS TESTO_BREVE,
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        TESTO_FEEDBACK,
        ['Entusiasta', 'Critica costruttiva', 'Reclamo', 'Suggerimento', 'Neutro']
    ):label::VARCHAR AS CLASSIFICAZIONE,
    ROUND(SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        TESTO_FEEDBACK,
        ['Entusiasta', 'Critica costruttiva', 'Reclamo', 'Suggerimento', 'Neutro']
    ):score::FLOAT, 3) AS CONFIDENCE
FROM MEDIASET_LAB.RAW.FEEDBACK_SOCIAL
LIMIT 15;

-- 5b. Distribuzione classificazioni
SELECT
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        TESTO_FEEDBACK,
        ['Entusiasta', 'Critica costruttiva', 'Reclamo', 'Suggerimento', 'Neutro']
    ):label::VARCHAR AS CLASSIFICAZIONE,
    COUNT(*) AS CONTEGGIO
FROM MEDIASET_LAB.RAW.FEEDBACK_SOCIAL
GROUP BY CLASSIFICAZIONE
ORDER BY CONTEGGIO DESC;

-- ============================================================================
-- 6. EXTRACT_ANSWER - Estrazione Informazioni
-- ============================================================================
-- SNOWFLAKE.CORTEX.EXTRACT_ANSWER() estrae risposte specifiche da un testo.

-- 6a. Estrazione programma e opinione dai feedback
SELECT
    FEEDBACK_ID,
    TESTO_FEEDBACK,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        TESTO_FEEDBACK,
        'Quale programma televisivo viene menzionato o commentato?'
    ) AS PROGRAMMA_MENZIONATO,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        TESTO_FEEDBACK,
        'Qual e'' l''aspetto positivo o negativo principale evidenziato?'
    ) AS ASPETTO_PRINCIPALE
FROM MEDIASET_LAB.RAW.FEEDBACK_SOCIAL
LIMIT 10;

-- 6b. Estrazione informazioni dalle descrizioni dei programmi
SELECT
    TITOLO,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        DESCRIZIONE_COMPLETA,
        'Chi e'' il conduttore o presentatore del programma?'
    ) AS CONDUTTORE,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        DESCRIZIONE_COMPLETA,
        'Qual e'' il formato o la meccanica del programma?'
    ) AS FORMATO
FROM MEDIASET_LAB.RAW.CONTENUTI_DESCRIZIONI
LIMIT 10;
