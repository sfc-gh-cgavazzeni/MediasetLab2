-- ============================================================================
-- MODULO 1: Cortex Analyst, Semantic Views e Snowflake Intelligence
-- Mediaset Workshop 2 - AI & Cortex
-- ============================================================================
-- Questo script contiene le query di verifica e test per il Modulo 1.
-- La Semantic View e il Cortex Agent vengono creati tramite l'interfaccia
-- grafica di Snowsight (vedi la Guida Partecipanti per le istruzioni).
-- ============================================================================

-- Prerequisito: assicurarsi che i dati del Workshop 1 siano caricati
USE ROLE SYSADMIN;
USE DATABASE MEDIASET_LAB;
USE SCHEMA RAW;
USE WAREHOUSE MEDIASET_WH;

-- ============================================================================
-- SEZIONE 1: Verifica Prerequisiti (dati Workshop 1)
-- ============================================================================

-- Verifica che tutte le tabelle siano presenti e popolate
SELECT 'PROGRAMMI_TV' as tabella, COUNT(*) as righe FROM MEDIASET_LAB.RAW.PROGRAMMI_TV
UNION ALL SELECT 'PALINSESTO', COUNT(*) FROM MEDIASET_LAB.RAW.PALINSESTO
UNION ALL SELECT 'ASCOLTI', COUNT(*) FROM MEDIASET_LAB.RAW.ASCOLTI
UNION ALL SELECT 'ABBONATI', COUNT(*) FROM MEDIASET_LAB.RAW.ABBONATI
UNION ALL SELECT 'CONTENUTI_DESCRIZIONI', COUNT(*) FROM MEDIASET_LAB.RAW.CONTENUTI_DESCRIZIONI
UNION ALL SELECT 'CONTRATTI_PUBBLICITARI', COUNT(*) FROM MEDIASET_LAB.RAW.CONTRATTI_PUBBLICITARI
UNION ALL SELECT 'FEEDBACK_SOCIAL', COUNT(*) FROM MEDIASET_LAB.RAW.FEEDBACK_SOCIAL
ORDER BY tabella;

-- Risultati attesi:
-- ABBONATI: 20, ASCOLTI: 5000, CONTENUTI_DESCRIZIONI: 20,
-- CONTRATTI_PUBBLICITARI: 15, FEEDBACK_SOCIAL: 20, PALINSESTO: 600, PROGRAMMI_TV: 20

-- Verifica che lo schema ANALYTICS esista (necessario per la Semantic View)
SHOW SCHEMAS IN DATABASE MEDIASET_LAB;

-- Se lo schema ANALYTICS non esiste, crearlo:
-- CREATE SCHEMA IF NOT EXISTS MEDIASET_LAB.ANALYTICS;

-- ============================================================================
-- SEZIONE 2: Verifica Semantic View (dopo creazione via UI)
-- ============================================================================

-- Verifica che la Semantic View sia stata creata
SHOW SEMANTIC VIEWS IN SCHEMA MEDIASET_LAB.ANALYTICS;

-- Descrivi la struttura della Semantic View
DESCRIBE SEMANTIC VIEW MEDIASET_LAB.ANALYTICS.MEDIASET_TV_INTELLIGENCE;

-- ============================================================================
-- SEZIONE 3: Test Cortex Analyst - Domande Esempio
-- ============================================================================
-- Queste domande possono essere usate nel Playground di Cortex Analyst
-- (accessibile da Catalog > Semantic View > ... > Open with Cortex Analyst)
-- oppure nell'interfaccia di Snowflake Intelligence dopo aver creato l'agente.
--
-- DOMANDE SUGGERITE PER IL TEST:
--
-- 📊 Ascolti e Share:
--   "Quali sono i top 5 programmi per share medio?"
--   "Qual è lo share medio in prime time per canale?"
--   "Come si distribuiscono i telespettatori per regione?"
--   "Quanti telespettatori totali per fascia oraria?"
--
-- 📺 Programmi e Palinsesto:
--   "Quanti programmi abbiamo per genere?"
--   "Qual è il costo medio di produzione per canale?"
--   "Quante prime TV e quante repliche nel palinsesto?"
--   "Quali programmi vanno in onda in prima serata su Canale 5?"
--
-- 💰 Pubblicità:
--   "Qual è il budget pubblicitario totale per settore merceologico?"
--   "Quali sono i top 3 inserzionisti per budget?"
--   "Quanti contratti attivi abbiamo per fascia oraria target?"
--
-- 👥 Abbonati:
--   "Quanti abbonati Premium abbiamo?"
--   "Qual è l'importo medio per tipo abbonamento?"
--   "Come si distribuiscono gli abbonati per regione?"
--
-- 💬 Feedback Social:
--   "Quanti feedback abbiamo per piattaforma social?"
--   "Quali programmi hanno più likes sui social?"
--   "Qual è il totale condivisioni per programma?"

-- ============================================================================
-- SEZIONE 4: Verifica Cortex Agent (dopo creazione via UI)
-- ============================================================================

-- Verifica che l'agente sia stato creato
SHOW CORTEX AGENTS IN SCHEMA MEDIASET_LAB.ANALYTICS;

-- ============================================================================
-- SEZIONE 5: Query SQL di supporto per verifica risultati
-- ============================================================================

-- Query 1: Top 5 programmi per share medio (per confronto con risposta AI)
SELECT 
    p.TITOLO,
    p.GENERE,
    p.CANALE,
    ROUND(AVG(a.SHARE_PERCENTUALE), 2) AS SHARE_MEDIO,
    SUM(a.TELESPETTATORI) AS TELESPETTATORI_TOTALI
FROM MEDIASET_LAB.RAW.PROGRAMMI_TV p
JOIN MEDIASET_LAB.RAW.ASCOLTI a ON p.PROGRAMMA_ID = a.PROGRAMMA_ID
GROUP BY p.TITOLO, p.GENERE, p.CANALE
ORDER BY SHARE_MEDIO DESC
LIMIT 5;

-- Query 2: Share medio per canale in Prime Time
SELECT 
    p.CANALE,
    ROUND(AVG(a.SHARE_PERCENTUALE), 2) AS SHARE_MEDIO_PRIMETIME,
    SUM(a.TELESPETTATORI) AS TELESPETTATORI_TOTALI
FROM MEDIASET_LAB.RAW.ASCOLTI a
JOIN MEDIASET_LAB.RAW.PROGRAMMI_TV p ON a.PROGRAMMA_ID = p.PROGRAMMA_ID
WHERE a.FASCIA_ORARIA = 'Prime Time'
GROUP BY p.CANALE
ORDER BY SHARE_MEDIO_PRIMETIME DESC;

-- Query 3: Budget pubblicitario per settore
SELECT 
    SETTORE_MERCEOLOGICO,
    COUNT(*) AS NUM_CONTRATTI,
    SUM(BUDGET_TOTALE_EUR) AS BUDGET_TOTALE,
    ROUND(AVG(BUDGET_TOTALE_EUR), 0) AS BUDGET_MEDIO
FROM MEDIASET_LAB.RAW.CONTRATTI_PUBBLICITARI
GROUP BY SETTORE_MERCEOLOGICO
ORDER BY BUDGET_TOTALE DESC;

-- Query 4: Abbonati per tipo
SELECT 
    TIPO_ABBONAMENTO,
    COUNT(*) AS NUM_ABBONATI,
    ROUND(AVG(IMPORTO_MENSILE), 2) AS IMPORTO_MEDIO,
    SUM(IMPORTO_MENSILE) AS RICAVO_MENSILE
FROM MEDIASET_LAB.RAW.ABBONATI
WHERE STATO_ABBONAMENTO = 'Attivo'
GROUP BY TIPO_ABBONAMENTO
ORDER BY NUM_ABBONATI DESC;

-- Query 5: Feedback social per piattaforma
SELECT 
    PIATTAFORMA,
    COUNT(*) AS NUM_FEEDBACK,
    SUM(LIKES) AS TOTALE_LIKES,
    SUM(SHARES) AS TOTALE_SHARES
FROM MEDIASET_LAB.RAW.FEEDBACK_SOCIAL
GROUP BY PIATTAFORMA
ORDER BY NUM_FEEDBACK DESC;
