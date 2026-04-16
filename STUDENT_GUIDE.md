# Workshop Snowflake AI & Cortex per Mediaset

## Guida per i Partecipanti

---

## Informazioni Workshop

|  |  |
|---|---|
| **Durata** | 2 ore |
| **Livello** | Intermedio |
| **Prerequisiti** | Completamento Workshop 1 (dati caricati in MEDIASET_LAB), conoscenze base SQL |
| **Settore** | TV Broadcasting |

---

## Agenda

| Orario | Modulo | Durata |
|--------|--------|--------|
| 00:00 - 00:15 | Modulo 1: Snowpark DataFrames in Notebook | 15 min |
| 00:15 - 01:00 | Modulo 2: Cortex Analyst, Semantic Views e Snowflake Intelligence | 45 min |
| 01:00 - 01:30 | Modulo 3: Cortex Search - Ricerca Semantica | 30 min |
| 01:30 - 02:00 | Modulo 4: AI SQL in Notebook (Sentiment, Classify, Summarize, Translate) | 30 min |

---

## Prerequisiti

> Questo workshop richiede che i dati del **Workshop 1** siano già caricati nel database `MEDIASET_LAB`. Verifica eseguendo lo script `modulo_1_setup_verifiche.sql` (Sezione 1) oppure questa query:

```sql
SELECT 'PROGRAMMI_TV' as tabella, COUNT(*) as righe FROM MEDIASET_LAB.RAW.PROGRAMMI_TV
UNION ALL SELECT 'PALINSESTO', COUNT(*) FROM MEDIASET_LAB.RAW.PALINSESTO
UNION ALL SELECT 'ASCOLTI', COUNT(*) FROM MEDIASET_LAB.RAW.ASCOLTI
UNION ALL SELECT 'ABBONATI', COUNT(*) FROM MEDIASET_LAB.RAW.ABBONATI
UNION ALL SELECT 'CONTENUTI_DESCRIZIONI', COUNT(*) FROM MEDIASET_LAB.RAW.CONTENUTI_DESCRIZIONI
UNION ALL SELECT 'CONTRATTI_PUBBLICITARI', COUNT(*) FROM MEDIASET_LAB.RAW.CONTRATTI_PUBBLICITARI
UNION ALL SELECT 'FEEDBACK_SOCIAL', COUNT(*) FROM MEDIASET_LAB.RAW.FEEDBACK_SOCIAL
ORDER BY tabella;
```

**Risultati attesi:** ABBONATI (20), ASCOLTI (5000), CONTENUTI_DESCRIZIONI (20), CONTRATTI_PUBBLICITARI (15), FEEDBACK_SOCIAL (20), PALINSESTO (600), PROGRAMMI_TV (20).

Se le tabelle non sono presenti, eseguire prima lo script `01_setup_data.sql` del Workshop 1.

---

## Modulo 1: Snowpark DataFrames in Notebook (15 min)

> **Snowpark** è il framework Python di Snowflake che permette di manipolare dati con un'API DataFrame simile a Pandas/PySpark, ma il codice viene eseguito direttamente nel cloud Snowflake. Non è necessario scaricare i dati in locale.

### Obiettivi di Apprendimento

- Creare e manipolare DataFrame Snowpark in un notebook
- Eseguire operazioni di select, filter, group_by e aggregazione
- Effettuare join tra DataFrame di tabelle diverse
- Utilizzare window functions per ranking
- Salvare risultati in nuove tabelle
- Creare visualizzazioni con matplotlib

### Istruzioni Passo-Passo

#### Step 1.1: Importare il Notebook Snowpark

1. Nel menu laterale, vai su **Projects** > **Workspaces**
2. Clicca su **"..."** (menu) > **Import** > **Import .ipynb file**
3. Seleziona il file `Mediaset_Snowpark_Lab.ipynb` fornito
4. Il notebook si apre con celle Python pronte per l'esecuzione

#### Step 1.2: Setup Sessione Snowpark

Esegui la prima cella per ottenere la sessione attiva:

```python
from snowflake.snowpark.context import get_active_session
session = get_active_session()
session.use_database("MEDIASET_LAB")
session.use_schema("RAW")
print("Sessione Snowpark attiva!")
```

> **Nota:** In un notebook Snowflake, la sessione è già disponibile tramite `get_active_session()`. Non è necessario configurare credenziali.

#### Step 1.3: Caricamento DataFrame

```python
df_programmi = session.table("MEDIASET_LAB.RAW.PROGRAMMI_TV")
df_ascolti = session.table("MEDIASET_LAB.RAW.ASCOLTI")

print(f"Programmi TV: {df_programmi.count()} righe")
print(f"Ascolti: {df_ascolti.count()} righe")

df_programmi.show()
```

> **Nota:** `session.table()` crea un DataFrame **lazy** — non scarica i dati in memoria. Le operazioni vengono tradotte in SQL ed eseguite nel warehouse Snowflake.

#### Step 1.4: Operazioni Base

```python
from snowflake.snowpark.functions import col

# Select colonne specifiche
df_programmi.select("TITOLO", "GENERE", "CANALE", "COSTO_EPISODIO_EUR").show()

# Filter: solo programmi di Canale 5
df_canale5 = df_programmi.filter(col("CANALE") == "Canale 5")
print(f"Programmi su Canale 5: {df_canale5.count()}")
df_canale5.select("TITOLO", "GENERE").show()
```

#### Step 1.5: Aggregazioni e Join

```python
from snowflake.snowpark.functions import avg, sum as sum_, count, round as round_

# Top 10 programmi per share medio (join Ascolti + Programmi)
df_top = df_ascolti.join(df_programmi, "PROGRAMMA_ID") \
    .group_by("TITOLO", "GENERE", "CANALE") \
    .agg(
        round_(avg("SHARE_PERCENTUALE"), 2).alias("SHARE_MEDIO"),
        sum_("TELESPETTATORI").alias("TELESPETTATORI_TOTALI"),
        count("*").alias("RILEVAZIONI")
    ) \
    .sort(col("SHARE_MEDIO").desc()) \
    .limit(10)

df_top.show()
```

#### Step 1.6: Window Functions

```python
from snowflake.snowpark.functions import rank
from snowflake.snowpark import Window

# Ranking programmi per share all'interno di ogni canale
window_spec = Window.partition_by("CANALE").order_by(col("SHARE_MEDIO").desc())

df_ranking = df_ascolti.join(df_programmi, "PROGRAMMA_ID") \
    .group_by("TITOLO", "CANALE") \
    .agg(round_(avg("SHARE_PERCENTUALE"), 2).alias("SHARE_MEDIO")) \
    .with_column("RANK_IN_CANALE", rank().over(window_spec))

df_ranking.filter(col("RANK_IN_CANALE") <= 3).sort("CANALE", "RANK_IN_CANALE").show()
```

#### Step 1.7: Salvataggio e Visualizzazione

Salva i risultati nello schema ANALYTICS:

```python
df_top.write.mode("overwrite").save_as_table("MEDIASET_LAB.ANALYTICS.TOP_PROGRAMMI_SHARE")
print("Tabella salvata con successo!")
```

Crea un grafico a barre dei top programmi:

```python
import matplotlib.pyplot as plt

pdf_top = df_top.to_pandas()
fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(pdf_top["TITOLO"], pdf_top["SHARE_MEDIO"], color="#29B5E8")
ax.set_xlabel("Share Medio (%)")
ax.set_title("Top 10 Programmi Mediaset per Share Medio")
ax.invert_yaxis()
plt.tight_layout()
plt.show()
```

### Best Practice

- **Lazy evaluation**: le operazioni Snowpark non vengono eseguite finché non chiami `.show()`, `.collect()`, `.count()` o `.to_pandas()`
- **Pushdown**: tutte le operazioni vengono tradotte in SQL ed eseguite nel warehouse, non in locale
- **to_pandas()**: usa solo per piccoli dataset destinati alla visualizzazione. Per dataset grandi, lavora sempre con DataFrame Snowpark

### Esercizio Pratico 1

Usando Snowpark, scrivi un'analisi che mostri per ogni fascia oraria il numero di trasmissioni, lo share medio e il top programma. Salva il risultato in `MEDIASET_LAB.ANALYTICS.ANALISI_FASCE_ORARIE`.

---

## Modulo 2: Cortex Analyst, Semantic Views e Snowflake Intelligence (45 min)

> Le **Semantic View** sono un layer semantico dichiarativo che descrive tabelle, colonne, relazioni e metriche in modo comprensibile all'AI. **Cortex Analyst** traduce domande in linguaggio naturale in query SQL usando la Semantic View. **Snowflake Intelligence** è un agente conversazionale che combina Analyst e Search per rispondere a domande complesse.

### Obiettivi di Apprendimento

- Comprendere il concetto di Semantic View e il suo ruolo nel modello semantico
- Creare una Semantic View tramite l'interfaccia Snowsight
- Testare Cortex Analyst con domande in linguaggio naturale
- Creare un agente Snowflake Intelligence tramite Snowsight
- Interagire con l'agente e generare artifacts (grafici)

### Istruzioni Passo-Passo

#### Step 2.1: Cos'è una Semantic View

Una **Semantic View** è un oggetto Snowflake che definisce:

- **Tabelle**: quali tabelle del database includere nel modello
- **Colonne**: con descrizioni (COMMENT) e sinonimi (SYNONYMS) comprensibili all'AI
- **Relazioni**: come le tabelle sono collegate tra loro (chiavi esterne)
- **Metriche**: calcoli predefiniti (SUM, AVG, COUNT) che l'AI può usare direttamente

Grazie alla Semantic View, Cortex Analyst può tradurre domande come *"Qual è lo share medio in prime time?"* nella query SQL corretta, senza che l'utente conosca la struttura del database.

#### Step 2.2: Creazione della Semantic View tramite Snowsight

1. Nel menu laterale, vai su **Catalog** > **Database Explorer**
2. Naviga fino a **MEDIASET_LAB** > **ANALYTICS**
3. Clicca sul pulsante **Create** (in alto a destra) > **Semantic View**
4. Inserisci il nome: `MEDIASET_TV_INTELLIGENCE`
5. Clicca **Next**

**Aggiunta tabelle:**

6. Clicca **+ Add Tables** e seleziona le seguenti tabelle dallo schema `MEDIASET_LAB.RAW`:
   - `PROGRAMMI_TV`
   - `PALINSESTO`
   - `ASCOLTI`
   - `ABBONATI`
   - `CONTENUTI_DESCRIZIONI`
   - `CONTRATTI_PUBBLICITARI`
   - `FEEDBACK_SOCIAL`

**Configurazione colonne:**

7. Per ogni tabella, seleziona **tutte le colonne**
8. Per le colonne principali, verifica che le descrizioni auto-generate siano corrette e migliorale se necessario. Esempi di descrizioni raccomandate:
   - `PROGRAMMI_TV.TITOLO` → *"Nome del programma televisivo"*
   - `ASCOLTI.SHARE_PERCENTUALE` → *"Percentuale di share (quota di ascolto sul totale dei telespettatori attivi)"*
   - `ASCOLTI.FASCIA_ORARIA` → *"Fascia oraria della rilevazione: Mattina, Access Prime Time, Prime Time, Seconda Serata, Pomeriggio, Preserale"*
   - `CONTRATTI_PUBBLICITARI.BUDGET_TOTALE_EUR` → *"Budget totale del contratto pubblicitario in euro"*
   - `FEEDBACK_SOCIAL.TESTO_FEEDBACK` → *"Testo completo del commento o feedback"*

9. Aggiungi **sinonimi** alle colonne più importanti per migliorare la comprensione dell'AI:
   - `TELESPETTATORI` → sinonimi: *spettatori, audience, numero spettatori*
   - `SHARE_PERCENTUALE` → sinonimi: *share, quota di ascolto*
   - `CANALE` → sinonimi: *rete, emittente, network*
   - `GENERE` → sinonimi: *tipo programma, categoria, formato*
   - `BUDGET_TOTALE_EUR` → sinonimi: *investimento pubblicitario, spesa pubblicitaria*

**Configurazione relazioni:**

10. Nella sezione **Relationships**, aggiungi le seguenti relazioni:

| Tabella origine | Colonna | → | Tabella destinazione | Colonna |
|----------------|---------|---|---------------------|---------|
| PALINSESTO | PROGRAMMA_ID | → | PROGRAMMI_TV | PROGRAMMA_ID |
| ASCOLTI | PROGRAMMA_ID | → | PROGRAMMI_TV | PROGRAMMA_ID |
| ASCOLTI | PALINSESTO_ID | → | PALINSESTO | PALINSESTO_ID |
| FEEDBACK_SOCIAL | PROGRAMMA_ID | → | PROGRAMMI_TV | PROGRAMMA_ID |
| CONTENUTI_DESCRIZIONI | PROGRAMMA_ID | → | PROGRAMMI_TV | PROGRAMMA_ID |

**Configurazione metriche:**

11. Nella sezione **Metrics**, aggiungi le metriche calcolate:

| Nome Metrica | Espressione | Descrizione |
|-------------|-------------|-------------|
| TELESPETTATORI_TOTALI | SUM(ASCOLTI.TELESPETTATORI) | Numero totale di telespettatori |
| SHARE_MEDIO | AVG(ASCOLTI.SHARE_PERCENTUALE) | Share medio percentuale |
| NUMERO_RILEVAZIONI | COUNT(ASCOLTI.ASCOLTO_ID) | Numero totale di rilevazioni |
| TRASMISSIONI_TOTALI | COUNT(PALINSESTO.PALINSESTO_ID) | Numero totale di trasmissioni |
| NUMERO_ABBONATI | COUNT(ABBONATI.ABBONATO_ID) | Numero totale di abbonati |
| RICAVO_MENSILE_ABBONAMENTI | SUM(ABBONATI.IMPORTO_MENSILE) | Ricavo mensile totale abbonamenti |
| BUDGET_PUBBLICITARIO_TOTALE | SUM(CONTRATTI_PUBBLICITARI.BUDGET_TOTALE_EUR) | Budget pubblicitario totale |
| NUMERO_CONTRATTI | COUNT(CONTRATTI_PUBBLICITARI.CONTRATTO_ID) | Numero contratti pubblicitari |
| TOTALE_FEEDBACK | COUNT(FEEDBACK_SOCIAL.FEEDBACK_ID) | Numero feedback social |
| LIKES_TOTALI | SUM(FEEDBACK_SOCIAL.LIKES) | Totale likes social |
| SHARES_TOTALI | SUM(FEEDBACK_SOCIAL.SHARES) | Totale condivisioni social |
| COSTO_PRODUZIONE_TOTALE | SUM(PROGRAMMI_TV.COSTO_EPISODIO_EUR) | Costo totale di produzione |

12. Clicca **Create** per salvare la Semantic View

#### Step 2.3: Verifica della Semantic View

Apri un worksheet SQL ed esegui:

```sql
-- Verifica che la Semantic View sia stata creata
SHOW SEMANTIC VIEWS IN SCHEMA MEDIASET_LAB.ANALYTICS;

-- Descrivi la struttura
DESCRIBE SEMANTIC VIEW MEDIASET_LAB.ANALYTICS.MEDIASET_TV_INTELLIGENCE;
```

Puoi anche verificare dal **Catalog**: naviga a `MEDIASET_LAB` > `ANALYTICS` e dovresti vedere `MEDIASET_TV_INTELLIGENCE` nella lista degli oggetti.

#### Step 2.4: Test nel Playground di Cortex Analyst

1. Dal **Catalog**, clicca sulla Semantic View `MEDIASET_TV_INTELLIGENCE`
2. Clicca su **"..." (menu)** > **"Open with Cortex Analyst"**
3. Si apre il Playground dove puoi fare domande in linguaggio naturale

**Prova queste domande:**

- *"Quali sono i top 5 programmi per share medio?"*
- *"Qual è lo share medio in prime time per canale?"*
- *"Budget pubblicitario totale per settore merceologico"*
- *"Quanti abbonati Premium abbiamo?"*
- *"Come si distribuiscono i telespettatori per regione?"*

> **Suggerimento:** Osserva la query SQL generata da Cortex Analyst per ogni domanda. Verifica che sia corretta confrontandola con le query nella Sezione 5 dello script `modulo_1_setup_verifiche.sql`.

#### Step 2.5: Creazione del Cortex Agent (Snowflake Intelligence) tramite Snowsight

1. Nel menu laterale, vai su **AI & ML** > **Snowflake Intelligence**
2. Clicca su **"+ Create"** (in alto a destra)
3. Compila i campi:
   - **Name**: `MEDIASET_INTELLIGENCE_AGENT`
   - **Database**: `MEDIASET_LAB`
   - **Schema**: `ANALYTICS`
   - **Description**: *"Assistente AI per l'analisi dei dati televisivi Mediaset"*

**Aggiunta tool Semantic View:**

4. Nella sezione **Tools**, clicca **"+ Add Tool"**
5. Seleziona **"Analyst (Semantic View)"**
6. Scegli la Semantic View: `MEDIASET_LAB.ANALYTICS.MEDIASET_TV_INTELLIGENCE`
7. Nel campo Description del tool, inserisci: *"Interroga i dati su ascolti, palinsesto, programmi TV, abbonati, pubblicità e feedback social di Mediaset"*

**Configurazione istruzioni:**

8. Nella sezione **Instructions**, inserisci il seguente testo:

```
Sei un assistente per l'analisi dei dati televisivi di Mediaset.
Rispondi sempre in italiano.

Il tuo compito è aiutare gli utenti a esplorare i dati su:
- Ascolti televisivi (telespettatori, share, regioni, fasce orarie)
- Palinsesto (programmazione, prime TV, repliche)
- Programmi TV (generi, canali, costi di produzione)
- Abbonati (piani, stato, distribuzione geografica)
- Contratti pubblicitari (budget, inserzionisti, settori, campagne)
- Feedback social (commenti, likes, condivisioni per programma)

Linee guida:
- Usa il linguaggio televisivo italiano (share, prime time, palinsesto, audience)
- Quando presenti numeri di telespettatori, formattali in milioni (es. 1.5M)
- Quando presenti importi in euro, usa il simbolo EUR e formatta con separatore delle migliaia
- Se l'utente chiede informazioni generiche, suggerisci domande più specifiche
- Se una domanda non è collegata ai dati televisivi, rispondi cortesemente che puoi aiutare solo con analisi sui dati Mediaset
```

9. Clicca **Create** per salvare l'agente

#### Step 2.6: Test di Snowflake Intelligence

Dopo la creazione, l'agente si apre automaticamente nella chat. Prova queste domande:

**Domande su ascolti e share:**
- *"Quali sono i programmi con lo share più alto?"*
- *"Come vanno gli ascolti in prime time su Canale 5?"*
- *"Qual è la distribuzione dei telespettatori per regione?"*

**Domande su pubblicità:**
- *"Chi sono i top 3 inserzionisti per budget?"*
- *"Quanto investono nel settore alimentare?"*

**Domande sugli abbonati:**
- *"Quanti abbonati attivi abbiamo per tipo di abbonamento?"*
- *"Qual è il ricavo mensile dagli abbonamenti?"*

#### Step 2.7: Creazione di Artifacts (Grafici)

Snowflake Intelligence può generare grafici interattivi. Prova:

- *"Mostrami un grafico a barre dello share medio per canale"*
- *"Crea un grafico a torta della distribuzione dei programmi per genere"*
- *"Grafico trend telespettatori per fascia oraria"*

> **Nota:** Gli artifacts vengono generati automaticamente quando l'agente riconosce che una visualizzazione è utile. Puoi anche richiederli esplicitamente.

### Esercizio Pratico 2

Formula 3 domande personalizzate all'agente che combinino dati da tabelle diverse (es. ascolti + programmi, pubblicità + fasce orarie). Salva almeno un artifact come grafico.

---

## Modulo 3: Cortex Search - Ricerca Semantica (30 min)

> **Cortex Search** è un servizio di ricerca semantica che permette di cercare informazioni in linguaggio naturale all'interno di dati testuali. A differenza di una ricerca SQL con `LIKE`, Cortex Search comprende il **significato** delle parole: cercando *"programmi di satira"* troverà anche risultati che contengono *"comicità politica"* o *"umorismo"*.

### Obiettivi di Apprendimento

- Comprendere il concetto di ricerca semantica vs ricerca testuale
- Preparare dati testuali per l'indicizzazione (chunking)
- Creare un Cortex Search Service
- Testare ricerche semantiche in linguaggio naturale
- Integrare Search Service con Snowflake Intelligence

### Istruzioni Passo-Passo

#### Step 3.1: Preparazione Dati - Tabella Chunks

Apri un worksheet SQL ed esegui lo script `modulo_2_cortex_search.sql`, oppure esegui i seguenti comandi passo per passo.

Per prima cosa, creiamo una tabella con un campo di testo ricco che concatena tutte le informazioni di ogni programma:

```sql
USE ROLE SYSADMIN;
USE DATABASE MEDIASET_LAB;
USE SCHEMA RAW;
USE WAREHOUSE MEDIASET_WH;

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
```

Verifica i chunks creati:

```sql
SELECT CONTENUTO_ID, TITOLO, LENGTH(CHUNK_TEXT) AS LUNGHEZZA_CHUNK
FROM MEDIASET_LAB.RAW.CONTENUTI_CHUNKS
ORDER BY CONTENUTO_ID;
```

#### Step 3.2: Creazione del Cortex Search Service

```sql
CREATE OR REPLACE CORTEX SEARCH SERVICE MEDIASET_LAB.RAW.RICERCA_CONTENUTI
  ON CHUNK_TEXT
  ATTRIBUTES TITOLO
  WAREHOUSE = MEDIASET_WH
  TARGET_LAG = '1 hour'
  COMMENT = 'Ricerca semantica sui contenuti e descrizioni dei programmi TV Mediaset'
AS (
    SELECT
        CHUNK_TEXT,
        TITOLO,
        CONTENUTO_ID,
        PROGRAMMA_ID
    FROM MEDIASET_LAB.RAW.CONTENUTI_CHUNKS
);
```

Verifica:

```sql
SHOW CORTEX SEARCH SERVICES IN SCHEMA MEDIASET_LAB.RAW;
```

#### Step 3.3: Test Ricerca Semantica

Usa `SEARCH_PREVIEW` per testare il servizio. La ricerca semantica comprende il significato, non solo le parole esatte:

```sql
-- Cerca programmi di satira
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
```

**Prova anche queste ricerche:**

```sql
-- Talent show musicali
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

-- Reality show
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

-- Programmi di informazione
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
```

> **Nota:** Osserva come la ricerca semantica restituisce risultati pertinenti anche quando le parole esatte non corrispondono. Questo è il potere degli embeddings!

#### Step 3.4: Integrazione con Snowflake Intelligence

Ora aggiungiamo il servizio di ricerca come tool aggiuntivo all'agente creato nel Modulo 2:

1. Vai su **AI & ML** > **Snowflake Intelligence**
2. Clicca su **MEDIASET_INTELLIGENCE_AGENT** per aprirlo
3. Clicca su **Edit** (icona matita) per modificare la configurazione
4. Nella sezione Tools, clicca **"+ Add Tool"**
5. Seleziona **"Search Service"**
6. Scegli il servizio: `MEDIASET_LAB.RAW.RICERCA_CONTENUTI`
7. Nel campo Description, inserisci: *"Cerca nelle descrizioni e schede dei programmi TV Mediaset per trovare informazioni su trama, cast, genere e dettagli produttivi"*
8. Clicca **Save** per salvare le modifiche

#### Step 3.5: Test Domande Combinate

Ora l'agente può rispondere usando sia i dati strutturati (Semantic View) sia la ricerca testuale (Search). Prova:

- *"Quali programmi comici abbiamo? Descrivili brevemente"*
- *"Cerca programmi adatti a un pubblico giovane e dimmi il loro share"*
- *"Trovami programmi con Gerry Scotti e mostrami i loro ascolti"*
- *"Quali programmi parlano di cucina? Quanto share fanno?"*

> **Nota:** L'agente decide automaticamente quale tool usare (o entrambi) in base alla domanda.

### Esercizio Pratico 3

Cerca almeno 3 programmi per tema (es. *"avventura"*, *"musica"*, *"investigazione"*) e confronta i risultati della ricerca semantica con una ricerca SQL tradizionale usando `LIKE`.

---

## Modulo 4: AI SQL in Notebook (30 min)

> Le **funzioni AI SQL** di Snowflake (prefisso `SNOWFLAKE.CORTEX.*`) permettono di applicare modelli di intelligenza artificiale direttamente nelle query SQL, senza spostare i dati fuori da Snowflake. Sono disponibili funzioni per sentiment analysis, riassunto, traduzione, classificazione, estrazione e generazione di testo.

### Obiettivi di Apprendimento

- Importare e utilizzare un notebook Snowflake
- Applicare Sentiment Analysis ai feedback social
- Riassumere automaticamente descrizioni di programmi
- Tradurre contenuti in più lingue
- Generare analisi con prompt LLM personalizzati
- Classificare ed estrarre informazioni da testi

### Istruzioni Passo-Passo

#### Step 4.1: Importare il Notebook AI SQL

1. Nel menu laterale, vai su **Projects** > **Workspaces**
2. Clicca su **"..."** (menu) > **Import** > **Import .ipynb file**
3. Seleziona il file `Mediaset_AISQL_Lab.ipynb` fornito
4. Il notebook si apre automaticamente con tutte le celle pronte

> **Alternativa:** Se preferisci lavorare in un worksheet SQL, usa lo script `modulo_3_aisql.sql` che contiene gli stessi esempi.

#### Step 4.2: Setup Iniziale

Esegui la prima cella del notebook per impostare il contesto:

```sql
USE DATABASE MEDIASET_LAB;
USE SCHEMA RAW;
USE WAREHOUSE MEDIASET_WH;
```

#### Step 4.3: Sentiment Analysis

La funzione `SNOWFLAKE.CORTEX.SENTIMENT()` analizza il tono emotivo di un testo e restituisce un punteggio da **-1** (molto negativo) a **+1** (molto positivo).

Esegui la cella che analizza il sentiment dei feedback social:

```sql
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
```

> **Osserva:** I feedback negativi (es. critiche ai programmi) hanno score vicino a -1, quelli entusiasti vicino a +1.

Esegui anche la cella con le statistiche aggregate per piattaforma per vedere quale social network ha il sentiment più positivo.

#### Step 4.4: Summarize

La funzione `SNOWFLAKE.CORTEX.SUMMARIZE()` genera un riassunto conciso di testi lunghi:

```sql
SELECT
    TITOLO,
    SNOWFLAKE.CORTEX.SUMMARIZE(DESCRIZIONE_COMPLETA) AS RIASSUNTO
FROM MEDIASET_LAB.RAW.CONTENUTI_DESCRIZIONI
WHERE LENGTH(DESCRIZIONE_COMPLETA) > 100
LIMIT 5;
```

#### Step 4.5: Translate

La funzione `SNOWFLAKE.CORTEX.TRANSLATE()` traduce testo tra lingue diverse:

```sql
SELECT
    FEEDBACK_ID,
    TESTO_FEEDBACK AS ORIGINALE_IT,
    SNOWFLAKE.CORTEX.TRANSLATE(TESTO_FEEDBACK, 'it', 'en') AS TRADUZIONE_EN,
    SNOWFLAKE.CORTEX.TRANSLATE(TESTO_FEEDBACK, 'it', 'es') AS TRADUZIONE_ES
FROM MEDIASET_LAB.RAW.FEEDBACK_SOCIAL
LIMIT 5;
```

#### Step 4.6: AI Complete

La funzione `SNOWFLAKE.CORTEX.COMPLETE()` invia prompt personalizzati a un LLM. Esegui la cella che analizza i feedback con suggerimenti editoriali:

```sql
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
```

Esegui anche la cella che genera un mini-report automatico dagli ascolti aggregati.

#### Step 4.7: Classify Text

La funzione `SNOWFLAKE.CORTEX.CLASSIFY_TEXT()` classifica il testo in categorie predefinite:

```sql
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
```

#### Step 4.8: Extract Answer

La funzione `SNOWFLAKE.CORTEX.EXTRACT_ANSWER()` estrae risposte specifiche da un testo:

```sql
SELECT
    FEEDBACK_ID,
    TESTO_FEEDBACK,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        TESTO_FEEDBACK,
        'Quale programma televisivo viene menzionato o commentato?'
    ) AS PROGRAMMA_MENZIONATO,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        TESTO_FEEDBACK,
        'Qual è l''aspetto positivo o negativo principale evidenziato?'
    ) AS ASPETTO_PRINCIPALE
FROM MEDIASET_LAB.RAW.FEEDBACK_SOCIAL
LIMIT 10;
```

### Best Practice

- **Governance**: tutte le funzioni AI SQL rispettano RBAC. Se un utente non ha accesso a una tabella, le funzioni AI non restituiscono risultati
- **Performance**: le funzioni AI aggiungono latenza alle query. Usa `LIMIT` durante lo sviluppo
- **Costi**: ogni chiamata AI consuma crediti Cortex. Evita di eseguire funzioni AI su tabelle molto grandi senza filtri
- **Modelli**: `SNOWFLAKE.CORTEX.COMPLETE()` supporta diversi modelli (mistral-large2, llama3.1-70b, etc.). Scegli in base al trade-off qualità/costo

### Esercizio Pratico 4

Scrivi una query che combini Sentiment Analysis e Classify Text per creare un report completo dei feedback: per ogni feedback mostra il testo breve, il sentiment score, la classificazione e la confidence.

---

## Pulizia Risorse (Opzionale)

Al termine del workshop, se desideri rimuovere gli oggetti creati:

```sql
USE ROLE SYSADMIN;

-- Rimuovi Cortex Search Service
DROP CORTEX SEARCH SERVICE IF EXISTS MEDIASET_LAB.RAW.RICERCA_CONTENUTI;

-- Rimuovi tabella chunks
DROP TABLE IF EXISTS MEDIASET_LAB.RAW.CONTENUTI_CHUNKS;

-- Rimuovi tabelle analytics create dai notebook
DROP TABLE IF EXISTS MEDIASET_LAB.ANALYTICS.TOP_PROGRAMMI_SHARE;
DROP TABLE IF EXISTS MEDIASET_LAB.ANALYTICS.ANALISI_FASCE_ORARIE;

-- Rimuovi Semantic View e Agent (via SQL o via UI)
DROP SEMANTIC VIEW IF EXISTS MEDIASET_LAB.ANALYTICS.MEDIASET_TV_INTELLIGENCE;
DROP CORTEX AGENT IF EXISTS MEDIASET_LAB.ANALYTICS.MEDIASET_INTELLIGENCE_AGENT;
```

> **Nota:** Il database `MEDIASET_LAB` e i dati del Workshop 1 rimangono intatti. Vengono rimossi solo gli oggetti creati in questo workshop.

---

## Risorse Utili

| Risorsa | Descrizione |
|---------|-------------|
| **Semantic Views** | Documentazione Snowflake sulle Semantic View |
| **Cortex Analyst** | Guida a Cortex Analyst e modello semantico |
| **Cortex Search** | Documentazione Cortex Search Service |
| **AI SQL Functions** | Riferimento funzioni SNOWFLAKE.CORTEX.* |
| **Snowpark Python** | Guida Snowpark DataFrame API |
| **Snowflake Notebooks** | Documentazione notebook Snowflake |
