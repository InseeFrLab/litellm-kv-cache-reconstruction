# Pipeline Prefix Cache V1 — Guide de reproduction

**Date**: 2026-09-17  
**Objectif**: Reconstruire les sessions multi-turnes, assigner des block IDs, et comparer le hit rate synthétique avec Prometheus.

---

## 1. Prérequis

- Python 3.12+
- Modules: `matplotlib` (optionnel, pour le graphique)

## 2. Organisation des fichiers

```
v1/
├── litellm_logs_20260916_1d_hosted_vllm-Qwen36-35B.csv  ← données brutes LiteLLM
├── prometheus_prefix_cache.csv                          ← données Prometheus (optionnel)
├── Qwen36-35B_20260916.csv                             ← nettoyage (étape 1)
├── Qwen36-35B_20260916_grouped.csv                     ← groupage (étape 2)
├── Qwen36-35B_20260916_sessions.csv                    ← sessions (étape 3)
├── Qwen36-35B_20260916_09h_blocks.csv                  ← blocks (étape 5 + 6)
├── Qwen36-35B_20260916_09h_trace.jsonl                 ← trace GuideLLM (étape 7)
├── Qwen36-35B_20260916_09h_trace_augmented.jsonl       ← trace augmentée (étape 8, opt-in)
├── Qwen36-35B_20260916_09h_trace_augmented.audit.json  ← audit d'augmentation (étape 8)
├── timeseries_comparison.png                           ← graphique (étape 4)
├── pipeline.py                                         ← script principal (8 étapes)
├── augment_trace.py                                    ← augmentation de trace (étape 8)
└── README.md                                           ← ce fichier
```

## 3. Commandes

Le pipeline a **2 scripts CLI** : `pipeline.py` (le pipeline complet) et `augment_trace.py`
(module standalone réutilisable pour l'étape 8).

### 3.1 `pipeline.py` — pipeline complet

```bash
python3 v1/pipeline.py [options]
```

#### Arguments

| Argument | Type | Reqs | Défaut | Rôle |
|----------|------|------|--------|------|
| `--csv PATH` | str | ⬜* | — | Chemin vers le **CSV LiteLLM brut** (mode fichier) |
| `--litellm-url URL` | str | ⬜* | — | URL de base LiteLLM (mode API, ex: `http://litellm:4000`) — *mutuellement exclusif avec `--csv`* |
| `--litellm-api-key KEY` | str | si `--litellm-url` | — | Clé API LiteLLM |
| `--start-date DATE` | str | si `--litellm-url` | — | Date de début `YYYY-MM-DD` |
| `--end-date DATE` | str | si `--litellm-url` | — | Date de fin `YYYY-MM-DD` |
| `--prometheus PATH` | str | | (off) | Chemin vers le CSV Prometheus (active l'étape 4) |
| `--output-dir DIR` | str | | répertoire courant | Répertoire de sortie (les fichiers intermédiaires y sont écrits) |
| `--hour HOUR` | int | | 9 | Heure pour la génération des block IDs (fenêtre `[HOUR:00, HOUR+1:00)`) |
| `--augment` | bool | | off | **Active les étapes 7 + 8** (opt-in) |
| `--delta-t T` | float | avec `--augment` | 5.0 | Décalage rigide du clone en secondes |
 | `--dup-n N` | int | | 1 | **Factor de concurrence exact** : `N` clones par session multi-turn → nombre de sessions ×(1+N), nombre de reqs ×(1+N). 0 = pas d'augmentation, 1 = ×2 (défaut), 2 = ×3, … |
 | `--seed N` | int | | 42 | Graine (rétro-compat d'API ; l'augmentation est déterministe) |

\* **`--csv` XOR `--litellm-url`** : on alimente soit un fichier local, soit une URL API. Les 4 arguments `--litellm-*` sont requis **uniquement** quand `--litellm-url` est utilisé.

> ⚠️ Le flag existe dans le code mais n'est pas dans la CLI publique : `--augment` (et ses 3 arguments dérivés) est la **seule** façon d'activer les étapes 7 (trace) et 8 (augmentation). Sans `--augment`, le pipeline s'arrête à l'étape 6.

#### Exemples

```bash
# Mode fichier (le plus courant)
python3 v1/pipeline.py --csv ../data_raw/litellm_logs_20260916_1d_hosted_vllm.csv \
                       --output-dir output

# Mode API
python3 v1/pipeline.py --litellm-url http://litellm:4000 \
                       --litellm-api-key $LITELLM_KEY \
                       --start-date 2026-09-16 --end-date 2026-09-16 \
                       --output-dir output

# Ajuster la fenêtre horaire (10h au lieu de 9h)
python3 v1/pipeline.py --csv ../data_raw/litellm_logs_20260916_1d_hosted_vllm.csv \
                       --hour 10 --output-dir output

# Avec augmentation de la trace (concurrence ×2)
python3 v1/pipeline.py --csv ../data_raw/litellm_logs_20260916_1d_hosted_vllm.csv \
                       --output-dir output --augment \
                       --delta-t 5 --dup-n 1 --seed 42

# Avec augmentation de la trace (concurrence ×5)
python3 v1/pipeline.py --csv ../data_raw/litellm_logs_20260916_1d_hosted_vllm.csv \
                       --output-dir output --augment \
                       --delta-t 5 --dup-n 4 --seed 42

# Avec comparaison Prometheus
python3 v1/pipeline.py --csv ../data_raw/litellm_logs_20260916_1d_hosted_vllm.csv \
                       --prometheus ../data_raw/prometheus_prefix_cache.csv \
                       --output-dir output
```

Le script vérifie si les sorties intermédiaires existent déjà et skip les étapes déjà exécutées.

### 3.2 `augment_trace.py` — module standalone (étape 8 seule)

```bash
python3 v1/augment_trace.py [options]
```

Utilisable **sans le pipeline** (à partir d'un `blocks.csv` déjà produit) : utile pour itérer
sur l'augmentation sans rejouer les 6 étapes de reconstruction.

#### Arguments

| Argument | Type | Reqs | Défaut | Rôle |
|----------|------|------|--------|------|
| `--blocks PATH` | str | ✅ | — | Chemin du `blocks.csv` **en entrée** (produit par les étapes 5-6) |
| `--output PATH` | str | ✅ | — | Chemin de la **trace augmentée en sortie** (JSONL). Un audit `<output>.audit.json` est aussi écrit. |
| `--delta-t T` | float | | 5.0 | Décalage rigide du clone en secondes |
| `--dup-n N` | int | | 1 | **Factor de concurrence exact** : `N` clones par session multi-turn → nombre de sessions ×(1+N), nombre de reqs ×(1+N). 0 = pas d'augmentation, 1 = ×2 (défaut), 2 = ×3, … |
| `--seed N` | int | | 42 | Graine (rétro-compat d'API ; l'augmentation est déterministe) |
| `--block-size N` | int | | 256 | Taille d'un bloc en tokens (doit correspondre à celle utilisée à l'étape 5) |

#### Exemples

```bash
# Concurrence ×2 (le cas typique), décalage 5 s
python3 v1/augment_trace.py --blocks output/blocks.csv \
                            --output output/trace_augmented.jsonl \
                            --delta-t 5 --dup-n 1 --seed 42

# Concurrence ×5, décalage 30 s
python3 v1/augment_trace.py --blocks output/blocks.csv \
                            --output /tmp/aug5.jsonl \
                            --delta-t 30 --dup-n 4 --seed 123

# Pas d'augmentation (trace = origine seule)
python3 v1/augment_trace.py --blocks output/blocks.csv \
                            --output /tmp/aug1.jsonl \
                            --delta-t 5 --dup-n 0 --seed 42
```

**Invariants vérifiés** : cf. §4 Étape 8 (invariants de non-biais).

---

## 4. Pipeline complet (8 étapes, dont 2 opt-in)

> Les étapes 7 et 8 sont **opt-in** : elles ne s'exécutent que si le flag `--augment`
> est passé (cf. §3 Commandes). Sans ce flag, le pipeline s'arrête à l'étape 6.

### Étape 1 — Nettoyage des logs

Entrée: `litellm_logs_YYYYMMDD_1d_hosted_vllm-*.csv` (brut, ~237 MB)

Actions:
- Supprimer la colonne `metadata` JSON
- Extraire `user_api_key_alias` de metadata → nouvelle colonne
- Extraire `client_tool` du premier `User-Agent` dans `tags`
- Sortie: 9 colonnes, 6 MB

### Étape 2 — Groupage et filtrage

Actions:
- Regrouper par `(api_key_alias, client_tool)`
- **Filtrer**: groupes avec `max(prompt_tokens) < 5000` supprimés (on ne peut pas détecter de sessions multi-turn dessus — 50 groupes, ~792 reqs, 2.6% du trafic)
- Trier chaque groupe chronologiquement
- Sortie: groupés + triés

### Étape 3 — Reconstruction des sessions

**Multi-Session Tracker** — approche heuristique, déterministe.

Pour chaque groupe, maintient un ensemble de sessions actives. Pour chaque requête:

1. Fermer les sessions pleines (`max_session_size`)
2. Fermer les sessions inactives (`max_session_gap`)
3. Scorer la requête contre chaque session active
4. Affecter à la meilleure si `score >= threshold`, sinon nouvelle session

**3 signaux**:
- `time_score`: `exp(-dt / tau)` — décroissance temporelle
- `growth_score`: asymétrique — tolère hausse, pénalise chute > 30%
- `output_score`: bonus si `growth ≈ completion_tokens` (réinjection contexte)

**Score final**: `w_time * time + w_growth * growth + w_output * output`

### Étape 4 — Comparaison Prometheus

Pour chaque requête dans les sessions reconstruites, estimer le shared prefix:
- Si `req` est après une autre req dans la même session: `shared = min(P_i, P_{i-1} + C_{i-1})`
- Sinon (première req): `shared = 0`

Hit rate par fenêtre de 30s sur une fenêtre de 5 min:
```
hit_rate = sum(shared) / sum(prompt_tokens)
```

Comparer avec les données Prometheus (moyenne 3 pods, même fenêtre).

### Étape 5 — Génération des block IDs

**Objectif**: Assigner des block IDs (entiers uniques) pour chaque session multi-turn.

Règles:
- Block size = 256 tokens
- Les block IDs sont **uniques entre sessions** — un block ID désigne toujours les mêmes 256 tokens
- Pour une session, les blocks s'accumulent au fil des turns

Algorithme par session:
```
GLOBAL counter = 0  # compteur unique pour TOUTES les sessions du dataset

Pour chaque session multi-turn (ordonnée chronologiquement):
  accumulated_blocks = []  # mémoire du session
  
  Pour chaque requête R:
    P_i = prompt_tokens de R
    C_{i-1} = completion_tokens de la précédente
    
    if c'est la 1ère req:
      shared_tokens = 0
    else:
      shared_tokens = min(P_i, P_{i-1} + C_{i-1})
      
    needed_blocks = ceil(P_i / 256)
    shared_blocks = floor(shared_tokens / 256)
    
    # FIX BUG 1 — Gap ceil vs floor:
    # shared_blocks est calculé avec floor((P_prev + C_prev) / 256)
    # mais accumulated_blocks contient ceil(P_prev / 256) blocs.
    # Quand C_prev > 0, floor((P+C)/256) > ceil(P/256) crée un décalage.
    # Ex: P=94704 -> ceil(94704/256)=370, C=793 -> floor((94704+793)/256)=372.
    # Gap de 2 blocs comblé par un while loop (placeholder block IDs).
    while len(accumulated_blocks) < shared_blocks:
        accumulated_blocks.append(global_counter)
        global_counter += 1
    
    # shared_blocks ne peut pas dépasser ce qu'on a (sécurité)
    shared_actual = min(shared_blocks, len(accumulated_blocks))
    assigned_blocks = accumulated_blocks[:shared_actual]
    new_block_count = needed_blocks - shared_actual
    new_blocks = [GLOBAL counter for _ in range(new_block_count)]
    GLOBAL counter += new_block_count
    
    assigned_blocks.extend(new_blocks)
    
    # FIX BUG 2 — extend au lieu de replace:
    # Ancien code: accumulated_blocks = assigned_blocks
    # Problème: quand P_i < P_{i-1}, needed_blocks < len(accumulated)
    # => assigned_blocks est plus court, écrase accumulated_blocks.
    # Les requêtes suivantes échouent silencieusement (slicing Python tronque).
    # Fix: accumulated_blocks.extend(new_blocks) — conserve tous les blocs existants.
    accumulated_blocks.extend(new_blocks)
```

Sortie: un fichier CSV avec toutes les requêtes + colonne `block_ids` (format JSON d'entiers)
- Multi-turn: liste d'entiers, ex: `[1, 2, 3, 5, 6]`
- Single-turn: chaîne vide `""`
- **Règle**: seuls les requests de sessions multi-turns (taille > 1) ont des block_ids. First turn d'une session multi-turn a aussi des block_ids car il fait partie de la session.

Filtrage temporel: uniquement l'heure sélectionnée (ex: 09:00-10:00, qui correspond à 11:00-12:00 dans Prometheus +2h).

**Modélisation**: Le pipeline modélise l'évolution du prompt de manière linéaire (pas de branches).
Un prompt LLM est toujours un préfixe continu depuis le début: `[block0, block1, ..., blockN]`.
L'arbre mooncake (relations `previous_id`) est géré implicitement par l'ordre séquentiel des IDs.

**Bugs fixes (v1.0 -> v1.1)**:

- **BUG 1 — Gap ceil vs floor**: `needed_blocks = ceil(P/256)` vs `shared_blocks = floor((P_prev+C_prev)/256)`. Quand `C_prev > 0`, `floor((P+C)/256)` peut dépasser `ceil(P/256)` (ex: P=94704→370 blocs, C=793→floor(95497/256)=372). Le `while` comble ce gap avec des blocs placeholders représentant les tokens de complétion en cache KV.

- **BUG 2 — `extend` au lieu de `=`**: Ancien code `accumulated_blocks = assigned` écrasait la liste quand un prompt était plus court que le précédent. Ex: accumulated=[0..369] (370 blocs), nouvelle req P=80616→315 blocs→assigned=[0..314, 370]. Le `extend([370])` préserve les 371 blocs (le prompt n'utilise que 315, mais les blocs 315..370 peuvent être partagés par les futures requêtes). Le bug causait des échecs silencieux sur les requêtes suivantes.

**Notes sur les bugs fixes (v1.1)**:
- **Bugs fixes (v1.0 -> v1.1)**:
  - **BUG 1 (gap ceil/floor)**: le `needed_blocks` utilise `ceil(P/256)` mais `shared_blocks` utilise `floor(shared/256)`. La différence entre `ceil` et `floor` crée des gaps de 1-2 blocs. Ces blocs manquant correspondent aux tokens de complétion (`C_prev`) qui sont dans le cache KV mais pas dans la liste `accumulated_blocks`.
  - **BUG 2 (replace vs extend)**: l'ancien code `accumulated_blocks = assigned` écrasait la liste quand un prompt était plus court que le précédent. Ex: `accumulated = [0..369]` (370 blocs), nouvelle req `P=80616` → `assigned = [0..314]` (315 blocs). Le `extend([new])` préserve les 370 blocs, le `=` les réduit à 315. Les requêtes suivantes avec `shared_blocks=372` échouaient silencieusement.

- **Modélisation**:

### Étape 7 — Génération de la trace Mooncake/GuideLLM

**Objectif**: convertir le fichier `blocks.csv` en une trace JSONL lisible par le benchmarker
GuideLLM (mooncake).

Format de sortie (une ligne JSON par requête) :
```json
{"timestamp": <float>, "input_length": <int>, "output_length": <int>, "hash_ids": [<int>, ...]}
```
- `timestamp` : secondes depuis le début de l'heure (ex: 09:00:00.000)
- `input_length` : `prompt_tokens`
- `output_length` : `completion_tokens`
- `hash_ids` : `block_ids` existants ; pour les single-turn / groupes filtrés, des IDs frais uniques
  alloués à partir de `max(block_ids) + 1`.

La trace est triée par `timestamp` avant écriture.

### Étape 8 — Augmentation de la trace (opt-in)

 **Objectif** : générer une **trace augmentée** dans laquelle chaque session multi-turn est
 dupliquée `N` fois (`--dup-n N`), pour alimenter un bench de **routing par rapport au cache**
 avec un trafic plus dense de sessions concurrentes — **sans biaiser les résultats**
 (pas de hit croisé artificiel). Le **factor de concurrence exact** obtenu est
 `1 + N` (le nombre de sessions ET de requêtes est multiplié par `1 + N`).

| `--dup-n` | Sessions | Reqs | Concurrency |
|-----------|----------|------|-------------|
| 0 | ×1 | ×1 | ×1 (trace = origine seule) |
| 1 | ×2 | ×2 | ×2 |
| 2 | ×3 | ×3 | ×3 |
| N | 1 + N | 1 + N | 1 + N |

L'augmentation est portée par `v1/augment_trace.py` (module standalone, stdlib uniquement).
Elle lit `blocks.csv` et écrit **deux** nouveaux fichiers (jamais `trace.jsonl`) :
- `<hour>_trace_augmented.jsonl` — la trace augmentée (même format que l'étape 7)
- `<hour>_trace_augmented.audit.json` — compteurs de validation (sans PII)

**Algorithme** :
1. **Grouper** `blocks.csv` par `(group_key, reconstructed_session_id)`, trier par `startTime`.
   Les sessions multi-turn (>1 ligne) sont les seules candidates au clonage.
2. **Reconstituer la partie « origine »** : exactement la même logique d'IDs que l'étape 7
   (multi-turn = `block_ids` ; single-turn = IDs frais à partir de `max_id + 1`).
 3. **Cloner** : pour **chaque** session multi-turn, produire `N` clones (`--dup-n N`,
    déterminisme 100 % — le `--seed` n'a plus d'effet, il est conservé pour compat d'API) :
    - chaque **clone** reçoit sa **propre bijection** `block_id_orig → block_id_clone`
      (première apparition dans l'ordre chronologique des turns) — les clones d'une même
      session sont disjoints entre eux (sinon hit croisé artificiel entre les copies) ;
    - le `clone_counter` démarre à `max_id + 1` et s'incrémente de façon continue
      → **disjoint total** entre tous les clones et l'origine.
    - chaque turn i du clone est décalé rigidement : `clone_i.ts = orig_i.ts + delta_t`.
4. **Fusionner** origine + clones, trier par `timestamp`, écrire la trace + l'audit.

**Invariants de non-biais** (vérifiés automatiquement, reportés dans l'audit) :
| Invariant | Signification |
|-----------|---------------|
| `min_clone_id > max_original_id` | aucun ID du clone est partagé avec l'origine |
| `clone_id_overlap_original == 0` | aucun ID du clone est partagé avec l'origine (le clone lui-même est cohérent, c'est la bijection) |
| `clone_i.ts − orig_i.ts == delta_t` | décalage rigide (pas de jitter) |
| `gap_{i+1} == gap_i` (clone vs original) | écarts internes entre turns préservés |
| `len(clone.hash_ids) == len(orig.hash_ids)` | même nombre de blocs par turn |
| `newblocks(clone_i) == newblocks(orig_i)` | même nombre de **nouveaux** blocs par turn |
| `output_sha256` | empreinte de la trace émise (traçabilité) |

**Pourquoi c'est non biaisant** : le clone n'a **aucun** bloc partagé avec l'original ni avec
les autres sessions — son turn 1 est *cold*, ses following turns sont *hits* uniquement dans
la mémoire de son propre clone. C'est exactement la sémantique d'un **utilisateur concurrent
indépendant**, qui est l'objectif du bench de routing.

 **Usage** :
 ```bash
 # Standalone
 python3 v1/augment_trace.py \
     --blocks output/blocks.csv \
     --output output/trace_augmented.jsonl \
     --delta-t 5 --dup-n 1 --seed 42
 
 # Via pipeline (étape 8 opt-in)
 python3 v1/pipeline.py \
     --csv ... --output-dir output \
     --augment --delta-t 5 --dup-n 1 --seed 42
 ```
 
 **Flags** : `--augment` (bool), `--delta-t <float>` (défaut 5.0 s), `--dup-n <int>`
 (défaut 1), `--seed <int>` (défaut 42, rétro-compat), `--block-size <int>` (défaut 256).

**Note sur l'interprétation des métriques** : l'augmentation augmente mécaniquement la part
de trafic « chaud » (sessions multi-turn à fort hit intra-session). Le **hit-rate brut** de
`trace_augmented.jsonl` est donc supérieur à celui de `trace.jsonl` — c'est **l'intention**
(stress cache), mais il ne faut pas comparer les deux traces de manière naïve : c'est le
**métrique de routing** (par ex. taux de hit inter-pods, répartition des requêtes) qu'on veut
observer sur `trace_augmented.jsonl`.

### Étape 6 — Réintégration des groupes filtrés

Les 50 groupes filtrés (<5k tokens) de l'étape 2 sont réintégrés :
- Charger le fichier blocks généré à l'étape 5
- Charger le fichier groupé
- Trouver les requêtes de groupes manquants (qui n'étaient pas dans `grouped.csv` car supprimés)
- Les rechercher dans les données nettoyées brutes (`Qwen36-35B_20260916.csv`) pour l'heure sélectionnée
- Ajouter comme single-turn avec `block_ids=""`

Sortie: le fichier final `Qwen36-35B_20260916_09h_blocks.csv` contient TOUTES les requêtes de l'heure.

---

## 5. Hyperparamètres finaux

| Paramètre | Valeur | Description |
|-----------|--------|-------------|
| `tau` | 300s | constante de temps signal temporel |
| `max_session_gap` | 1800s (30min) | session inactive fermée |
| `max_session_size` | 250 | max reqs par session |
| `assignment_threshold` | 1.5 | seuil min pour rejoindre session |
| `w_time` | 1.5 | poids signal temporel |
| `w_growth` | 1.0 | poids signal taille prompt |
| `w_output` | 1.0 | poids signal output |
| `growth_scale` | 2000 | échelle pénalité croissance |
| `drop_threshold` | 0.7 | ratio min avant pénalité forte |
| `max_growth_ratio` | 5.0 | ratio max avant pénalité extrême |
| `output_match_scale` | 200 | échelle matching output |
| `prompt_min_tokens` | 5000 | filtre: groupes < 5k tokens |
| `window_size_sec` | 30 | fenêtre calcul hit rate |
| `cache_window_min` | 5 | fenêtre cache |
| `prompt_tps` | 40000 | 40k tokens/s throughput prefill |
| `block_size` | 256 | taille d'un bloc de cache |

Tous ces paramètres sont définis dans la classe `TrackerConfig` dans `pipeline.py`.

## 5bis. Augmentation — synthèse

Les 5 flags d'augmentation (`--augment`, `--delta-t`, `--dup-n`, `--seed`, `--block-size`)
sont documentés dans le tableau de la **§3.1** (`pipeline.py` — pipeline complet) et, pour
l'usage standalone, dans la **§3.2** (`augment_trace.py`).

L'augmentation est **déterministe** (pas de random : chaque session reçoit exactement
`N = dup_n` clones) : deux runs avec le même `--dup-n` produisent des
`trace_augmented.jsonl` byte-identiques (`cmp` exit 0).

---

## 6. Résultats attendus

- **Requêtes**: ~29k après filtrage
- **Sessions**: ~950 (max 250 reqs/session)
- **Médiane session**: ~5 turns
- **Hit rate synthétique**: ~93% (estimation intra-session)
- **Hit rate Prometheus**: ~77% (moyenne 3 pods, round-robin)
- **Écart**: ~16% (attribuable à round-robin + estimation optimiste)
- **Block IDs**: ~28k blocs uniques générés

## 7. Champs `assignment_type`

Les valeurs possibles dans la colonne `assignment_type` sont :

| Valeur | Description |
|--------|-------------|
| `new_session` | Premier turn d'une session détectée comme multi-turn |
| `existing_session` | Turn suivant dans une session existante |
| `filtered_group` | Requête d'un groupe filtré (<5k tokens), réinjectée à l'étape 6 |
| `grouped_data_missing` | Requête d'un groupe présent dans `grouped.csv` mais manquant dans le fichier blocks |
| (vide) | Ancienne valeur (avant correction) — ne devrait plus apparaître |
