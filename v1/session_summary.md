# Reconstruction Prefix Cache — V1

**Date**: 2026-09-17
**Source**: `litellm_logs_20260916_1d_hosted_vllm-Qwen36-35B.csv` (1 jour, 30 114 requêtes)

---

## 1. Données sources

| Fichier | Lignes | Taille | Description |
|---------|--------|--------|-------------|
| `litellm_logs_20260916_1d_hosted_vllm-Qwen36-35B.csv` | 30 114 | 237 MB | Logs bruts LiteLLM |
| `Qwen36-35B_20260916.csv` | 30 114 | 6 MB | Nettoyé, colonnes extraites |
| `Qwen36-35B_20260916_grouped.csv` | 29 322 | 9.3 MB | Groupé, filtré >5k tokens |
| `Qwen36-35B_20260916_sessions.csv` | 29 322 | 10.7 MB | Sessions reconstruites |

### Colonnes finales

| Colonne | Type |
|---------|------|
| `startTime` | timestamp |
| `Status` | success/error |
| `Duration (s)` | float |
| `model` | string |
| `prompt_tokens` | int |
| `completion_tokens` | int |
| `completionStartTime` | timestamp |
| `user_api_key_alias` | string (99 uniques) |
| `client_tool` | string (24 uniques) |
| `reconstructed_session_id` | int |
| `assignment_score` | float |
| `time_score` | float |
| `growth_score` | float |
| `output_score` | float |
| `second_best_score` | float |
| `score_margin` | float |
| `assignment_type` | existing/new |

---

## 2. Pré-traitement

### 2.1 Extraction metadata

Le CSV brut contient une colonne `metadata` JSON avec 37 clés. Seules 2 ont été extraites :

- `user_api_key_alias` → colonne `user_api_key_alias` (99 uniques)
- `litellm_overhead_time_ms` → **supprimé** (non utilisé)

### 2.2 Extraction client_tool

La colonne `tags` contient des `User-Agent`. On prend le premier et extrait juste le nom de l'outil :

| Outil | Requêtes |
|-------|----------|
| OpenAI | 9 831 |
| opencode | 5 945 |
| Python-urllib | 4 989 |
| Kilo-Code | 3 961 |
| ... | ... (20 autres) |

### 2.3 Filtrage

- **Groupes (api_key, client_tool)**: 189 groupes
- **Filtrage**: groupes avec `max(prompt_tokens) < 5000` supprimés → 50 groupes, 792 reqs retirés
- **Résultat**: 29 322 requêtes dans 139 groupes
- **Tri**: chaque groupe trié chronologiquement par `startTime`

---

## 3. Détection de sessions multi-turnes

### 3.1 Méthodologie

**Multi-Session Tracker** — approche heuristique, déterministe, sans ML.

Pour chaque groupe, maintient un ensemble de sessions actives. Pour chaque requête R :

1. Fermer les sessions pleines (> `max_session_size`)
2. Fermer les sessions inactives (> `max_session_gap`)
3. Scorer R contre chaque session active
4. Affecter à la meilleure si `score >= threshold`, sinon créer nouvelle session

### 3.2 Signaux de score

**Signal 1 — Temporel** (`time_score`): décroissance exponentielle
```
score = exp(-dt / tau)
```

**Signal 2 — Continuité de taille prompt** (`growth_score`): asymétrique
- Stable ou légère croissance (ratio 0.7-3x): score haut
- Baisse > 30%: score très bas
- Croissance > 5x: pénalité forte

**Signal 3 — Relation completion→prompt** (`output_score`): bonus si `prompt_new ≈ prompt_prev + completion_prev`
- Ratio growth/output entre 0.5-1.5: bon match (score 0.7-1.0)
- Ratio 1.5-3.0: outil calls (score moyen)
- Ratio < 0.5 ou négatif: score bas

**Score final**: `w_time * time + w_growth * growth + w_output * output`

### 3.3 Hyperparamètres finaux

| Paramètre | Valeur | Description |
|-----------|--------|-------------|
| `tau` | 300s | constante temps signal temporel |
| `max_session_gap` | 1800s (30min) | session inactive fermée |
| `max_session_size` | 250 | max reqs par session |
| `assignment_threshold` | 1.5 | seuil min pour rejoindre session |
| `w_time` | 1.5 | poids signal temporel |
| `w_growth` | 1.0 | poids signal taille prompt |
| `w_output` | 1.0 | poids signal output |
| `growth_scale` | 2000 | échelle pénalité croissance |
| `drop_threshold` | 0.7 | ratio min avant pénalité |
| `max_growth_ratio` | 5.0 | ratio max avant pénalité extrême |
| `output_match_scale` | 200 | échelle matching output |

### 3.4 Sorties par requête

Chaque requête a un score d'affectation détaillé :
- `assignment_score`: score final
- `time_score`, `growth_score`, `output_score`: scores individuels
- `second_best_score`: score du 2ème meilleur candidat
- `score_margin`: différence best vs second best
- `assignment_type`: `existing_session` ou `new_session`

---

## 4. Résultats finaux

### 4.1 Sessions reconstruites

| Metric | Valeur |
|--------|--------|
| Sessions multitournes | 950 |
| Sessions mono-turn | 258 |
| Total requêtes analysées | 29 322 |
| Max session size | 250 |
| Médiane sessions | 5 turns |
| Moyenne sessions | 24 turns |

### 4.2 Distribution (bins de 25)

| Bin | Count | % |
|-----|-------|------|
| 1-25 | 994 | 81.4% |
| 26-50 | 91 | 7.5% |
| 51-75 | 41 | 3.4% |
| 76-100 | 18 | 1.5% |
| 101-125 | 10 | 0.8% |
| 126-150 | 8 | 0.7% |
| 151-175 | 3 | 0.2% |
| 176-200 | 4 | 0.3% |
| 201-225 | 3 | 0.2% |
| 226-250 | 49 | 4.0% |

### 4.3 Répartition API calls

| Catégorie | Sessions | API calls | % calls |
|-----------|----------|-----------|---------|
| Mono-turn (1) | 258 | 258 | 0.9% |
| Court (2-5) | 391 | 1 163 | 4.0% |
| Moyen (6-10) | 182 | 1 392 | 4.7% |
| Long (11-30) | 185 | 3 323 | 11.3% |
| Très long (31-100) | 128 | 6 897 | 23.5% |
| Extrême (101-250) | 77 | 16 289 | 55.6% |

### 4.4 Hit rate estimé (tokens)

**Méthode**: `hit_ratio = 1 - (ttft_observed / (prompt_tokens / 40000))`

| Position dans session | Hit rate |
|----------------------|----------|
| Turn 0 (1er) | 15.2% |
| Turn 1-2 | 34.4% |
| Turn 3-5 | 43.4% |
| Turn 6-10 | 51.4% |
| Turn 11-30 | 59.6% |
| Turn 31+ | 68.7% |

**Hit rate global**: **60.4%** (target Prometheus: ~70%)

- Total prompt tokens: ~1.02 milliards
- Tokens estimés en cache: ~618 millions
- Requêtes avec cache >0: 38.9%
- Médiane hit ratio: 0% (beaucoup de mono-turns sans cache)
- P95 hit ratio: 84.7%

### 4.5 Affectations ambiguës

- 1 042 requêtes (3.6%) avec `score_margin < 0.2` ou `score < 0.5`
- Majoritairement des `new_session` avec scores nuls (première req d'une session)

---

## 5. Fichiers

| Fichier | Rôle |
|---------|------|
| `multi_session_tracker.py` | Script principal — tracker + diagnostics |
| `Qwen36-35B_20260916.csv` | Données nettoyées (9 colonnes) |
| `Qwen36-35B_20260916_grouped.csv` | Groupé par (api_key, client_tool), trié chronologiquement |
| `Qwen36-35B_20260916_sessions.csv` | Sessions reconstruites |
| `Qwen36-35B_20260916_09h_blocks.csv` | Requêtes 9h avec block IDs générés (étape 5 pipeline.py) |
| `replay_09h_09h20.jsonl` | JSONL GuideLLM pour benchmark mooncake trace replay (fenêtre 9h00-9h20) |
| `sessions_histogram.png` | Histogramme distribution sessions |
| `session_summary.md` | Ce fichier |

## 6. Comment reproduire

```bash
cd v1
python3 multi_session_tracker.py
```

Le script charge `Qwen36-35B_20260916_grouped.csv`, exécute le tracker, et écrit `Qwen36-35B_20260916_sessions.csv` + les diagnostics en stdout.

## 7. Génération des Block IDs (v1.1)

### 7.1 Pipeline

Les block IDs sont générés par `pipeline.py` (étape 5) avec la fonction `generate_block_ids()`.
Cette fonction lit `Qwen36-35B_20260916_sessions.csv` et produit `Qwen36-35B_20260916_{HH}h_blocks.csv`.

### 7.2 Algorithme

Pour chaque session multi-turn (ordonnée chronologiquement) :

```
shared_tokens = min(P_i, P_{i-1} + C_{i-1})
needed_blocks = ceil(P_i / 256)
shared_blocks = floor(shared_tokens / 256)
```

Le block ID de la requête = `accumulated_blocks[:shared_actual] + new_blocks`.

### 7.3 Bugs corrigés (v1.0 → v1.1)

**BUG 1 — Gap ceil/floor** : `ceil(P/256)` vs `floor((P_prev+C_prev)/256)` crée des gaps de 1-2 blocs. Ces blocs manquant correspondent aux tokens de complétion en cache KV. Comblés par un `while` loop.

**BUG 2 — `extend` au lieu de `=`** : L'ancien code `accumulated_blocks = assigned` écrasait la liste quand un prompt était plus court. Le `extend` préserve les blocs existants pour les requêtes futures.

### 7.4 Résultat

| Heure | Requêtes | Block IDs uniques | Mismatches |
|-------|----------|-------------------|------------|
| 09:00 | 3 532 | 29 795 | 0 |

### 7.5 Benchmark GuideLLM

Pour lancer un benchmark mooncake trace replay :

```bash
guidellm run \
  --backend kind=openai_http,target=http://<vllm>:8000 \
  --data "kind=mooncake,path=replay_09h_09h20.jsonl,hash_id_block_size=256" \
  --profile kind=synchronous
```

Le fichier `replay_09h_09h20.jsonl` contient 1 231 requêtes validées (fenêtre 9h00-9h20, timestamps relatifs à 9h en secondes).
