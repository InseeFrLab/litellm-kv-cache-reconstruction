# cache_reconstruction

Reconstruction heuristique des sessions multi-turnes à partir des logs LiteLLM,
et génération d'IDs de blocs pour le prefix cache de vLLM (mooncake).

## Résumé

Le pipeline identifie les conversations multi-turnes dans les logs d'inférence,
assigne des block IDs uniques aux chunks de tokens (256 tokens/bloc), et produit
des traces compatibles GuideLLM mooncake pour benchmarker le prefix cache.

**Résultat V1** : ~950 sessions détectées sur 30k requêtes, hit rate synthétique
~93% vs ~77% Prometheus (écart attribuable au round-robin et à l'estimation).

## Structure

```
.
├── archive/                # Version V1 (heuristique fusionnée)
│   ├── rapport_v1.md       # Rapport détaillé V1 avec analyse des biais
│   └── reconstruct_cache_v1.py
├── data_raw/               # Logs bruts LiteLLM (non versionnés)
├── v1/                     # Version courante
│   ├── pipeline.py         # Script principal (8 étapes, dont 2 opt-in)
│   ├── augment_trace.py    # Augmentation de trace (étape 8, stdlib uniquement)
│   ├── multi_session_tracker.py
│   ├── samples/            # Exemples de données
│   └── replay_09h_09h20.jsonl  # Trace GuideLLM validée (fenêtre 9h00-9h20)
└── README.md
```

## Quick start

```bash
cd v1
python3 pipeline.py [chemin_logs_csv] [chemin_prometheus_csv]
```

Le script génère automatiquement les fichiers intermédiaires et skip les étapes déjà exécutées.

## Augmentation de la trace (optionnel)

 Le pipeline peut générer une **trace augmentée** : une copie de la trace GuideLLM dans laquelle
 **chaque session multi-turn est dupliquée `N` fois** (`--dup-n N`) — c'est un **factor de
 concurrence exact** : le nombre de sessions ET le nombre de requêtes sont multipliés par
 `1 + N` (ex: `--dup-n 1` → ×2, `--dup-n 4` → ×5).

 Les IDs des clones sont 100 % disjoints de l'origine ET entre eux — le bench de
 **routing par rapport au cache** est donc non-biaisé (pas de hit croisé artificiel entre
 les copies d'une même session).

 ```bash
 # Via pipeline (étape 8 opt-in), concurrence ×2
 python3 v1/pipeline.py --csv ... --output-dir output \
     --augment --delta-t 5 --dup-n 1 --seed 42
 
 # Standalone (lu du blocks.csv produit par les étapes 5-6), concurrence ×5
 python3 v1/augment_trace.py --blocks output/blocks.csv \
     --output output/trace_augmented.jsonl \
     --delta-t 5 --dup-n 4 --seed 42
 ```
 
 Ce qui est garanti :
 - le clone d'une session n'a **aucun** bloc de cache partagé avec l'original (IDs 100 % disjoints)
 - les `N` clones d'une même session sont **disjoints entre eux** (pas de préfixe commun)
 - le décalage est **rigide** (`clone_i.ts = orig_i.ts + delta_t`)
 - le run est **déterministe** (pas de RNG : `N` clones par session, exactement)
 - `trace.jsonl` (étape 7) n'est **jamais** modifié — la trace augmentée est dans un fichier séparé + un audit JSON

Voir `v1/README.md` (§ Étape 8) pour la sémantique, les invariants et les flags.
