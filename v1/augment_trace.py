"""
Augmentation de trace Mooncake/GuideLLM — duplicité de sessions multi-tour.

Lit un fichier `blocks.csv` (sortie de l'étape 5/6 du pipeline) et produit une
trace JSONL augmentée dans laquelle chaque session multi-tour est dupliquée `dup_n`
fois (factor de concurrence exact) : chaque clone reçoit de nouveaux `hash_ids`
100 % disjoints de l'original, des autres clones **d'une même session**, et des clones
d'autres sessions — donc aucun hit croisé artificiel. Les timestamps sont décalés
rigidement d'un `delta_t` (en secondes).

Objectif : alimenter un bench de routing par rapport au cache avec un trafic plus
dense de sessions concurrentes, sans biaiser les résultats. La garantie clé est la
disjointness : min_id_du_clone > max_id_d_origine, et aucun ID du clone est partagé
avec quelque chose d'autre.

Sémantique de `--dup-n` (factor de concurrence exact) :
  - dup_n = 0 → trace = origine seule (pas de clone)
  - dup_n = 1 → nombre de sessions ×2, nombre de reqs ×2 (le cas « ×2 »)
  - dup_n = k ≥ 1 → nombre de sessions ×(1+k), nombre de reqs ×(1+k)

Usage :
  python3 augment_trace.py --blocks output/blocks.csv --output trace_augmented.jsonl \
      --delta-t 5 --dup-n 1 --seed 42 [--block-size 256]

Entrée / sortie :
  - entrée   : blocks.csv (colonnes startTime, prompt_tokens, completion_tokens,
               group_key, reconstructed_session_id, block_ids)
  - sortie 1 : trace augmentée JSONL  (timestamp, input_length, output_length, hash_ids)
  - sortie 2 : <sortie>.audit.json    (compteurs PII-free)

Aucune dépendance externe (stdlib uniquement).
"""

import argparse
import csv
import hashlib
import json
import random
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# ==============================================================================
# Parsing (identique à pipeline.parse_timestamp)
# ==============================================================================

def parse_timestamp(s: str) -> datetime:
    for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse: {s}")


# ==============================================================================
# Fonctions pures
# ==============================================================================

def load_sessions(blocks_path: str):
    """
    Charger blocks.csv et regrouper par (group_key, reconstructed_session_id).
    Retourne (sessions, hour_origin) :
      sessions : dict [(group_key, session_id_int)] -> liste de rows triées par startTime
      hour_origin : datetime (début de l'heure du premier timestamp)
    """
    groups = defaultdict(list)
    with open(blocks_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row['group_key'], int(row['reconstructed_session_id']))
            groups[key].append(row)

    for key in groups:
        groups[key].sort(key=lambda r: r['startTime'])

    hour_origin = None
    for key in groups:
        for r in groups[key]:
            try:
                dt = parse_timestamp(r['startTime'])
                if hour_origin is None:
                    hour_origin = dt.replace(minute=0, second=0, microsecond=0)
                    break
            except ValueError:
                continue
        if hour_origin is not None:
            break

    if hour_origin is None:
        raise ValueError(f"Aucun timestamp valide dans {blocks_path}")

    return groups, hour_origin


def build_original_entries(groups: dict, hour_origin: datetime, block_size: int = 256):
    """
    Construire la partie « origine » de la trace, avec la même logique d'ID que
    `pipeline.generate_trace` :
      - multi-turn  : hash_ids = block_ids de blocks.csv
      - single-turn : hash_ids = range(next_id, next_id + ceil(prompt_tokens/block_size))
    Retourne (entries, max_id_used, max_block_id_source) :
      entries           : liste de dicts {timestamp, input_length, output_length, hash_ids}
      max_id_used       : max des IDs assignés (pour placer le clone_counter)
      max_block_id_source : max des IDs bruts de `block_ids` (si aucun, -1)
    """
    # max des block_ids bruts
    max_block_id = -1
    for rows in groups.values():
        for row in rows:
            bid = row.get('block_ids', '')
            if bid:
                ids = json.loads(bid)
                if ids:
                    max_block_id = max(max_block_id, max(ids))

    next_id = max_block_id + 1

    entries = []
    max_id_used = max_block_id

    # Parcours déterministe : trié par clé de groupe + session, puis par startTime
    for key in sorted(groups.keys(), key=lambda k: (k[0], k[1])):
        rows = groups[key]
        is_multi = len(rows) > 1
        for row in rows:
            try:
                dt = parse_timestamp(row['startTime'])
            except ValueError:
                continue
            pt = int(row['prompt_tokens'])
            ct = int(row['completion_tokens'])
            bid = row.get('block_ids', '')

            if bid:
                hash_ids = json.loads(bid)
            else:
                n_blocks = -(-pt // block_size)  # ceil
                hash_ids = list(range(next_id, next_id + n_blocks))
                next_id += n_blocks

            entries.append({
                'timestamp': round((dt - hour_origin).total_seconds(), 3),
                'input_length': pt,
                'output_length': ct,
                'hash_ids': hash_ids,
                # interne (retiré avant écriture)
                '_key': key,
                '_is_multi': is_multi,
            })

    max_id_used = max([max_id_used] + [max(e['hash_ids']) for e in entries if e['hash_ids']])
    return entries, max_id_used, max_block_id


def clone_sessions(groups: dict, hour_origin: datetime,
                   max_id_used: int, dup_n: int, seed: int,
                   delta_t: float) -> list:
    """
    Cloner les sessions multi-turn.

    Sémantique de `dup_n` (factor de concurrence exact) :
      - dup_n = 0 → aucune session n'est clonée (trace = origine seule)
      - dup_n = k >= 1 → chaque session multi-turn reçoit exactement k clones
                    (k = 1 → nombre de sessions ×2, nombre de reqs ×2 ;
                     k = 2 → nombre de sessions ×3, nombre de reqs ×3 ; ...)

    Parcours déterministe des sessions multi-turn (triées par (g, sid)).
    Les nouveaux IDs sont alloués à partir de max_id_used + 1 (disjoint de
    l'origine). Chaque **clone** d'une session reçoit sa propre bijection
    block_id_orig -> block_id_clone (première apparition) : les clones d'une même
    session sont disjoints entre eux, et entre toute la partie origine.

    `seed` n'est plus utilisé par la fonction (le tirage aléatoire a été retiré) ;
    on le garde dans la signature pour compatibilité d'API / audit.

    Retourne la liste des entrées clonées.
    """
    _unused_seed = seed  # silencieux, documenté ci-dessus
    del _unused_seed

    multi_keys = sorted(
        [k for k, v in groups.items() if len(v) > 1],
        key=lambda k: (k[0], k[1]),
    )

    clone_counter = max_id_used + 1
    entries = []

    for key in multi_keys:
        rows = groups[key]

        for _clone_idx in range(dup_n):
            # Bijection (par clone, pas par session) : les 2 clones n'ont AUCUN
            # ID partagé entre eux (sinon un hit croisé artificiel entre les
            # 2 copies de la même session).
            mapping = {}
            for row in rows:
                bid = row.get('block_ids', '')
                if not bid:
                    continue
                for b in json.loads(bid):
                    if b not in mapping:
                        mapping[b] = clone_counter
                        clone_counter += 1

            for row in rows:
                try:
                    dt = parse_timestamp(row['startTime'])
                except ValueError:
                    continue
                pt = int(row['prompt_tokens'])
                ct = int(row['completion_tokens'])
                bid = row.get('block_ids', '')
                if not bid:
                    continue
                orig_ids = json.loads(bid)
                new_ids = [mapping[b] for b in orig_ids]

                entries.append({
                    'timestamp': round((dt - hour_origin).total_seconds() + delta_t, 3),
                    'input_length': pt,
                    'output_length': ct,
                    'hash_ids': new_ids,
                    '_key': key,
                    '_is_multi': True,
                })

    return entries


def verify_clones(original_entries: list, clone_entries: list,
                   groups: dict, delta_t: float, dup_n: int):
    """
    Vérifier les invariants de non-biais entre origine et clones.

    Avec `dup_n = k`, chaque session multi-turn de taille `L` (L turns) produit
    `k × L` entrées clonées : `k` sous-séquences de `L` entrées, l'une par clone.
    Les invariants sont vérifiés **clone par clone** (on divise les `k × L`
    entrées clonées de la session en `k` blocs de `L`).

    Invariants vérifiés :
      - offset   : clone_i.ts - orig_i.ts == delta_t (arrondi 3 déc)
      - gap      : clone_{i+1}.ts - clone_i.ts == orig_{i+1}.ts - orig_i.ts (INTRA-CLONE)
      - len      : len(clone.hash_ids) == len(orig.hash_ids)
      - newblock : |set(i) - set(i-1)| identique clone vs original (INTRA-CLONE)

    Retourne un dict de compteurs (doit être tous 0, sauf n_turns_checked).
    """
    # Indexer par (key)
    orig_by_key = defaultdict(list)
    for i, e in enumerate(original_entries):
        orig_by_key[e['_key']].append((i, e))

    clone_by_key = defaultdict(list)
    for e in clone_entries:
        clone_by_key[e['_key']].append(e)

    stats = {
        'n_turns_checked': 0,
        'offset_mismatches': 0,
        'gap_mismatches': 0,
        'len_mismatches': 0,
        'newblock_mismatches': 0,
        'n_clone_blocks': 0,
    }

    for key, clones in clone_by_key.items():
        origs = orig_by_key.get(key, [])
        L = len(origs)
        if L == 0:
            continue
        # Avec dup_n = k, on s'attend à k × L entrées clonées
        if len(clones) != dup_n * L:
            stats['offset_mismatches'] += len(clones)
            continue

        # Découper en k blocs de L entrées (l'ordre des blocs est celui de clone_sessions)
        for clone_idx in range(dup_n):
            block = clones[clone_idx * L : (clone_idx + 1) * L]
            stats['n_clone_blocks'] += 1

            for i, (orig_idx, orig) in enumerate(origs):
                clone = block[i]
                stats['n_turns_checked'] += 1

                # offset
                expected_ts = round(orig['timestamp'] + delta_t, 3)
                if abs(clone['timestamp'] - expected_ts) > 1e-9:
                    stats['offset_mismatches'] += 1

                # len
                if len(clone['hash_ids']) != len(orig['hash_ids']):
                    stats['len_mismatches'] += 1

                # gap (intra-clone ; si i < last)
                if i + 1 < L:
                    gap_orig = round(origs[i + 1][1]['timestamp'] - orig['timestamp'], 3)
                    gap_clone = round(block[i + 1]['timestamp'] - clone['timestamp'], 3)
                    if abs(gap_orig - gap_clone) > 1e-9:
                        stats['gap_mismatches'] += 1

                # newblock (intra-clone)
                if i > 0:
                    prev_orig = origs[i - 1][1]
                    prev_clone = block[i - 1]
                    new_orig = len(set(orig['hash_ids']) - set(prev_orig['hash_ids']))
                    new_clone = len(set(clone['hash_ids']) - set(prev_clone['hash_ids']))
                    if new_orig != new_clone:
                        stats['newblock_mismatches'] += 1

    return stats


# ==============================================================================
# Orchestration + écriture
# ==============================================================================

def augment(blocks_path: str, output_path: str,
            delta_t: float = 5.0, dup_n: int = 1, seed: int = 42,
            block_size: int = 256) -> dict:
    """
    Produire la trace augmentée + l'audit.

    `dup_n` = factor de concurrence exact :
      - dup_n = 0 → pas de clone (trace = origine seule)
      - dup_n = k >= 1 → k clones par session multi-turn
                      (nombre de sessions × (1 + k), nombre de reqs × (1 + k))

    Retourne le dict d'audit (également écrit dans <output_path>.audit.json).
    """
    groups, hour_origin = load_sessions(blocks_path)
    original_entries, max_id_used, max_block_id_source = build_original_entries(
        groups, hour_origin, block_size=block_size
    )
    clone_entries = clone_sessions(
        groups, hour_origin, max_id_used,
        dup_n=dup_n, seed=seed, delta_t=delta_t,
    )

    # Invariants
    stats = verify_clones(original_entries, clone_entries, groups, delta_t, dup_n=dup_n)

    # Fusion + tri par timestamp
    all_entries = list(original_entries) + list(clone_entries)
    all_entries.sort(key=lambda e: e['timestamp'])

    # Construction de l'audit (tandis que '_key' est encore disponible)
    all_id_set = set()
    for e in all_entries:
        all_id_set.update(e['hash_ids'])

    clone_ids = set()
    for e in clone_entries:
        clone_ids.update(e['hash_ids'])

    orig_id_set = all_id_set - clone_ids

    # Écrire la trace (après nettoyage des champs internes)
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, 'w') as f:
        for e in all_entries:
            e_out = {k: v for k, v in e.items() if not k.startswith('_')}
            f.write(json.dumps(e_out) + '\n')

    # SHA256 de la sortie
    with open(out, 'rb') as f:
        sha = hashlib.sha256(f.read()).hexdigest()

    n_multi = sum(1 for v in groups.values() if len(v) > 1)
    audit = {
        'n_original_entries': len(original_entries),
        'n_clone_entries': len(clone_entries),
        'n_total_entries': len(original_entries) + len(clone_entries),
        'n_multi_sessions': n_multi,
        'n_cloned_sessions': len({e['_key'] for e in clone_entries}),
        'dup_n': dup_n,
        'concurrency_factor': (1 + dup_n) if n_multi > 0 else 1,
        'seed': seed,
        'delta_t': delta_t,
        'block_size': block_size,
        'max_original_id': max_id_used,
        'min_clone_id': min(clone_ids) if clone_ids else None,
        'clone_id_overlap_original': len(clone_ids & orig_id_set),
        'n_unique_ids_total': len(all_id_set),
        **stats,
        'output_sha256': sha,
    }

    audit_path = out.parent / (out.name + '.audit.json')
    with open(audit_path, 'w') as f:
        json.dump(audit, f, indent=2)

    return audit


# ==============================================================================
# CLI
# ==============================================================================

def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Augmenter une trace Mooncake/GuideLLM (dupliqué sessions multi-turn)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('--blocks', required=True,
                        help="Chemin du blocks.csv (entrée)")
    parser.add_argument('--output', required=True,
                        help="Chemin de la trace augmentée (sortie JSONL)")
    parser.add_argument('--delta-t', type=float, default=5.0,
                        help="Décalage rigide du clone (secondes)")
    parser.add_argument('--dup-n', type=int, default=1,
                        help=("Nombre de clones par session multi-turn (factor de "
                              "concurrence exact : dup_n=k → sessions ×(1+k), reqs ×(1+k)). "
                              "0 = pas d'augmentation."))
    parser.add_argument('--seed', type=int, default=42,
                        help="Graine (déterminisme, conservé pour compatibilité d'API)")
    parser.add_argument('--block-size', type=int, default=256,
                        help="Taille d'un bloc (tokens)")
    args = parser.parse_args(argv)

    if args.dup_n < 0:
        parser.error(f"--dup-n doit être >= 0 (recu {args.dup_n})")
    if args.delta_t < 0:
        parser.error(f"--delta-t doit être >= 0 (recu {args.delta_t})")

    audit = augment(
        blocks_path=args.blocks,
        output_path=args.output,
        delta_t=args.delta_t,
        dup_n=args.dup_n,
        seed=args.seed,
        block_size=args.block_size,
    )

    print(f"   Trace augmentée: {args.output}")
    print(f"   Audit:           {Path(args.output).parent / (Path(args.output).name + '.audit.json')}")
    print(f"   Origine: {audit['n_original_entries']} entrées, "
          f"clones: {audit['n_clone_entries']} entrées, "
          f"total: {audit['n_total_entries']}")
    print(f"   Multi-sessions: {audit['n_multi_sessions']}, "
          f"clonées: {audit['n_cloned_sessions']}, "
          f"factor de concurrence: ×{audit['concurrency_factor']}")
    print(f"   Delta_t: {audit['delta_t']}s, dup_n: {audit['dup_n']}, seed: {audit['seed']}")
    print(f"   max_id original: {audit['max_original_id']}, min_id clone: {audit['min_clone_id']}")
    print(f"   Invariants: offset={audit['offset_mismatches']} gap={audit['gap_mismatches']} "
          f"len={audit['len_mismatches']} newblock={audit['newblock_mismatches']} "
          f"(turns vérifiés: {audit['n_turns_checked']})")
    return 0


if __name__ == '__main__':
    sys.exit(main())
