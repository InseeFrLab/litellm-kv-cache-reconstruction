"""
Pipeline complet — Reconstruction prefix cache + Comparaison Prometheus + Block IDs.

Étapes :
1. Chargement des logs LiteLLM (API ou CSV)
2. Groupage par (api_key, client_tool) + filtrage
3. Reconstruction des sessions multi-turnes
4. Estimation du hit rate synthétique
5. Comparaison avec Prometheus (graphique)
6. Génération des block IDs (256 tokens)
7. Réintégration des groupes filtrés (<5k tokens)

Usage :
  # Mode API (recommandé) :
  python3 pipeline.py --litellm-url http://litellm:4000 --litellm-api-key sk-... \
      --start-date 2026-09-16 --end-date 2026-09-17

  # Mode CSV (legacy) :
  python3 pipeline.py --csv ./litellm_logs_20260916_1d_hosted_vllm-Qwen36-35B.csv

  # Options supplémentaires :
  --prometheus PATH   Chemin vers le CSV Prometheus
  --output-dir DIR    Répertoire de sortie (défaut: .)
  --hour H            Heure pour les block IDs (défaut: 9)
"""

import argparse
import csv
import json
import math
import sys
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

try:
    import requests as requests_lib
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


# ==============================================================================
# CONFIGURATION FINALE — V1
# ==============================================================================

class TrackerConfig:
    """Hyperparamètres finaux — ne pas modifier sauf calibration."""

    # SIGNAL 1 — TEMPORAL
    tau: float = 300.0           # constante de temps en secondes
    max_session_gap: float = 1800.0  # 30 min — session inactive fermée

    # SIGNAL 2 — CONTINUITÉ TAILLE PROMPT
    growth_scale: float = 2000.0     # échelle pénalité croissance
    drop_threshold: float = 0.7      # ratio min avant pénalité forte
    max_growth_ratio: float = 5.0   # ratio max avant pénalité extrême

    # SIGNAL 3 — RELATION COMPLETION
    output_match_scale: float = 200.0  # échelle matching output -> prompt

    # SCORE FINAL
    w_time: float = 1.5
    w_growth: float = 1.0
    w_output: float = 1.0
    assignment_threshold: float = 1.5  # seuil min pour rejoindre session
    max_session_size: int = 250        # max reqs par session

    # AUTRES
    prompt_min_tokens: int = 5000      # filtre: groupes < 5k tokens supprimés
    window_size_sec: int = 30          # fenêtre calcul hit rate
    cache_window_min: int = 5          # fenêtre cache (5 min)
    prompt_tps: int = 40000            # 40k tokens/s throughput prefill


# ==============================================================================
# ÉTAPE 1 — Nettoyage des logs
# ==============================================================================

def clean_logs(input_path: str, output_path: str):
    """
    Extraire les colonnes utiles et supprimer metadata.

    Colonne metadata -> extraire user_api_key_alias
    Colonne tags -> extraire client_tool (premier User-Agent)
    Colonne end_user -> extraire directement ou depuis metadata
    """
    print(f"\n[1/6] Nettoyage des logs: {input_path}")

    with open(input_path, 'r') as fin, open(output_path, 'w', newline='') as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=[
            'startTime', 'Status', 'Duration (s)', 'model',
            'prompt_tokens', 'completion_tokens', 'completionStartTime',
            'user_api_key_alias', 'client_tool', 'end_user'
        ])
        writer.writeheader()

        for row in reader:
            meta = json.loads(row.get('metadata', '{}'))
            tags_str = row.get('tags', '')

            # Extraire user_api_key_alias
            alias = meta.get('user_api_key_alias', '')

            # Extraire end_user
            end_user = row.get('end_user', '')

            # Extraire client_tool du premier User-Agent
            client_tool = ''
            try:
                tags = json.loads(tags_str)
                for tag in tags:
                    if tag.startswith('User-Agent:'):
                        ua = tag.split('User-Agent:')[1].strip()
                        client_tool = ua.split('/')[0].strip()
                        break
            except:
                pass

            writer.writerow({
                'startTime': row['startTime'],
                'Status': row['Status'],
                'Duration (s)': row['Duration (s)'],
                'model': row['model'],
                'prompt_tokens': row['prompt_tokens'],
                'completion_tokens': row['completion_tokens'],
                'completionStartTime': row['completionStartTime'],
                'user_api_key_alias': alias,
                'client_tool': client_tool,
                'end_user': end_user,
            })

    total = sum(1 for _ in open(output_path)) - 1
    print(f"   {total} lignes sauvegardées")
    return total


# ==============================================================================
# ÉTAPE 1b — Chargement via API LiteLLM
# ==============================================================================

def _parse_api_timestamp(ts: str) -> str:
    """Convertir un timestamp API (ISO 8601) en format pipeline YYYY-MM-DD HH:MM:SS.ffffff."""
    if not ts:
        return ''
    for fmt in ('%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ',
                '%Y-%m-%dT%H:%M:%S.%f%z', '%Y-%m-%dT%H:%M:%S%z',
                '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
        try:
            dt = datetime.strptime(ts, fmt)
            return dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            continue
    return ts


def fetch_litellm_logs(litellm_url: str, litellm_api_key: str,
                       start_date: str, end_date: str, output_path: str):
    """
    Récupérer les logs depuis l'API LiteLLM /spend/logs et écrire le CSV nettoyé.

    Paramètres :
      litellm_url    : URL de base LiteLLM (ex: http://litellm:4000)
      litellm_api_key: Clé API LiteLLM
      start_date     : Date de début (YYYY-MM-DD)
      end_date       : Date de fin (YYYY-MM-DD)
      output_path    : Chemin de sortie CSV
    """
    print(f"\n[1/6] Récupération des logs via API LiteLLM")
    print(f"   URL: {litellm_url}")
    print(f"   Période: {start_date} → {end_date}")

    url = f"{litellm_url.rstrip('/')}/spend/logs"
    params = {
        "summarize": "false",
        "start_date": start_date,
        "end_date": end_date,
    }
    headers = {
        "Authorization": f"Bearer {litellm_api_key}",
    }

    response = requests_lib.get(url, params=params, headers=headers, timeout=120)
    response.raise_for_status()
    logs = response.json()

    with open(output_path, 'w', newline='') as fout:
        writer = csv.DictWriter(fout, fieldnames=[
            'startTime', 'Status', 'Duration (s)', 'model',
            'prompt_tokens', 'completion_tokens', 'completionStartTime',
            'user_api_key_alias', 'client_tool', 'end_user'
        ])
        writer.writeheader()

        for log in logs:
            # Extraire user_api_key_alias depuis metadata
            metadata = log.get('metadata') or {}
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except (json.JSONDecodeError, TypeError):
                    metadata = {}
            alias = metadata.get('user_api_key_alias', '')

            # Extraire end_user
            end_user = log.get('end_user', '')

            # Extraire client_tool depuis request_tags (User-Agent)
            client_tool = ''
            tags = log.get('request_tags', '')
            if isinstance(tags, str):
                try:
                    tags = json.loads(tags)
                except (json.JSONDecodeError, TypeError):
                    tags = []
            if isinstance(tags, list):
                for tag in tags:
                    if isinstance(tag, str) and tag.startswith('User-Agent:'):
                        ua = tag.split('User-Agent:')[1].strip()
                        client_tool = ua.split('/')[0].strip()
                        break

            # Timestamps
            start_time = _parse_api_timestamp(log.get('startTime', ''))
            completion_start = _parse_api_timestamp(log.get('completionStartTime', ''))
            end_time = _parse_api_timestamp(log.get('endTime', ''))

            # Durée (utiliser request_duration_ms si disponible)
            duration_ms = log.get('request_duration_ms')
            if duration_ms is not None:
                duration = f"{duration_ms / 1000:.3f}"
            elif start_time and end_time:
                try:
                    st = parse_timestamp(start_time)
                    et = parse_timestamp(end_time)
                    duration = f"{(et - st).total_seconds():.3f}"
                except ValueError:
                    duration = ''
            else:
                duration = ''

            writer.writerow({
                'startTime': start_time,
                'Status': log.get('status', 'success'),
                'Duration (s)': duration,
                'model': log.get('model', ''),
                'prompt_tokens': log.get('prompt_tokens', 0),
                'completion_tokens': log.get('completion_tokens', 0),
                'completionStartTime': completion_start or end_time,
                'user_api_key_alias': alias,
                'client_tool': client_tool,
                'end_user': end_user,
            })

    print(f"   {len(logs)} lignes récupérées et sauvegardées")
    return len(logs)


# ==============================================================================
# ÉTAPE 2 — Groupage et filtrage
# ==============================================================================

def group_and_filter(input_path: str, output_path: str, config: TrackerConfig):
    """
    Regrouper par end_user, filtrer les groupes < 5k tokens,
    trier chronologiquement.
    """
    print(f"\n[2/6] Groupage et filtrage: {input_path}")

    groups = defaultdict(list)

    with open(input_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row.get('end_user', '')
            groups[key].append(row)

    def parse_time(r):
        try:
            return datetime.strptime(r['startTime'], '%Y-%m-%d %H:%M:%S.%f')
        except:
            return datetime.strptime(r['startTime'], '%Y-%m-%d %H:%M:%S')

    total = 0
    filtered = 0
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'group_idx', 'row_idx_in_group', 'group_key',
            'startTime', 'Status', 'Duration (s)', 'model',
            'prompt_tokens', 'completion_tokens', 'completionStartTime',
            'user_api_key_alias', 'client_tool', 'end_user'
        ])
        writer.writeheader()

        idx = 0
        for end_user in sorted(groups.keys()):
            reqs = groups[end_user]
            max_prompt = max(int(r['prompt_tokens']) for r in reqs)

            if max_prompt < config.prompt_min_tokens:
                filtered += len(reqs)
                continue

            reqs.sort(key=parse_time)

            for i, row in enumerate(reqs):
                writer.writerow({
                    'group_idx': idx,
                    'row_idx_in_group': i,
                    'group_key': end_user,
                    'startTime': row['startTime'],
                    'Status': row['Status'],
                    'Duration (s)': row['Duration (s)'],
                    'model': row['model'],
                    'prompt_tokens': row['prompt_tokens'],
                    'completion_tokens': row['completion_tokens'],
                    'completionStartTime': row['completionStartTime'],
                    'user_api_key_alias': row.get('user_api_key_alias', ''),
                    'client_tool': row.get('client_tool', ''),
                    'end_user': end_user,
                })
                total += 1

            idx += 1

    print(f"   {total} requêtes dans {idx} groupes (par end_user)")
    print(f"   {filtered} requêtes supprimées (max prompt < {config.prompt_min_tokens})")
    return total


# ==============================================================================
# ÉTAPE 3 — Reconstruction des sessions
# ==============================================================================

def parse_timestamp(s: str) -> datetime:
    for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse: {s}")


def time_score(dt_seconds: float) -> float:
    if dt_seconds < 0:
        return 0.0
    return math.exp(-dt_seconds / config.tau)


def growth_score(p_prev: int, p_new: int) -> float:
    if p_prev <= 0:
        return 0.0

    ratio = p_new / p_prev

    if ratio >= 0.7 and ratio <= 3.0:
        if abs(ratio - 1.0) < 0.1:
            return 1.0
        elif abs(ratio - 1.0) < 0.3:
            return 0.85
        else:
            return 0.6 + 0.2 * (1.0 - abs(ratio - 1.0) / 2.0)
    elif ratio > 3.0 and ratio <= config.max_growth_ratio:
        return 0.3 * (1.0 / ratio)
    else:
        if ratio < config.drop_threshold:
            return 0.05 * ratio
        else:
            return 0.01


def output_score(p_prev: int, p_new: int, o_prev: int) -> float:
    if o_prev <= 0:
        return 0.3

    growth = p_new - p_prev
    expected = o_prev

    if expected <= 0:
        return 0.3

    ratio = growth / expected

    if ratio >= 0.5 and ratio <= 1.5:
        diff = abs(ratio - 1.0)
        return 1.0 - 0.3 * diff
    elif ratio > 1.5 and ratio <= 3.0:
        return 0.4 * (1.0 / ratio)
    elif ratio > 0 and ratio < 0.5:
        return 0.1 * ratio
    else:
        return 0.05


@dataclass
class SessionState:
    session_id: int
    last_request: dict = field(default_factory=dict)
    last_timestamp: Optional[datetime] = None
    prompt_tokens_history: list = field(default_factory=list)
    request_count: int = 0
    created_at: Optional[datetime] = None
    last_updated_at: Optional[datetime] = None


class MultiSessionTracker:
    def __init__(self, config: TrackerConfig):
        self.config = config

    def process_group(self, group_key: str, requests: list) -> list:
        active_sessions: dict[int, SessionState] = {}
        next_session_id = 0
        results = []

        for req in requests:
            req_ts = parse_timestamp(req['startTime'])
            p_new = int(req['prompt_tokens'])

            best_session_id = None
            best_score = -1.0
            best_scores_individual = {}
            second_best_score = -1.0

            # Fermer sessions pleines
            full_ids = [
                sid for sid, session in active_sessions.items()
                if session.request_count >= self.config.max_session_size
            ]
            for sid in full_ids:
                del active_sessions[sid]

            # Fermer sessions trop inactives
            idle_ids = [
                sid for sid, session in active_sessions.items()
                if session.last_timestamp is not None
                and (req_ts - session.last_timestamp).total_seconds() > self.config.max_session_gap
            ]
            for sid in idle_ids:
                del active_sessions[sid]

            # Scorer chaque session active
            for sid, session in active_sessions.items():
                dt = (req_ts - session.last_timestamp).total_seconds()
                p_prev = int(session.last_request.get('prompt_tokens', 0))
                o_prev = int(session.last_request.get('completion_tokens', 0))

                s_time = time_score(dt)
                s_growth = growth_score(p_prev, p_new)
                s_output = output_score(p_prev, p_new, o_prev)

                total = (self.config.w_time * s_time
                        + self.config.w_growth * s_growth
                        + self.config.w_output * s_output)

                if total > best_score:
                    second_best_score = best_score
                    best_score = total
                    best_session_id = sid
                    best_scores_individual = {
                        'time': s_time,
                        'growth': s_growth,
                        'output': s_output,
                    }
                elif total > second_best_score:
                    second_best_score = total

            if best_session_id is not None and best_score >= self.config.assignment_threshold:
                session = active_sessions[best_session_id]
                session.last_request = req
                session.last_timestamp = req_ts
                session.prompt_tokens_history.append(p_new)
                session.request_count += 1
                session.last_updated_at = req_ts
                assignment_type = "existing_session"
            else:
                session = SessionState(
                    session_id=next_session_id,
                    last_request=req,
                    last_timestamp=req_ts,
                    prompt_tokens_history=[p_new],
                    request_count=1,
                    created_at=req_ts,
                    last_updated_at=req_ts,
                )
                active_sessions[next_session_id] = session
                best_session_id = next_session_id
                next_session_id += 1
                assignment_type = "new_session"
                best_scores_individual = {'time': 0.0, 'growth': 0.0, 'output': 0.0}
                second_best_score = -1.0

            if len(active_sessions) > 50:
                sorted_by_time = sorted(
                    active_sessions.items(),
                    key=lambda x: x[1].last_updated_at or datetime.min,
                    reverse=True,
                )
                for sid, _ in sorted_by_time[50:]:
                    del active_sessions[sid]

            result = dict(req)
            result['reconstructed_session_id'] = best_session_id
            result['assignment_score'] = round(best_score, 4)
            result['time_score'] = round(best_scores_individual.get('time', 0), 4)
            result['growth_score'] = round(best_scores_individual.get('growth', 0), 4)
            result['output_score'] = round(best_scores_individual.get('output', 0), 4)
            result['second_best_score'] = round(second_best_score, 4)
            result['score_margin'] = round(best_score - second_best_score, 4)
            result['assignment_type'] = assignment_type
            result['_group_key'] = group_key

            results.append(result)

        return results


def run_tracker(input_path: str, output_path: str, config: TrackerConfig):
    """Exécuter le tracker de sessions sur les données groupées."""
    print(f"\n[3/6] Reconstruction des sessions: {input_path}")

    groups = defaultdict(list)
    with open(input_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row['group_key']
            groups[key].append(row)

    tracker = MultiSessionTracker(config)
    all_results = []

    for group_key in sorted(groups.keys()):
        reqs = groups[group_key]
        results = tracker.process_group(group_key, reqs)
        all_results.extend(results)

    all_results.sort(key=lambda r: r['startTime'])

    fieldnames = [
        'startTime', 'Status', 'Duration (s)', 'model',
        'prompt_tokens', 'completion_tokens', 'completionStartTime',
        'user_api_key_alias', 'group_key', 'client_tool', 'end_user',
        'reconstructed_session_id', 'assignment_score',
        'time_score', 'growth_score', 'output_score',
        'second_best_score', 'score_margin', 'assignment_type',
    ]

    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for r in all_results:
            out_row = {k: r.get(k, '') for k in fieldnames}
            writer.writerow(out_row)

    # Diagnostics
    session_reqs = defaultdict(list)
    for r in all_results:
        session_reqs[(r['group_key'], int(r['reconstructed_session_id']))].append(r)

    sizes = [len(v) for v in session_reqs.values()]
    print(f"   {len(all_results)} requêtes → {len(sizes)} sessions")
    print(f"   Taille session: min={min(sizes)} max={max(sizes)} méd={statistics.median(sizes)}")

    return all_results


# ==============================================================================
# ÉTAPE 4 — Comparaison Prometheus
# ==============================================================================

def compare_with_prometheus(sessions_path: str, prometheus_path: str):
    """
    Estimer le hit rate synthétique et comparer avec Prometheus.

    Méthode : pour chaque requête, le shared prefix = min(P_i, P_{i-1} + C_{i-1})
    Hit rate par fenêtre de 30s sur une fenêtre de 5 min.
    """
    print(f"\n[4/4] Comparaison Prometheus: {prometheus_path}")

    # Charger sessions
    session_reqs = defaultdict(list)
    with open(sessions_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                dt_start = datetime.strptime(row['startTime'], '%Y-%m-%d %H:%M:%S.%f')
            except:
                continue
            sess_id = int(row['reconstructed_session_id'])
            gkey = row['group_key']
            pt = int(row['prompt_tokens'])
            ct = int(row['completion_tokens'])

            session_reqs[(gkey, sess_id)].append({
                'dt': dt_start, 'pt': pt, 'ct': ct,
            })

    for key in session_reqs:
        session_reqs[key].sort(key=lambda r: r['dt'])

    # Calculer shared prefix
    all_reqs = []
    for sess_key, reqs in session_reqs.items():
        for i, req in enumerate(reqs):
            pt = req['pt']
            if i > 0:
                prev_req = reqs[i-1]
                shared = min(pt, prev_req['pt'] + prev_req['ct'])
            else:
                shared = 0

            all_reqs.append({
                'dt': req['dt'], 'pt': pt, 'shared': shared,
            })

    all_reqs.sort(key=lambda r: r['dt'])

    # Fenêtres de 30s sur 5 min
    WINDOW_SIZE = 30
    CACHE_WINDOW = timedelta(minutes=5)

    windows = defaultdict(list)
    for req in all_reqs:
        ts = req['dt']
        window_start = ts.replace(second=(ts.second // WINDOW_SIZE) * WINDOW_SIZE, microsecond=0)
        windows[window_start].append(req)

    synth_data = {}
    for window_30s_start in sorted(windows.keys()):
        window_end = window_30s_start + timedelta(seconds=WINDOW_SIZE)
        window_start_5min = window_end - CACHE_WINDOW

        reqs_in_window = [r for r in all_reqs if window_start_5min <= r['dt'] < window_end]

        total_tokens = sum(r['pt'] for r in reqs_in_window)
        cached_tokens = sum(r['shared'] for r in reqs_in_window)

        synth_data[window_30s_start] = {
            'hit_rate': cached_tokens / total_tokens if total_tokens > 0 else 0,
            'cached_tokens': cached_tokens,
            'total_tokens': total_tokens,
            'n_reqs': len(reqs_in_window),
        }

    # Charger Prometheus
    prom_rows = []
    with open(prometheus_path) as f:
        next(f)  # sep line
        next(f)  # header
        for line in f:
            parts = line.strip().split(',')
            if len(parts) >= 4:
                time_str = parts[0].strip('"')
                try:
                    dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
                    prom_rows.append({
                        'dt': dt,
                        'pod1': float(parts[1]),
                        'pod2': float(parts[2]),
                        'pod3': float(parts[3]),
                    })
                except:
                    pass

    for p in prom_rows:
        p['avg'] = (p['pod1'] + p['pod2'] + p['pod3']) / 3

    # Stats
    synth_avg = sum(s['hit_rate'] for s in synth_data.values() if s['total_tokens'] > 0) / len(synth_data)
    prom_avg = sum(p['avg'] for p in prom_rows) / len(prom_rows)

    print(f"   Synthétique: {synth_avg:.3f} ({synth_avg:.1%})")
    print(f"   Prometheus: {prom_avg:.3f} ({prom_avg:.1%})")
    print(f"   Écart: {abs(synth_avg - prom_avg):.3f}")
    print(f"   Points synth: {len(synth_data)} (30s windows)")
    print(f"   Points prom: {len(prom_rows)} (30s windows)")

    # Graphique
    if HAS_MATPLOTLIB:
        synth_times = sorted([t for t in synth_data.keys() if synth_data[t]['total_tokens'] > 0])
        synth_x = [t + timedelta(hours=2) for t in synth_times]
        synth_y = [synth_data[t]['hit_rate'] for t in synth_times]

        prom_x = [p['dt'] for p in prom_rows]
        prom_y = [p['avg'] for p in prom_rows]

        plt.figure(figsize=(16, 7))
        plt.plot(synth_x, synth_y, 'b-', linewidth=1, alpha=0.8, label='Synthetic (09:00 → 11:00)')
        plt.plot(prom_x, prom_y, 'r-', linewidth=1, alpha=0.8, label='Prometheus (11:00)')
        plt.axhline(y=synth_avg, color='blue', linestyle='--', alpha=0.5, label=f'Synth avg: {synth_avg:.3f}')
        plt.axhline(y=prom_avg, color='red', linestyle='--', alpha=0.5, label=f'Prom avg: {prom_avg:.3f}')

        plt.xlabel('Time')
        plt.ylabel('Prefix Cache Hit Rate (tokens)')
        plt.title('Prefix Cache Hit Rate: Synthetic vs Prometheus')
        plt.legend(loc='lower left')
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()

        output_dir = Path(sessions_path).parent
        plt.savefig(output_dir / 'timeseries_comparison.png', dpi=150)
        print(f"   Graphique sauvegardé: timeseries_comparison.png")

    return synth_avg, prom_avg


# ==============================================================================
# ÉTAPE 5 — Génération des block IDs
# ==============================================================================

def generate_block_ids(sessions_path: str, blocks_path: str, hour: int = 9, block_size: int = 256):
    """
    Assigner des block IDs (entiers uniques) pour chaque session multi-turn.
    
    Règles:
    - Block size = 256 tokens
    - Les block IDs sont uniques entre sessions
    - Pour une session, les blocks s'accumulent au fil des turns
    - Seules les requêtes de l'heure sélectionnée sont traitées
    
    Algorithme par session:
    - shared = min(P_i, P_{i-1} + C_{i-1})
    - needed_blocks = ceil(P_i / block_size)
    - shared_blocks = floor(shared / block_size)
    - Fill gap: if shared_blocks > accumulated (due to ceil vs floor rounding),
      add placeholder blocks for cached completion tokens (C_prev)
    - assigned = accumulated[:shared_actual] + new_blocks
    - accumulated = accumulated + new_blocks (extend, never replace)
    
    Bugs fixes (anciennes versions):
    - BUG 1 (gap ceil/floor): floor((P+C)/256) > ceil(P/256) crée un décalage de 1-2
      blocs. Ces blocs représentent les tokens de complétion C qui sont en cache KV
      mais pas dans accumulated. Ils sont comblés par un while loop.
    - BUG 2 (replace vs extend): `accumulated = assigned` écrasait la liste quand
      un prompt était plus court que le précédent. Les requêtes suivantes échouaient
      silencieusement (slice Python tronque au lieu d'érreurer). Fix: `extend`.
    """
    print(f"\n[5/6] Génération des block IDs (heure {hour:02d}:00, block_size={block_size})")
    
    # Charger sessions
    session_reqs = defaultdict(list)
    with open(sessions_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                dt = datetime.strptime(row['startTime'], '%Y-%m-%d %H:%M:%S.%f')
            except:
                continue
            if dt.hour == hour:
                sess_key = (row['group_key'], int(row['reconstructed_session_id']))
                session_reqs[sess_key].append(row)
    
    # Trier chaque session chronologiquement
    for key in session_reqs:
        session_reqs[key].sort(key=lambda r: r['startTime'])
    
    # Filtrer sessions multi-turn uniquement
    multi_sessions = {k: v for k, v in session_reqs.items() if len(v) > 1}
    
    global_counter = 0
    results = []
    
    for sess_key, reqs in multi_sessions.items():
        accumulated_blocks = []
        
        for i, req in enumerate(reqs):
            pt = int(req['prompt_tokens'])
            ct_prev = int(reqs[i-1]['completion_tokens']) if i > 0 else 0
            
            if i == 0:
                shared_tokens = 0
            else:
                shared_tokens = min(pt, int(reqs[i-1]['prompt_tokens']) + ct_prev)
            
            needed_blocks = -(-pt // block_size)  # ceil(P / block_size)
            shared_blocks = shared_tokens // block_size  # floor(shared / block_size)
            
            # FIX BUG 1 — Gap ceil vs floor:
            # needed_blocks uses ceil (P_i / 256), shared_blocks uses floor (shared / 256).
            # When shared = P_prev + C_prev (completion tokens from previous turn),
            # floor((P_prev + C_prev) / 256) can exceed ceil(P_prev / 256) because
            # C_prev adds tokens to the shared prefix. This creates a gap that must
            # be filled with placeholder block IDs representing cached completion tokens.
            # Example: P_prev=94704 -> ceil=370 blocks. shared=94704+793=95497 -> floor=372.
            # Gap of 2 tokens blocks must be allocated to avoid index errors below.
            while len(accumulated_blocks) < shared_blocks:
                accumulated_blocks.append(global_counter)
                global_counter += 1
            
            # Use at most what we have in accumulated (safety, though the while above
            # should guarantee shared_blocks <= len(accumulated_blocks))
            shared_actual = min(shared_blocks, len(accumulated_blocks))
            assigned = accumulated_blocks[:shared_actual]
            new_count = needed_blocks - shared_actual
            
            if new_count > 0:
                new_blocks = list(range(global_counter, global_counter + new_count))
                global_counter += new_count
                assigned.extend(new_blocks)
                # FIX BUG 2 — extend instead of replace:
                # Previously: accumulated_blocks = assigned
                # This was WRONG: when P_i < P_{i-1}, needed_blocks < len(accumulated)
                # so assigned would be shorter, overwriting accumulated_blocks with
                # fewer elements. Subsequent requests with larger shared_blocks would
                # silently fail (python slice truncates instead of raising).
                # Fix: extend to preserve all previously allocated blocks.
                # Example:
                #   accumulated = [0..369] (370 blocks)
                #   P_i = 80616 -> needed=315, shared=314 -> assigned = [0..313, 370] (315 blocks)
                #   extend([370]) -> accumulated = [0..370] (371 blocks)
                # The prompt uses only 315 blocks, but we keep 371 in accumulated
                # because blocks 315..370 may be shared by future requests.
                accumulated_blocks.extend(new_blocks)
            
            out_row = {k: v for k, v in req.items() if not k.startswith('_')}
            out_row['block_ids'] = json.dumps(assigned)
            results.append(out_row)
    
    # Single-turn : block_ids vide
    # Filtrer les single-turn qui appartiennent à des sessions multi-turns
    multi_turn_keys = set(multi_sessions.keys())
    single_turn_reqs = [v for k, v in session_reqs.items() if len(v) == 1 and k not in multi_turn_keys]
    for req in single_turn_reqs:
        out_row = {k: v for k, v in req.items() if not k.startswith('_')}
        out_row['block_ids'] = ''
        results.append(out_row)
    
    # Trier par startTime
    results.sort(key=lambda r: r['startTime'])
    
    fieldnames = [
        'startTime', 'Status', 'Duration (s)', 'model',
        'prompt_tokens', 'completion_tokens', 'completionStartTime',
        'user_api_key_alias', 'group_key', 'client_tool', 'end_user',
        'reconstructed_session_id', 'assignment_score',
        'time_score', 'growth_score', 'output_score',
        'second_best_score', 'score_margin', 'assignment_type',
        'block_ids'
    ]

    with open(blocks_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    multi = sum(1 for r in results if r['block_ids'] != '')
    single = sum(1 for r in results if r['block_ids'] == '')
    print(f"   Sessions multi-turn: {len(multi_sessions)}")
    print(f"   Requêtes avec blocks: {multi}")
    print(f"   Requêtes single-turn: {single}")
    print(f"   Block IDs uniques: {global_counter}")


# ==============================================================================
# ÉTAPE 6 — Réintégration des groupes filtrés (<5k tokens)
# ==============================================================================

def integrate_filtered_groups(blocks_path: str, cleaned_path: str, grouped_path: str, hour: int = 9):
    """
    Réintégrer les requêtes des groupes filtrés (<5k tokens) dans le fichier blocks.

    Ces groupes ont été supprimés à l'étape 2 car on ne peut pas détecter
    de sessions multi-turn dessus. Elles sont ajoutées comme single-turn
    avec block_ids="".
    """
    print(f"\n[6/6] Réintégration des groupes filtrés (<5k tokens)")

    # Charger blocks existant
    blocks_data = []
    with open(blocks_path) as f:
        for row in csv.DictReader(f):
            blocks_data.append(row)

    covered_keys = set(r['group_key'] for r in blocks_data)

    # Charger données groupées pour trouver les groupes manquants dans la fenêtre
    grouped_data = defaultdict(list)
    with open(grouped_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                dt = datetime.strptime(row['startTime'], '%Y-%m-%d %H:%M:%S.%f')
            except:
                dt = datetime.strptime(row['startTime'], '%Y-%m-%d %H:%M:%S')
            if dt.hour == hour:
                grouped_data[row['group_key']].append(row)

    missing_keys = [g for g in grouped_data.keys() if g not in covered_keys]
    print(f"   Groupes manquant dans groupées: {len(missing_keys)}")

    # Charger données nettoyées brutes pour trouver les groupes filtrés
    filtered_groups = defaultdict(list)
    with open(cleaned_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                dt = datetime.strptime(row['startTime'], '%Y-%m-%d %H:%M:%S.%f')
            except:
                dt = datetime.strptime(row['startTime'], '%Y-%m-%d %H:%M:%S')
            if dt.hour == hour:
                end_user = row.get('end_user', '')
                if end_user not in covered_keys:
                    filtered_groups[end_user].append(row)

    # Add missing rows from grouped data (groups that existed in grouped but not in blocks)
    missing_count = 0
    for gkey in missing_keys:
        for req in grouped_data[gkey]:
            out_row = {k: v for k, v in req.items() if not k.startswith('_')}
            out_row['group_key'] = gkey
            out_row['assignment_type'] = 'grouped_data_missing'
            out_row['block_ids'] = ''
            blocks_data.append(out_row)
            missing_count += 1

    # Add filtered groups (<5k tokens, removed at step 2) as single-turn with empty block_ids
    for end_user, reqs in filtered_groups.items():
        for req in reqs:
            out_row = {k: v for k, v in req.items() if not k.startswith('_')}
            out_row['group_key'] = end_user
            out_row['end_user'] = end_user
            out_row['assignment_type'] = 'filtered_group'
            out_row['block_ids'] = ''
            blocks_data.append(out_row)
            missing_count += 1

    # Trier et écrire
    blocks_data.sort(key=lambda r: r['startTime'])

    fieldnames = [
        'startTime', 'Status', 'Duration (s)', 'model',
        'prompt_tokens', 'completion_tokens', 'completionStartTime',
        'user_api_key_alias', 'group_key', 'client_tool', 'end_user',
        'reconstructed_session_id', 'assignment_score',
        'time_score', 'growth_score', 'output_score',
        'second_best_score', 'score_margin', 'assignment_type',
        'block_ids'
    ]

    with open(blocks_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for r in blocks_data:
            writer.writerow(r)

    multi = sum(1 for r in blocks_data if r['block_ids'] != '')
    single = sum(1 for r in blocks_data if r['block_ids'] == '')
    groups = len(set(r['group_key'] for r in blocks_data))

    print(f"   Groupes ajoutés (grouped): {len(grouped_data) - len([g for g in grouped_data if g not in covered_keys]) + len(missing_keys)}")
    print(f"   Requêtes ajoutées: {missing_count}")
    print(f"   Total: {len(blocks_data)} (multi: {multi}, single: {single})")
    print(f"   Groupes totaux (end_users): {groups}")


# ==============================================================================
# ÉTAPE 7 — Génération de la trace Mooncake/GuideLLM
# ==============================================================================

def generate_trace(blocks_path: str, trace_path: str, hour: int = 9, block_size: int = 256):
    """
    Convertir blocks.csv en trace JSONL compatible Mooncake/GuideLLM.

    Format de sortie (une ligne JSON par requête) :
      {"timestamp": <float>, "input_length": <int>, "output_length": <int>, "hash_ids": [<int>, ...]}

    - timestamp : secondes depuis le début de l'heure (ex: 09:00:00.000)
    - input_length : prompt_tokens
    - output_length : completion_tokens
    - hash_ids : block_ids existants, ou IDs uniques générés pour les single-turn
    """
    print(f"\n[7/7] Génération de la trace Mooncake/GuideLLM")

    rows = []
    with open(blocks_path) as f:
        for row in csv.DictReader(f):
            rows.append(row)

    # Trouver le max block_id existant pour générer des IDs uniques pour les single-turn
    max_block_id = -1
    for row in rows:
        bid = row.get('block_ids', '')
        if bid:
            ids = json.loads(bid)
            if ids:
                max_block_id = max(max_block_id, max(ids))

    next_id = max_block_id + 1

    # Référence temporelle : début de l'heure
    hour_origin = None
    for row in rows:
        try:
            dt = parse_timestamp(row['startTime'])
            if hour_origin is None:
                hour_origin = dt.replace(minute=0, second=0, microsecond=0)
                break
        except ValueError:
            continue

    if hour_origin is None:
        print("   ERREUR: aucune requête avec timestamp valide")
        return

    trace = []
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
            # Single-turn / filtered : générer des IDs uniques
            n_blocks = -(-pt // block_size)  # ceil
            hash_ids = list(range(next_id, next_id + n_blocks))
            next_id += n_blocks

        timestamp = (dt - hour_origin).total_seconds()

        trace.append({
            'timestamp': round(timestamp, 3),
            'input_length': pt,
            'output_length': ct,
            'hash_ids': hash_ids,
        })

    trace.sort(key=lambda r: r['timestamp'])

    with open(trace_path, 'w') as f:
        for entry in trace:
            f.write(json.dumps(entry) + '\n')

    print(f"   {len(trace)} requêtes écrites dans {trace_path}")
    print(f"   Timestamp range: {trace[0]['timestamp']:.1f}s → {trace[-1]['timestamp']:.1f}s")
    print(f"   Hash IDs uniques: {next_id}")


# ==============================================================================
# MAIN
# ==============================================================================

config = TrackerConfig()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pipeline Prefix Cache — V1 (8 étapes, dont 2 opt-in)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--csv", metavar="PATH",
                        help="Chemin vers le CSV LiteLLM brut (mode fichier)")
    source.add_argument("--litellm-url", metavar="URL",
                        help="URL de base LiteLLM (mode API, ex: http://litellm:4000)")

    parser.add_argument("--litellm-api-key", metavar="KEY",
                        help="Clé API LiteLLM (requis avec --litellm-url)")
    parser.add_argument("--start-date", metavar="DATE",
                        help="Date de début YYYY-MM-DD (requis avec --litellm-url)")
    parser.add_argument("--end-date", metavar="DATE",
                        help="Date de fin YYYY-MM-DD (requis avec --litellm-url)")
    parser.add_argument("--prometheus", metavar="PATH",
                        help="Chemin vers le CSV Prometheus (optionnel)")
    parser.add_argument("--output-dir", metavar="DIR", default=".",
                        help="Répertoire de sortie (défaut: répertoire courant)")
    parser.add_argument("--hour", type=int, default=9,
                        help="Heure pour les block IDs (défaut: 9)")

    # Étape 8 (opt-in) : Augmentation de la trace
    parser.add_argument("--augment", action="store_true",
                        help="Activer l'augmentation de la trace (dupliqué des sessions multi-turn)")
    parser.add_argument("--delta-t", type=float, default=5.0,
                        help="Décalage du clone en secondes (défaut: 5.0, requis avec --augment)")
    parser.add_argument("--dup-n", type=int, default=1,
                        help=("Nombre de clones par session multi-turn (factor de "
                              "concurrence exact : dup_n=k → sessions ×(1+k), reqs ×(1+k)). "
                              "0 = pas d'augmentation. Défaut: 1 (×2)."))
    parser.add_argument("--seed", type=int, default=42,
                        help="Graine pour l'augmentation (défaut: 42)")

    args = parser.parse_args()

    # Validation mode API
    if args.litellm_url:
        if not args.litellm_api_key:
            parser.error("--litellm-api-key est requis avec --litellm-url")
        if not args.start_date or not args.end_date:
            parser.error("--start-date et --end-date sont requis avec --litellm-url")
        if not HAS_REQUESTS:
            parser.error("Le module 'requests' est requis pour le mode API. "
                         "Installez-le : pip install requests")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    CLEANED = str(output_dir / "cleaned.csv")
    GROUPED = str(output_dir / "grouped.csv")
    SESSIONS = str(output_dir / "sessions.csv")
    BLOCKS = str(output_dir / "blocks.csv")
    TRACE = str(output_dir / "trace.jsonl")

    print("=" * 80)
    print("PIPELINE PREFIX CACHE — V1 (7 étapes)")
    print("=" * 80)

    if args.litellm_url:
        print(f"   Source: LiteLLM API ({args.litellm_url})")
        print(f"   Période: {args.start_date} → {args.end_date}")
    else:
        print(f"   Source: CSV ({args.csv})")

    print(f"   Sortie: {output_dir.resolve()}")

    # Étape 1: Chargement des logs
    if not Path(CLEANED).exists():
        if args.litellm_url:
            fetch_litellm_logs(
                litellm_url=args.litellm_url,
                litellm_api_key=args.litellm_api_key,
                start_date=args.start_date,
                end_date=args.end_date,
                output_path=CLEANED,
            )
        else:
            clean_logs(args.csv, CLEANED)
    else:
        print(f"\n[1/6] Skip: {CLEANED} existe déjà")

    # Étape 2: Groupage
    if not Path(GROUPED).exists():
        group_and_filter(CLEANED, GROUPED, config)
    else:
        print(f"\n[2/6] Skip: {GROUPED} existe déjà")

    # Étape 3: Reconstruction sessions
    if not Path(SESSIONS).exists():
        run_tracker(GROUPED, SESSIONS, config)
    else:
        print(f"\n[3/6] Skip: {SESSIONS} existe déjà")

    # Étape 4: Comparaison Prometheus
    if args.prometheus and Path(args.prometheus).exists():
        compare_with_prometheus(SESSIONS, args.prometheus)
    else:
        print(f"\n[4/6] Skip: Prometheus non fourni ou non trouvé")

    # Étape 5: Génération des block IDs
    print(f"\n[5/6] Génération des block IDs (heure {args.hour:02d})")
    generate_block_ids(SESSIONS, BLOCKS, hour=args.hour, block_size=256)

    # Étape 6: Réintégration des groupes filtrés
    integrate_filtered_groups(BLOCKS, CLEANED, GROUPED, hour=args.hour)

    # Étape 7: Génération de la trace Mooncake/GuideLLM
    generate_trace(BLOCKS, TRACE, hour=args.hour, block_size=256)

    # Étape 8 (opt-in) : Augmentation de la trace
    TRACE_AUGMENTED = str(output_dir / "trace_augmented.jsonl")
    if args.augment:
        # Augment_trace.py est dans le même dossier que pipeline.py
        import os as _os, sys as _sys
        _here = _os.path.dirname(_os.path.abspath(__file__))
        if _here not in _sys.path:
            _sys.path.insert(0, _here)
        if args.dup_n < 0:
            parser.error(f"--dup-n doit être >= 0 (recu {args.dup_n})")
        from augment_trace import augment as _augment
        _augment(
            blocks_path=BLOCKS,
            output_path=TRACE_AUGMENTED,
            delta_t=args.delta_t,
            dup_n=args.dup_n,
            seed=args.seed,
            block_size=256,
        )
    else:
        print(f"\n[8/8] Skip: --augment non fourni")

    print("\n" + "=" * 80)
    print(f"PIPELINE TERMINE")
    print(f"   Blocks: {BLOCKS}")
    print(f"   Trace:  {TRACE}")
    if args.augment:
        print(f"   Trace augmentée: {TRACE_AUGMENTED}")
    print("=" * 80)
