"""
Multi-Session Tracker — Reconstruction heuristique de sessions multitournes.

Approche déterministe, interprétable, sans ML.
Pour chaque groupe (api_key, client_tool), maintient plusieurs sessions actives
et affecte chaque requête au meilleur candidat selon 3 signaux combinés.
"""

import csv
import math
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional


# ==============================================================================
# Configuration
# ==============================================================================

@dataclass
class TrackerConfig:
    """Tous les paramètres configurables du tracker."""

    # SIGNAL 1 — TEMPORAL
    tau: float = 300.0           # constante de temps en secondes (exp decay)
    max_session_gap: float = 1800.0  # 30min — session inactive au-delà retirée

    # SIGNAL 2 — CONTINUITÉ TAILLE PROMPT
    growth_scale: float = 2000.0     # échelle pour la pénalité de croissance
    drop_threshold: float = 0.7      # si ratio < 0.7 => forte pénalité
    max_growth_ratio: float = 5.0   # si croissance > 5x => pénalité extrême

    # SIGNAL 3 — RELATION COMPLETION
    output_match_scale: float = 200.0  # échelle matching output -> prompt (plus strict)

    # SCORE FINAL
    w_time: float = 1.5
    w_growth: float = 1.0
    w_output: float = 1.0
    assignment_threshold: float = 1.5  # seuil min pour affecter à session existante
    max_session_size: int = 250        # max reqs par session — au-delà nouvelle session

    # SORTING
    max_active_sessions_per_group: int = 50  # garde les N sessions les plus récentes


# ==============================================================================
# Parsing
# ==============================================================================

def parse_timestamp(s: str) -> datetime:
    for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse: {s}")


# ==============================================================================
# Score functions
# ==============================================================================

def time_score(dt_seconds: float) -> float:
    """
    Score temporel : décroissance exponentielle.
    dt=0 -> 1.0, dt=tau -> ~0.37, dt=3*tau -> ~0.05
    
    On ne met pas de cutoff brutal. Le decay est doux.
    """
    if dt_seconds < 0:
        return 0.0
    return math.exp(-dt_seconds / config.tau)


def growth_score(p_prev: int, p_new: int) -> float:
    """
    Score de continuité de taille ASYMÉTRIQUE.
    
    Une même session multitournes a typiquement des tailles STABLE ou CROISSANTES.
    - Stabilité (ratio 0.7-1.3) : score haut
    - Légère croissance (<3x) : score moyen-haut (tool calls)
    - Croissance forte (>10x) : score bas (changement de contexte probable)
    - Baisse < 70% : score bas (changement de session probable)
    - Baisse > 70% : score très bas
    """
    if p_prev <= 0:
        return 0.0

    ratio = p_new / p_prev

    if ratio >= 0.7 and ratio <= 3.0:
        # Stable ou légère croissance : score proportionnel à la stabilité
        if abs(ratio - 1.0) < 0.1:
            return 1.0  # quasi identique
        elif abs(ratio - 1.0) < 0.3:
            return 0.85
        else:
            return 0.6 + 0.2 * (1.0 - abs(ratio - 1.0) / 2.0)
    elif ratio > 3.0 and ratio <= config.max_growth_ratio:
        # Croissance forte : possible tool calls, mais moins probable
        return 0.3 * (1.0 / ratio)
    else:
        # Croissance > 10x ou baisse > 30%
        if ratio < config.drop_threshold:
            return 0.05 * ratio  # très bas
        else:
            return 0.01  # pratiquement nul


def output_score(p_prev: int, p_new: int, o_prev: int) -> float:
    """
    Score de relation completion -> prompt suivant.
    
    Dans une conversation classique: p_new ≈ p_prev + O_prev
    Mais tool calls / system messages / context management peuvent modifier ça.
    
    On accepte une plage de tolérance: ±50% de O_prev
    """
    if o_prev <= 0:
        return 0.3  # pas d'info output -> neutre

    growth = p_new - p_prev
    expected = o_prev

    if expected <= 0:
        return 0.3

    ratio = growth / expected

    if ratio >= 0.5 and ratio <= 1.5:
        # Matching parfait dans la plage de tolérance
        diff = abs(ratio - 1.0)
        return 1.0 - 0.3 * diff  # 0.7 à 1.0
    elif ratio > 1.5 and ratio <= 3.0:
        # Croissance > output : tool calls, contexte ajouté
        return 0.4 * (1.0 / ratio)
    elif ratio > 0 and ratio < 0.5:
        # Croissance < 50% de l'output : output pas ou peu réinjecté
        return 0.1 * ratio
    else:
        # growth négatif ou nul: prompt plus petit que précédent
        return 0.05


# ==============================================================================
# Session state
# ==============================================================================

@dataclass
class SessionState:
    """État d'une session candidate."""
    session_id: int
    last_request: dict = field(default_factory=dict)
    last_timestamp: Optional[datetime] = None
    prompt_tokens_history: list = field(default_factory=list)
    request_count: int = 0
    created_at: Optional[datetime] = None
    last_updated_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            'session_id': self.session_id,
            'request_count': self.request_count,
            'last_prompt': self.last_request.get('prompt_tokens', 0),
            'last_completion': self.last_request.get('completion_tokens', 0),
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'last_updated_at': self.last_updated_at.isoformat() if self.last_updated_at else None,
        }


# ==============================================================================
# Multi-Session Tracker
# ==============================================================================

class MultiSessionTracker:
    """
    Pour un groupe de requêtes, maintient un set de sessions actives et
    affecte chaque nouvelle requête à la meilleure session candidate.
    """

    def __init__(self, config: TrackerConfig):
        self.config = config

    def process_group(self, group_key: str, requests: list) -> list:
        """
        Traite toutes les requêtes d'un groupe.

        Args:
            group_key: identifiant du groupe
            requests: liste de dicts triée par timestamp

        Returns:
            Liste de requêtes enrichies avec les champs de session.
        """
        active_sessions: dict[int, SessionState] = {}
        next_session_id = 0
        results = []

        for req in requests:
            req_ts = parse_timestamp(req['startTime'])
            p_new = int(req['prompt_tokens'])
            o_new = int(req['completion_tokens'])

            best_session_id = None
            best_score = -1.0
            best_scores_individual = {}
            second_best_score = -1.0

            # 1. Fermer les sessions pleines AVANT scoring
            full_ids = [
                sid for sid, session in active_sessions.items()
                if session.request_count >= self.config.max_session_size
            ]
            for sid in full_ids:
                del active_sessions[sid]

            # 2. Fermer les sessions trop inactives AVANT scoring
            idle_ids = [
                sid for sid, session in active_sessions.items()
                if session.last_timestamp is not None
                and (req_ts - session.last_timestamp).total_seconds() > self.config.max_session_gap
            ]
            for sid in idle_ids:
                del active_sessions[sid]

            # 3. Scorer chaque session active
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

            # Affectation
            if best_session_id is not None and best_score >= self.config.assignment_threshold:
                # Affecter à la session existante
                session = active_sessions[best_session_id]
                session.last_request = req
                session.last_timestamp = req_ts
                session.prompt_tokens_history.append(p_new)
                session.request_count += 1
                session.last_updated_at = req_ts
                assignment_type = "existing_session"
            else:
                # Créer nouvelle session
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

            # Nettoyage sessions trop anciennes (garder N plus récentes)
            if len(active_sessions) > self.config.max_active_sessions_per_group:
                sorted_by_time = sorted(
                    active_sessions.items(),
                    key=lambda x: x[1].last_updated_at or datetime.min,
                    reverse=True,
                )
                for sid, _ in sorted_by_time[self.config.max_active_sessions_per_group:]:
                    del active_sessions[sid]

            # Construire résultat enrichi
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


# ==============================================================================
# Diagnostics / Validation
# ==============================================================================

def run_diagnostics(all_results: list) -> dict:
    """Produire les métriques de validation."""

    # Grouper par session (clé composite groupe+id pour éviter fusion entre groupes)
    sessions = defaultdict(list)
    for r in all_results:
        session_key = (r['_group_key'], r['reconstructed_session_id'])
        sessions[session_key].append(r)

    stats = {
        'total_requests': len(all_results),
        'total_sessions': len(sessions),
        'sessions_by_group': defaultdict(int),
        'requests_per_session': [],
        'session_durations': [],
        'intra_session_gaps': [],
        'prompt_tokens_over_session': defaultdict(list),
        'ambiguous_assignments': [],
    }

    for (gkey, sid), reqs in sessions.items():
        stats['sessions_by_group'][gkey] += 1
        stats['requests_per_session'].append(len(reqs))

        # Durée
        times = [parse_timestamp(r['startTime']) for r in reqs]
        if len(times) > 1:
            duration = (max(times) - min(times)).total_seconds()
            stats['session_durations'].append(duration)

            # Gaps intra-session
            times_sorted = sorted(times)
            for j in range(1, len(times_sorted)):
                gap = (times_sorted[j] - times_sorted[j-1]).total_seconds()
                stats['intra_session_gaps'].append(gap)

        # Prompt tokens au fil des sessions
        stats['prompt_tokens_over_session'][sid] = [int(r['prompt_tokens']) for r in reqs]

    # Affectations ambiguës (faible score OU faible margin)
    for r in all_results:
        margin = float(r['score_margin'])
        score = float(r['assignment_score'])
        if margin < 0.2 or score < 0.5:
            stats['ambiguous_assignments'].append({
                'group_key': r['_group_key'],
                'session_id': r['reconstructed_session_id'],
                'assignment_type': r['assignment_type'],
                'assignment_score': score,
                'score_margin': margin,
                'time_score': r['time_score'],
                'growth_score': r['growth_score'],
                'output_score': r['output_score'],
                'prompt_tokens': int(r['prompt_tokens']),
                'completion_tokens': int(r['completion_tokens']),
                'startTime': r['startTime'],
            })

    stats['ambiguous_assignments'] = sorted(
        stats['ambiguous_assignments'],
        key=lambda x: x['score_margin'],
    )

    # Stats sur les sessions
    if stats['requests_per_session']:
        stats['requests_per_session_stats'] = {
            'min': min(stats['requests_per_session']),
            'max': max(stats['requests_per_session']),
            'median': statistics.median(stats['requests_per_session']),
            'mean': statistics.mean(stats['requests_per_session']),
        }
    if stats['session_durations']:
        stats['duration_stats'] = {
            'min': min(stats['session_durations']),
            'max': max(stats['session_durations']),
            'median': statistics.median(stats['session_durations']),
            'mean': statistics.mean(stats['session_durations']),
        }
    if stats['intra_session_gaps']:
        stats['gap_stats'] = {
            'min': min(stats['intra_session_gaps']),
            'max': max(stats['intra_session_gaps']),
            'median': statistics.median(stats['intra_session_gaps']),
            'mean': statistics.mean(stats['intra_session_gaps']),
        }

    return stats


# ==============================================================================
# Main pipeline
# ==============================================================================

config = TrackerConfig()

def load_and_group(data_path: str) -> dict:
    """Charger les logs et les regrouper par group_key (déjà groupé)."""
    groups = defaultdict(list)
    with open(data_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row['group_key']
            # Ajouter client_tool depuis la colonne existante
            if 'client_tool' not in row and 'group_key' in row:
                row['client_tool'] = ''  # sera complété si besoin
            groups[key].append(row)
    return groups


def run(data_path: str, output_path: str):
    """Pipeline complet : chargement, tracking, sauvegarde, diagnostics."""

    groups = load_and_group(data_path)
    tracker = MultiSessionTracker(config)
    all_results = []

    for group_key in sorted(groups.keys()):
        reqs = groups[group_key]
        results = tracker.process_group(group_key, reqs)
        all_results.extend(results)

    # Trier par timestamp global pour l'output
    all_results.sort(key=lambda r: r['startTime'])

    # Écrire output
    fieldnames = [
        'startTime', 'Status', 'Duration (s)', 'model',
        'prompt_tokens', 'completion_tokens', 'completionStartTime',
        'user_api_key_alias', 'group_key', 'client_tool',
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

    print(f"✅ Sauvegardé: {output_path}")
    print(f"   Lignes: {len(all_results)}")

    # Diagnostics
    stats = run_diagnostics(all_results)

    print(f"\n=== Diagnostics ===")
    print(f"  Total requêtes: {stats['total_requests']}")
    print(f"  Sessions reconstruites: {stats['total_sessions']}")

    if 'requests_per_session_stats' in stats:
        s = stats['requests_per_session_stats']
        print(f"  Req/session: min={s['min']} max={s['max']} méd={s['median']} moy={s['mean']:.1f}")

    if 'duration_stats' in stats:
        s = stats['duration_stats']
        dur_min = s['min']
        dur_max = s['max']
        dur_med = s['median']
        dur_mean = s['mean']
        print(f"  Durée sessions: min={dur_min:.0f}s max={dur_max:.0f}s méd={dur_med:.0f}s moy={dur_mean:.0f}s")

    if 'gap_stats' in stats:
        s = stats['gap_stats']
        print(f"  Gaps intra-session: min={s['min']:.1f}s max={s['max']:.1f}s méd={s['median']:.1f}s moy={s['mean']:.1f}s")

    ambiguous = len(stats['ambiguous_assignments'])
    total = stats['total_requests']
    print(f"  Affectations ambiguës: {ambiguous} ({ambiguous/total*100:.1f}%)")

    # Top 5 groupes par nb de sessions
    top_groups = sorted(stats['sessions_by_group'].items(), key=lambda x: -x[1])[:5]
    print(f"\n  Top 5 groupes par nombre de sessions:")
    for key, n in top_groups:
        short = key[:50] + "..." if len(key) > 53 else key
        print(f"    {short}: {n} sessions")

    # Top 5 ambiguës
    if stats['ambiguous_assignments']:
        print(f"\n  Exemples d'affectations ambiguës (top 3):")
        for a in stats['ambiguous_assignments'][:3]:
            print(f"    [{a['startTime']}] prompt={a['prompt_tokens']} "
                  f"completion={a['completion_tokens']} "
                  f"score={a['assignment_score']:.3f} margin={a['score_margin']:.3f} "
                  f"(t={a['time_score']:.3f} g={a['growth_score']:.3f} o={a['output_score']:.3f}) "
                  f"type={a['assignment_type']}")


if __name__ == "__main__":
    DATA_PATH = str(Path.home() / "Documents/optimization-vllm/cache_reconstruction/Qwen36-35B_20260916_grouped.csv")
    OUTPUT_PATH = str(Path.home() / "Documents/optimization-vllm/cache_reconstruction/Qwen36-35B_20260916_sessions.csv")
    run(DATA_PATH, OUTPUT_PATH)
