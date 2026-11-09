import http.server
import json
import mimetypes
import os
import socketserver
import threading
from urllib.parse import urlsplit

# 1. 클라우드 플랫폼(Render.com 등) 동적 포트 환경변수 우선 바인딩 (로컬 기본값: 3000)
PORT = int(os.environ.get('PORT', 3000))
HOST = '0.0.0.0'
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(ROOT_DIR, '.gt_server_state.json')

# 4. 멀티 유저 동시성 보호를 위한 전역 스레드 락
LOCK = threading.Lock()

# 3. 모바일 PWA 및 정적 리소스 표준 MIME 타입 등록
mimetypes.init()
mimetypes.add_type('application/manifest+json', '.webmanifest')
mimetypes.add_type('image/svg+xml', '.svg')
mimetypes.add_type('application/json', '.json')
mimetypes.add_type('application/javascript', '.js')
mimetypes.add_type('text/html', '.html')


def load_server_state():
    if not os.path.exists(STATE_FILE):
        return None
    try:
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def save_server_state(data):
    """원자적(Atomic) 파일 저장으로 동시 쓰기 시 파일 손상 원천 방지"""
    tmp_file = STATE_FILE + '.tmp'
    try:
        with open(tmp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp_file, STATE_FILE)
    except Exception:
        try:
            with open(STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
        except Exception:
            pass


def _is_bot_id(uid, users_map=None, profiles_map=None):
    if not uid or not isinstance(uid, str):
        return False
    if uid.startswith('bot_'):
        return True
    if isinstance(users_map, dict) and isinstance(users_map.get(uid), dict):
        if users_map[uid].get('isBot'):
            return True
    if isinstance(profiles_map, dict) and isinstance(profiles_map.get(uid), dict):
        if profiles_map[uid].get('isBot'):
            return True
    return False


def _empty_stats():
    return {'pts': 0, 'reb': 0, 'ast': 0, 'stl': 0, 'blk': 0, 'tov': 0, 'pf': 0}


def _status_rank(m):
    if not isinstance(m, dict):
        return 0
    st = m.get('status') or 'open'
    if st == 'finished' and isinstance(m.get('result'), dict):
        return 4
    if st == 'cancelled':
        return 3
    if st == 'live' and isinstance(m.get('live'), dict):
        return 2
    if st in ('open', 'closed'):
        return 1
    return 1


def merge_match_objects(m_c, m_i, users_map=None):
    """
    두 매치 상태를 병합하되:
    1) 기기 간 시계 오차(Clock Skew)가 있어도 매치 진행 단계(open/closed -> live -> finished)가 절대 역행하지 않도록 보호
    2) 방금 신청한 실제 유저(친구) 참가자 ID 및 프로필(participantProfiles)을 100% 보존
    3) 정원 도달 시 게스트 봇(bot_*)보다 실제 유저를 항상 우선 배치하고, live 경기 중 합류한 실제 유저도 즉시 팀/코트에 편입
    """
    r_c = _status_rank(m_c)
    r_i = _status_rank(m_i)
    rev_c = m_c.get('rev') or 0
    rev_i = m_i.get('rev') or 0
    t_c = m_c.get('updatedAt') or m_c.get('createdAt') or 0
    t_i = m_i.get('updatedAt') or m_i.get('createdAt') or 0

    if r_i != r_c:
        newer = m_i if r_i > r_c else m_c
        older = m_c if r_i > r_c else m_i
    elif rev_i != rev_c:
        newer = m_i if rev_i > rev_c else m_c
        older = m_c if rev_i > rev_c else m_i
    else:
        newer = m_i if t_i >= t_c else m_c
        older = m_c if t_i >= t_c else m_i

    # 참가자 프로필 스냅샷 병합
    merged_profiles = {}
    for src in (older.get('participantProfiles') or {}, newer.get('participantProfiles') or {}):
        if isinstance(src, dict):
            for uid, prof in src.items():
                if isinstance(prof, dict) and uid:
                    merged_profiles[uid] = dict(merged_profiles.get(uid) or {}, **prof)

    newer_participants = list(newer.get('participants') or [])
    older_participants = list(older.get('participants') or [])

    # 유저별 신청/취소 시각 맵 병합 (CRDT LWW-Element-Set)
    applied_at = {}
    for src in (older.get('appliedAt') or {}, newer.get('appliedAt') or {}):
        if isinstance(src, dict):
            for k, v in src.items():
                if isinstance(v, (int, float)) and v > applied_at.get(k, 0):
                    applied_at[k] = v

    cancelled_at = {}
    for src in (older.get('cancelledAt') or {}, newer.get('cancelledAt') or {}):
        if isinstance(src, dict):
            for k, v in src.items():
                if isinstance(v, (int, float)) and v > cancelled_at.get(k, 0):
                    cancelled_at[k] = v

    cancelled_set = set(older.get('cancelledIds') or []) | set(newer.get('cancelledIds') or [])
    removed_set = set(older.get('removedIds') or []) | set(newer.get('removedIds') or [])

    # 요청을 보낸 클라이언트의 직전 액션(lastOp)을 반영
    inc_op = m_i.get('lastOp') if isinstance(m_i.get('lastOp'), dict) else None
    cur_op = m_c.get('lastOp') if isinstance(m_c.get('lastOp'), dict) else None
    latest_op = inc_op
    if cur_op and inc_op and (cur_op.get('at') or 0) > (inc_op.get('at') or 0):
        latest_op = cur_op

    for op in (cur_op, inc_op, latest_op):
        if not op or not op.get('uid'):
            continue
        uid = op['uid']
        op_type = op.get('type')
        op_ts = op.get('at') or 0
        if op_type == 'APPLY' and op_ts >= cancelled_at.get(uid, 0):
            applied_at[uid] = max(applied_at.get(uid, 0), op_ts)
            cancelled_set.discard(uid)
            removed_set.discard(uid)
        elif op_type == 'CANCEL' and op_ts >= applied_at.get(uid, 0):
            cancelled_at[uid] = max(cancelled_at.get(uid, 0), op_ts)
            cancelled_set.add(uid)
        elif op_type == 'ADMIN_REMOVE_PARTICIPANT' and op_ts >= applied_at.get(uid, 0):
            cancelled_at[uid] = max(cancelled_at.get(uid, 0), op_ts)
            removed_set.add(uid)

    # 재신청 시각(applied_at)이 취소 시각(cancelled_at)보다 최신인 유저는 취소/제외 목록에서 해제
    for uid, a_ts in list(applied_at.items()):
        if a_ts > cancelled_at.get(uid, 0):
            cancelled_set.discard(uid)
            removed_set.discard(uid)
    for uid, c_ts in list(cancelled_at.items()):
        if c_ts > applied_at.get(uid, 0):
            cancelled_set.add(uid)

    raw_candidates = []
    candidate_pool = older_participants + newer_participants + [
        uid for uid, a_ts in applied_at.items() if a_ts > cancelled_at.get(uid, 0)
    ]

    for pid in candidate_pool:
        if not pid or pid in raw_candidates:
            continue
        if pid not in cancelled_set and pid not in removed_set:
            raw_candidates.append(pid)

    # 실제 유저(친구)를 항상 게스트 봇(bot_*)보다 우선 배치하고, 정원 초과 시 게스트 봇만 자동 밀어냄
    real_pids = [pid for pid in raw_candidates if not _is_bot_id(pid, users_map, merged_profiles)]
    bot_pids = [pid for pid in raw_candidates if _is_bot_id(pid, users_map, merged_profiles)]
    max_p = int(newer.get('maxPlayers') or 0)
    is_unlim = bool(newer.get('isUnlimited') or max_p == 0)
    if not is_unlim and max_p > 0 and len(real_pids) + len(bot_pids) > max_p:
        keep_bots = max(0, max_p - len(real_pids))
        merged_participants = real_pids + bot_pids[:keep_bots]
    else:
        merged_participants = real_pids + bot_pids

    merged = dict(older, **newer)

    # 두 상태 모두 live인 경우 더 많은 스탯 기록(undo 길이)을 가진 live 객체를 채택하고,
    # 새로 참가 신청한 실제 유저가 live.teams에 누락되어 있으면 즉시 팀/코트/스탯에 자동 편입
    if merged.get('status') == 'live':
        live_c = m_c.get('live') if isinstance(m_c.get('live'), dict) else None
        live_i = m_i.get('live') if isinstance(m_i.get('live'), dict) else None
        if live_c and live_i:
            u_c_len = len(live_c.get('undo') or [])
            u_i_len = len(live_i.get('undo') or [])
            best_live = dict(live_i if u_i_len >= u_c_len else live_c)
        else:
            best_live = dict(live_i or live_c or {})

        if best_live and isinstance(best_live.get('teams'), dict):
            teams_a = list(best_live['teams'].get('A') or [])
            teams_b = list(best_live['teams'].get('B') or [])
            on_a = list((best_live.get('onCourt') or {}).get('A') or [])
            on_b = list((best_live.get('onCourt') or {}).get('B') or [])
            stats_map = dict(best_live.get('stats') or {})
            fmt_str = str(merged.get('format') or '5vs5')
            n_court = 3 if '3vs3' in fmt_str else 5

            for pid in real_pids:
                if pid not in teams_a and pid not in teams_b:
                    real_a = sum(1 for x in teams_a if not _is_bot_id(x, users_map, merged_profiles))
                    real_b = sum(1 for x in teams_b if not _is_bot_id(x, users_map, merged_profiles))
                    target_team = 'A' if (real_a < real_b or (real_a == real_b and len(teams_a) <= len(teams_b))) else 'B'
                    if target_team == 'A':
                        teams_a.insert(0, pid)
                        if pid not in on_a:
                            bot_on_a = [x for x in on_a if _is_bot_id(x, users_map, merged_profiles)]
                            if len(on_a) < n_court:
                                on_a.append(pid)
                            elif bot_on_a:
                                on_a[on_a.index(bot_on_a[-1])] = pid
                    else:
                        teams_b.insert(0, pid)
                        if pid not in on_b:
                            bot_on_b = [x for x in on_b if _is_bot_id(x, users_map, merged_profiles)]
                            if len(on_b) < n_court:
                                on_b.append(pid)
                            elif bot_on_b:
                                on_b[on_b.index(bot_on_b[-1])] = pid
                if pid not in stats_map:
                    stats_map[pid] = _empty_stats()

            for pid in teams_a + teams_b:
                if pid not in stats_map:
                    stats_map[pid] = _empty_stats()
                if pid not in merged_participants:
                    merged_participants.append(pid)

            best_live['teams'] = {'A': teams_a, 'B': teams_b}
            best_live['onCourt'] = {'A': on_a, 'B': on_b}
            best_live['stats'] = stats_map
            merged['live'] = best_live

    if merged.get('status') == 'finished':
        res = m_i.get('result') if isinstance(m_i.get('result'), dict) else m_c.get('result')
        if isinstance(res, dict):
            merged['result'] = res

    merged['participants'] = merged_participants
    merged['participantProfiles'] = merged_profiles
    merged['cancelledIds'] = list(cancelled_set)
    merged['removedIds'] = list(removed_set)
    merged['appliedAt'] = applied_at
    merged['cancelledAt'] = cancelled_at
    merged['rev'] = max(rev_c, rev_i)
    merged['updatedAt'] = max(t_c, t_i)
    if latest_op:
        merged['lastOp'] = latest_op
    return merged


def merge_states(cur, inc):
    if not cur or not isinstance(cur, dict) or cur.get('version') != 2:
        cur = None
    if not inc or not isinstance(inc, dict) or inc.get('version') != 2:
        return cur
    if not cur:
        out = dict(inc)
        # 서버 상태에는 기기별 개인 세션(auth, meId)을 저장하지 않음
        out['auth'] = None
        out['meId'] = None
        out.pop('explicitLogout', None)
        return out

    # 삭제된 매치 ID 추적
    deleted_ids = set(cur.get('deletedMatchIds') or []) | set(inc.get('deletedMatchIds') or [])

    # 멀티 유저 병합: 각 스마트폰에서 가입한 모든 지인들의 프로필, 포지션 조정, 스탯 기록을 충돌 없이 합침
    # 주의: 경기수(gp)가 더 많은 상태를 최우선 보존하여 경기 종료 후 레이팅/랭킹이 절대 초기화되지 않도록 보장
    users = dict(cur.get('users') or {})
    for uid, u in (inc.get('users') or {}).items():
        if not uid or not isinstance(u, dict):
            continue
        if uid in users and isinstance(users[uid], dict):
            u_c = users[uid]
            gp_c = (u_c.get('totals') or {}).get('gp', 0)
            gp_i = (u.get('totals') or {}).get('gp', 0)
            g_c = len(u_c.get('games') or [])
            g_i = len(u.get('games') or [])
            rev_c = u_c.get('rev') or 0
            rev_i = u.get('rev') or 0
            u_c_t = u_c.get('updatedAt') or u_c.get('joinedAt') or 0
            u_i_t = u.get('updatedAt') or u.get('joinedAt') or 0

            # 프로필(닉네임, 포지션, 신체정보 등)은 더 최신 수정본을 따름
            prof_newer = u if (rev_i > rev_c or (rev_i == rev_c and u_i_t >= u_c_t)) else u_c
            prof_older = u_c if prof_newer is u else u
            merged_u = dict(prof_older, **prof_newer)

            # 경기 기록 및 레이팅/티어는 gp(출전 경기수)가 더 많은 쪽을 무조건 우선 보존
            if (gp_i, g_i, rev_i, u_i_t) >= (gp_c, g_c, rev_c, u_c_t):
                stat_winner = u
            else:
                stat_winner = u_c

            for k in ('rating', 'lastDelta', 'sigma', 'totals', 'badges', 'history', 'games'):
                if k in stat_winner:
                    merged_u[k] = stat_winner[k]
            merged_u['rev'] = max(rev_c, rev_i)
            merged_u['updatedAt'] = max(u_c_t, u_i_t)
            users[uid] = merged_u
        else:
            users[uid] = u

    # 매치 병합: ID 기준으로 최신 상태/진행단계 반영 + 멀티 유저 참가 신청자 합집합 보존
    cur_matches = {m['id']: m for m in (cur.get('matches') or []) if isinstance(m, dict) and m.get('id')}
    inc_matches = {m['id']: m for m in (inc.get('matches') or []) if isinstance(m, dict) and m.get('id')}
    all_ids = []
    for m in (inc.get('matches') or []) + (cur.get('matches') or []):
        mid = m.get('id') if isinstance(m, dict) else None
        if mid and mid not in all_ids and mid not in deleted_ids:
            all_ids.append(mid)

    merged_matches = []
    for mid in all_ids:
        m_c = cur_matches.get(mid)
        m_i = inc_matches.get(mid)
        if m_c and m_i:
            merged_matches.append(merge_match_objects(m_c, m_i, users))
        elif m_i:
            merged_matches.append(m_i)
        elif m_c:
            merged_matches.append(m_c)

    # 매치 내 participantProfiles에 담긴 실제 유저 프로필이 users 딕셔너리에 누락되어 있으면 자동 복구
    # + 완료된 경기(finished)의 result.rows가 유저 기록(games/rating/totals)에 누락되어 있으면 자동 반영
    for m in merged_matches:
        if not isinstance(m, dict):
            continue
        p_profs = m.get('participantProfiles') or {}
        if isinstance(p_profs, dict):
            for uid, prof in p_profs.items():
                if uid and isinstance(prof, dict) and uid not in users:
                    users[uid] = dict(prof)
        if m.get('status') == 'finished' and isinstance(m.get('result'), dict):
            mid = m.get('id')
            for row in (m['result'].get('rows') or []):
                if not isinstance(row, dict) or not row.get('id'):
                    continue
                uid = row['id']
                if uid not in users or not isinstance(users[uid], dict):
                    continue
                u_obj = dict(users[uid])
                u_games = list(u_obj.get('games') or [])
                if not any(isinstance(g, dict) and g.get('matchId') == mid for g in u_games):
                    totals = dict({'gp': 0, 'wins': 0, 'losses': 0, 'draws': 0, 'pts': 0, 'reb': 0, 'ast': 0, 'stl': 0, 'blk': 0, 'tov': 0, 'pf': 0, 'abei': 0}, **(u_obj.get('totals') or {}))
                    totals['gp'] = totals.get('gp', 0) + 1
                    res_code = row.get('result', 'D')
                    if res_code == 'W':
                        totals['wins'] = totals.get('wins', 0) + 1
                    elif res_code == 'L':
                        totals['losses'] = totals.get('losses', 0) + 1
                    else:
                        totals['draws'] = totals.get('draws', 0) + 1
                    st_row = row.get('stats') or {}
                    for sk in ('pts', 'reb', 'ast', 'stl', 'blk', 'tov', 'pf'):
                        totals[sk] = totals.get(sk, 0) + (st_row.get(sk) or 0)
                    totals['abei'] = totals.get('abei', 0) + (row.get('abei') or 0)
                    game_entry = {
                        'matchId': mid,
                        'title': row.get('title') or m.get('title'),
                        'court': row.get('court') or m.get('court'),
                        'date': row.get('date') or m.get('datetime'),
                        'team': row.get('team'),
                        'result': res_code,
                        'stats': st_row,
                        'abei': row.get('abei', 0),
                        'teamAvg': row.get('teamAvg', 0),
                        'weight': row.get('weight', 1),
                        'delta': row.get('delta', 0),
                        'before': row.get('before', u_obj.get('rating', 1000)),
                        'after': row.get('after', u_obj.get('rating', 1000)),
                        'score': row.get('score') or {'A': m['result'].get('scoreA', 0), 'B': m['result'].get('scoreB', 0)},
                        'teamSize': row.get('teamSize', 5),
                    }
                    u_obj['rating'] = row.get('after', u_obj.get('rating', 1000))
                    u_obj['lastDelta'] = row.get('delta', 0)
                    u_obj['totals'] = totals
                    u_obj['games'] = [game_entry] + u_games[:39]
                    hist = list(u_obj.get('history') or [{'label': '가입', 'rating': 1000, 'delta': 0}])
                    hist.append({'label': '경기', 'rating': u_obj['rating'], 'delta': u_obj['lastDelta'], 'matchId': mid})
                    u_obj['history'] = hist
                    u_obj['rev'] = (u_obj.get('rev') or 0) + 1
                    u_obj['updatedAt'] = max(u_obj.get('updatedAt') or 0, m['result'].get('submittedAt') or 0)
                    users[uid] = u_obj

    # 매니저가 직접 개설한 매치가 항상 앞에 오도록 정렬
    seed_ids = {'m_1', 'm_2', 'm_3', 'm_4'}
    mgr_matches = [m for m in merged_matches if m.get('isManagerCreated') or m.get('id') not in seed_ids]
    mgr_matches.sort(key=lambda x: -(x.get('createdAt') or 0))
    std_matches = [m for m in merged_matches if not (m.get('isManagerCreated') or m.get('id') not in seed_ids)]
    merged_matches = mgr_matches + std_matches

    # 커스텀 체육관 병합
    courts_by_name = {}
    for c in (cur.get('customCourts') or []) + (inc.get('customCourts') or []):
        if isinstance(c, dict) and c.get('name'):
            courts_by_name[c['name']] = c
    custom_courts = list(courts_by_name.values())

    out = dict(cur, **inc)
    out['version'] = 2
    # 서버의 .gt_server_state.json은 공용 데이터만 보존하고, 개별 기기의 auth와 meId는 절대 강제 덮어쓰지 않음
    out['auth'] = None
    out['meId'] = None
    out['users'] = users
    out['matches'] = merged_matches
    out['customCourts'] = custom_courts
    out['deletedMatchIds'] = list(deleted_ids)
    out.pop('explicitLogout', None)
    return out


class GTRequestHandler(http.server.SimpleHTTPRequestHandler):
    # 3. PWA 및 모바일 브라우저용 명시적 MIME 타입 매핑
    extensions_map = {
        **http.server.SimpleHTTPRequestHandler.extensions_map,
        '.webmanifest': 'application/manifest+json; charset=utf-8',
        '.json': 'application/json; charset=utf-8',
        '.svg': 'image/svg+xml; charset=utf-8',
        '.js': 'application/javascript; charset=utf-8',
        '.html': 'text/html; charset=utf-8',
        '.css': 'text/css; charset=utf-8',
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=ROOT_DIR, **kwargs)

    def end_headers(self):
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, HEAD')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        super().end_headers()

    def _rewrite_clean_routes(self):
        """
        2. 편의 라우팅 처리:
        - '/' 접속 시 -> '/index.html' 서빙
        - '/admin' 또는 '/admin/' 접속 시 -> 404 없이 '/admin.html' 서빙
        """
        parts = urlsplit(self.path)
        clean_path = parts.path
        query_suffix = f'?{parts.query}' if parts.query else ''

        if clean_path in ('', '/'):
            self.path = '/index.html' + query_suffix
        elif clean_path in ('/admin', '/admin/'):
            self.path = '/admin.html' + query_suffix

    def do_OPTIONS(self):
        self.send_response(204)
        self.end_headers()

    def do_HEAD(self):
        self._rewrite_clean_routes()
        return super().do_HEAD()

    def do_GET(self):
        if self.path.startswith('/api/state'):
            with LOCK:
                st = load_server_state()
                if isinstance(st, dict):
                    st['auth'] = None
                    st['meId'] = None
            body = json.dumps({'ok': True, 'state': st}, ensure_ascii=False).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path in ('/health', '/healthz'):
            body = json.dumps({'ok': True, 'service': 'garbagetime'}).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self._rewrite_clean_routes()
        return super().do_GET()

    def do_POST(self):
        if self.path.startswith('/api/state'):
            length = int(self.headers.get('Content-Length', 0))
            raw = self.rfile.read(length).decode('utf-8') if length > 0 else '{}'
            try:
                payload = json.loads(raw)
            except Exception:
                payload = {}

            # 4. LOCK 보호 하에 서버 상태 로드 -> 병합 -> 원자적 저장 수행
            with LOCK:
                if payload.get('reset'):
                    if os.path.exists(STATE_FILE):
                        try:
                            os.remove(STATE_FILE)
                        except Exception:
                            pass
                    merged = payload.get('state')
                    if isinstance(merged, dict):
                        merged = dict(merged)
                        merged['auth'] = None
                        merged['meId'] = None
                        save_server_state(merged)
                else:
                    inc = payload.get('state') if 'state' in payload else payload
                    cur = load_server_state()
                    merged = merge_states(cur, inc)
                    if merged:
                        save_server_state(merged)

            body = json.dumps({'ok': True, 'state': merged}, ensure_ascii=False).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(404)
        self.end_headers()


class ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


if __name__ == '__main__':
    with ThreadingHTTPServer((HOST, PORT), GTRequestHandler) as httpd:
        print(f'GarbageTime Server running on http://{HOST}:{PORT} (Local: http://localhost:{PORT})')
        httpd.serve_forever()
