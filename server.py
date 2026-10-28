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


def merge_match_objects(m_c, m_i):
    """
    두 매치 상태를 병합하되, updatedAt 타임스탬프 차이로 인해 방금 신청한 참가자 ID가
    누락되는 일이 없도록 취소(cancelledIds) 또는 매니저 강제 제외(removedIds)되지 않은
    모든 참가자 ID를 합집합으로 안전하게 보존합니다.
    """
    t_c = m_c.get('updatedAt') or m_c.get('createdAt') or 0
    t_i = m_i.get('updatedAt') or m_i.get('createdAt') or 0
    newer = m_i if t_i >= t_c else m_c
    older = m_c if t_i >= t_c else m_i

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

    merged_participants = []
    candidate_pool = older_participants + newer_participants + [
        uid for uid, a_ts in applied_at.items() if a_ts > cancelled_at.get(uid, 0)
    ]

    for pid in candidate_pool:
        if not pid or pid in merged_participants:
            continue
        if pid not in cancelled_set and pid not in removed_set:
            merged_participants.append(pid)

    merged = dict(older, **newer)
    merged['participants'] = merged_participants
    merged['cancelledIds'] = list(cancelled_set)
    merged['removedIds'] = list(removed_set)
    merged['appliedAt'] = applied_at
    merged['cancelledAt'] = cancelled_at
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

    # 매치 병합: ID 기준으로 최신 updatedAt / status 반영 + 멀티 유저 참가 신청자 합집합 보존
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
            merged_matches.append(merge_match_objects(m_c, m_i))
        elif m_i:
            merged_matches.append(m_i)
        elif m_c:
            merged_matches.append(m_c)

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

    # 멀티 유저 병합: 각 스마트폰에서 가입한 모든 지인들의 프로필, 포지션 조정, 스탯 기록을 충돌 없이 합침
    users = dict(cur.get('users') or {})
    for uid, u in (inc.get('users') or {}).items():
        if uid in users and isinstance(users[uid], dict) and isinstance(u, dict):
            gp_c = (users[uid].get('totals') or {}).get('gp', 0)
            gp_i = (u.get('totals') or {}).get('gp', 0)
            u_c_t = users[uid].get('updatedAt') or users[uid].get('joinedAt') or 0
            u_i_t = u.get('updatedAt') or u.get('joinedAt') or 0
            if gp_i > gp_c or u_i_t >= u_c_t:
                users[uid] = dict(users[uid], **u)
            else:
                users[uid] = dict(u, **users[uid])
        else:
            users[uid] = u

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
