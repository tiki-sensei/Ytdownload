# server.py
from flask import Flask, request, jsonify, Response, send_from_directory, abort
import tempfile, os, time, requests, logging, re, zipfile, subprocess, urllib.parse, uuid, glob, shutil, threading, json
from yt_dlp import YoutubeDL

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
app.logger.setLevel(logging.INFO)

BASE_TMP = os.path.join(tempfile.gettempdir(), "yt_server_files")
os.makedirs(BASE_TMP, exist_ok=True)
os.environ["PATH"] += os.pathsep + os.path.dirname(os.path.abspath(__file__))

# simple in-memory job store
jobs = {}  # job_id -> dict with fields: status, percent, stage, filename, filepath, error, log (list)
jobs_lock = threading.Lock()

# ---------------------- utilities ----------------------
def build_download_header(filename):
    quoted = urllib.parse.quote(filename)
    return {"Content-Disposition": f"attachment; filename*=UTF-8''{quoted}"}

def safe_filename(name):
    if not name:
        return "unknown"
    s = re.sub(r'[\\/*?:"<>|]', "_", name)
    return s[:200].strip()

def safe_replace(src, dst, retries=12, wait=0.5):
    last_exc = None
    for _ in range(retries):
        try:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            if os.path.exists(dst):
                try: os.remove(dst)
                except: pass
            os.replace(src, dst)
            return
        except PermissionError as e:
            last_exc = e
            time.sleep(wait)
    raise last_exc or RuntimeError("safe_replace 실패")

def stream_with_progress(path, filename, mimetype):
    if not os.path.exists(path):
        abort(404, "file not found")
    def generate():
        with open(path, 'rb') as f:
            while chunk := f.read(8192):
                yield chunk
    headers = build_download_header(filename)
    return Response(generate(), headers=headers, mimetype=mimetype)

# ---------------------- yt-dlp info opts ----------------------
YTDL_INFO_OPTS = {
    'skip_download': True,
    'quiet': True,
    'no_warnings': True,
    'format': 'best',
    'extractor_args': {'youtube': {'player_client': ['default']}}
}

def extract_meta(url):
    with YoutubeDL(YTDL_INFO_OPTS) as ydl:
        info = ydl.extract_info(url, download=False)
        dur = info.get('duration')
        dur_str = f"{dur//60:02d}:{dur%60:02d}" if dur else None
        return {
            'title': info.get('title'),
            'uploader': info.get('uploader'),
            'duration': dur_str,
            'thumbnail': info.get('thumbnail'),
        }

# ---------------------- job helpers ----------------------
def job_init(job_id):
    with jobs_lock:
        jobs[job_id] = {
            'status': 'queued',
            'percent': 0,
            'stage': 'queued',
            'filename': None,
            'filepath': None,
            'error': None,
            'log': []
        }

def job_log(job_id, message):
    with jobs_lock:
        job = jobs.get(job_id)
        if job:
            job['log'].insert(0, f"[{time.strftime('%H:%M:%S')}] {message}")
            # cap log length
            if len(job['log']) > 200:
                job['log'] = job['log'][:200]

def job_update(job_id, **kwargs):
    with jobs_lock:
        job = jobs.get(job_id)
        if not job: return
        for k,v in kwargs.items():
            job[k] = v

# ---------------------- download worker ----------------------
def download_worker(job_id, url, fmt, thumb):
    """
    runs in background thread, updates jobs[job_id] with progress and final file path
    """
    try:
        job_update(job_id, status='running', stage='meta', percent=0)
        job_log(job_id, f"작업 시작: {fmt} / {url}")

        # get metadata first
        with YoutubeDL(YTDL_INFO_OPTS) as ydl:
            info = ydl.extract_info(url, download=False)

        title_safe = safe_filename(info.get('title', 'unknown'))
        output_ext = 'mp3' if fmt == 'mp3' else 'mkv'
        final_file = os.path.join(BASE_TMP, f"{title_safe}.{output_ext}")
        job_update(job_id, filename=f"{title_safe}.{output_ext}")

        tmp_dir = os.path.join("C:/", "temp", "yt")
        os.makedirs(tmp_dir, exist_ok=True)
        rand = uuid.uuid4().hex[:8]
        tmp_prefix = os.path.join(tmp_dir, f"{title_safe}_{rand}_tmp")

        # progress hook for yt-dlp
        def ytdl_hook(d):
            try:
                if d.get('status') == 'downloading':
                    downloaded = d.get('downloaded_bytes') or d.get('downloaded_bytes', 0)
                    total = d.get('total_bytes') or d.get('total_bytes_estimate') or 0
                    pct = 0
                    if total:
                        pct = int(downloaded * 100 / total)
                    # update job
                    job_update(job_id, stage='downloading', percent=pct)
                elif d.get('status') == 'finished':
                    job_update(job_id, stage='downloaded', percent=100)
                    job_log(job_id, f"삭제된 조각 합침/완료: {d.get('filename')}")
            except Exception as e:
                job_log(job_id, f"hook error: {e}")

        # ---------------- MP3 ----------------
        if fmt == 'mp3':
            ydl_opts = {
                "format": "bestaudio/best",
                "outtmpl": tmp_prefix + ".%(ext)s",
                "quiet": True,
                "nopart": True,
                "overwrites": True,
                "windowsfilenames": True,
                "progress_hooks": [ytdl_hook],
                "extractor_args": {"youtube": {"player_client": ["default"]}},
            }
            job_log(job_id, "오디오 다운로드 시작")
            with YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])

            # find source
            candidates = glob.glob(tmp_prefix + ".*")
            src = next((c for c in candidates if not c.endswith(".mp3")), None)
            if not src or not os.path.exists(src):
                raise RuntimeError("오디오 파일이 생성되지 않았습니다.")

            mp3_temp = tmp_prefix + ".mp3"
            job_update(job_id, stage='converting', percent=0)
            job_log(job_id, "ffmpeg 변환 시작 (MP3 최고음질)")
            # use -q:a 0 for best
            cmd = [
                "ffmpeg", "-y",
                "-i", src,
                "-vn",
                "-acodec", "libmp3lame",
                "-q:a", "0",
                mp3_temp
            ]
            # run and wait
            subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if not os.path.exists(mp3_temp):
                raise RuntimeError("ffmpeg 변환 실패")
            try: os.remove(src)
            except: pass
            safe_replace(mp3_temp, final_file)
            job_update(job_id, stage='done', percent=100, filepath=final_file)
            job_log(job_id, "오디오 변환 완료")

        # ---------------- VIDEO ----------------
        else:
            # download video (raw) and audio (raw) separately
            v_opts = {
                "format": "bestvideo",
                "outtmpl": tmp_prefix + "_v.%(ext)s",
                "quiet": True,
                "nopart": True,
                "windowsfilenames": True,
                "postprocessors": [],
                "progress_hooks": [ytdl_hook],
                "extractor_args": {"youtube": {"player_client": ["default"]}},
            }

            a_opts = {
                "format": "bestaudio",
                "outtmpl": tmp_prefix + "_a.%(ext)s",
                "quiet": True,
                "nopart": True,
                "windowsfilenames": True,
                "postprocessors": [],
                "progress_hooks": [ytdl_hook],
                "extractor_args": {"youtube": {"player_client": ["default"]}},
            }

            job_log(job_id, "비디오 다운로드 시작 (비디오 트랙)")
            with YoutubeDL(v_opts) as ydl:
                ydl.extract_info(url, download=True)

            job_log(job_id, "비디오 다운로드 완료, 오디오 트랙 다운로드 시작")
            with YoutubeDL(a_opts) as ydl:
                ydl.extract_info(url, download=True)

            # find vfile and afile
            vfile = afile = None
            for f in os.listdir(tmp_dir):
                if f.startswith(os.path.basename(tmp_prefix) + "_v") and not f.endswith(".part"):
                    vfile = os.path.join(tmp_dir, f)
                if f.startswith(os.path.basename(tmp_prefix) + "_a") and not f.endswith(".part"):
                    afile = os.path.join(tmp_dir, f)

            if not vfile or not afile:
                raise RuntimeError("비디오 또는 오디오 파일을 찾을 수 없습니다.")

            # merging (we'll mark stage merging; percent 0->100 instantly after done)
            job_update(job_id, stage='merging', percent=0)
            job_log(job_id, "ffmpeg 병합 시작 (무손실 copy)")
            cmd = [
                "ffmpeg", "-y",
                "-i", vfile,
                "-i", afile,
                "-c:v", "copy",
                "-c:a", "copy",
                final_file
            ]
            subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if not os.path.exists(final_file):
                raise RuntimeError("ffmpeg 병합 실패")
            try: os.remove(vfile)
            except: pass
            try: os.remove(afile)
            except: pass
            job_update(job_id, stage='done', percent=100, filepath=final_file)
            job_log(job_id, "비디오 병합 완료")

        # thumbnail zip handling (optional)
        if thumb:
            zip_path = os.path.join(BASE_TMP, f"{title_safe}.zip")
            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.write(final_file, os.path.basename(final_file))
                turl = info.get("thumbnail")
                if turl:
                    try:
                        r = requests.get(turl, timeout=10)
                        if r.ok:
                            tfile = os.path.join(BASE_TMP, f"{title_safe}_thumb.jpg")
                            with open(tfile, "wb") as f:
                                f.write(r.content)
                            zf.write(tfile, os.path.basename(tfile))
                            os.remove(tfile)
                    except Exception as e:
                        job_log(job_id, f"썸네일 실패: {e}")
            # replace final to zip
            safe_replace(zip_path, final_file)
            job_update(job_id, filename=f"{title_safe}.zip")

    except Exception as e:
        job_update(job_id, status='error', error=str(e))
        job_log(job_id, f"오류: {e}")
        app.logger.exception("download_worker error")
    else:
        job_update(job_id, status='finished')
        job_log(job_id, "작업 완료")
    finally:
        # ensure percent at least 100 when finished
        with jobs_lock:
            j = jobs.get(job_id)
            if j and j.get('stage') == 'done' and j.get('percent',0) < 100:
                j['percent'] = 100

# ---------------------- Flask endpoints ----------------------
@app.route('/')
def index():
    app.logger.info("index.html 요청됨")
    return send_from_directory('.', 'index.html')

@app.route('/info', methods=['POST'])
def info_route():
    url = (request.get_json() or {}).get('url')
    if not url:
        return jsonify({'error': 'no url'}), 400
    try:
        meta = extract_meta(url)
        return jsonify(meta)
    except Exception as e:
        app.logger.exception("메타 정보 추출 중 오류 발생")
        return jsonify({'error': str(e)}), 500

@app.route('/download', methods=['POST'])
def start_download():
    data = request.get_json() or {}
    url = data.get('url')
    fmt = data.get('format', 'mp3')
    thumb = data.get('thumb', False)
    if not url:
        return jsonify({'error': 'url required'}), 400

    job_id = uuid.uuid4().hex
    job_init(job_id)
    # start background thread
    t = threading.Thread(target=download_worker, args=(job_id, url, fmt, thumb), daemon=True)
    t.start()
    return jsonify({'id': job_id}), 202

@app.route('/progress/<job_id>')
def progress_sse(job_id):
    if job_id not in jobs:
        return abort(404)
    def generator():
        last_state = None
        # SSE headers handled by Response
        while True:
            with jobs_lock:
                state = dict(jobs.get(job_id, {}))
            # only send if changed (or periodically)
            payload = {
                'status': state.get('status'),
                'stage': state.get('stage'),
                'percent': state.get('percent', 0),
                'filename': state.get('filename'),
                'error': state.get('error'),
                'log': state.get('log', [])[:20]
            }
            data = json.dumps(payload, ensure_ascii=False)
            if data != last_state:
                last_state = data
                yield f"data: {data}\n\n"
            # if finished or error, then close after sending final
            if state.get('status') in ('finished','error'):
                break
            time.sleep(0.5)
    return Response(generator(), mimetype='text/event-stream')

@app.route('/file/<job_id>')
def file_get(job_id):
    j = jobs.get(job_id)
    if not j:
        return abort(404)
    if j.get('status') != 'finished':
        return abort(404, "file not ready")
    filepath = j.get('filepath')
    filename = j.get('filename')
    if not filepath or not os.path.exists(filepath):
        return abort(404)
    mimetype = "audio/mpeg" if filepath.endswith(".mp3") else "video/x-matroska"
    return stream_with_progress(filepath, filename, mimetype)

# cleanup old files periodically (simple)
def cleanup_thread():
    while True:
        now = time.time()
        try:
            for f in os.listdir(BASE_TMP):
                fp = os.path.join(BASE_TMP, f)
                try:
                    if os.path.isfile(fp) and now - os.path.getmtime(fp) > 3600:
                        os.remove(fp)
                except: pass
            tmp_dir = os.path.join("C:/", "temp", "yt")
            if os.path.isdir(tmp_dir):
                for f in os.listdir(tmp_dir):
                    fp = os.path.join(tmp_dir, f)
                    try:
                        if os.path.isfile(fp) and now - os.path.getmtime(fp) > 3600:
                            os.remove(fp)
                    except: pass
        except:
            pass
        time.sleep(600)

# start cleanup worker
t_cleanup = threading.Thread(target=cleanup_thread, daemon=True)
t_cleanup.start()

if __name__ == "__main__":
    app.logger.info("서버 시작")
    app.run(host="0.0.0.0", port=52033, threaded=True)
