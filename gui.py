#!/usr/bin/env python3
"""
gui.py

Simple Flask-based GUI to control producer and consumer processes.

Features:
- Start/stop producer and consumer
- Toggle CSV logging and DRY_RUN mode for consumer
- Adjust producer `EVENTS_PER_SECOND`
- View basic statuses and recent logs

Run:
  python gui.py

The GUI manages processes by launching `python3 producer.py` and
`python3 consumer.py` as subprocesses and keeps their PIDs in `.pids/`.
"""
import os
import subprocess
import time
from pathlib import Path
from flask import Flask, render_template, request, redirect, url_for, jsonify

APP_DIR = Path(__file__).resolve().parent
PIDS_DIR = APP_DIR / ".pids"
PIDS_DIR.mkdir(exist_ok=True)

app = Flask(__name__)


def pid_path(name: str) -> Path:
    return PIDS_DIR / f"{name}.pid"


def read_pid(name: str):
    p = pid_path(name)
    if p.exists():
        try:
            return int(p.read_text().strip())
        except Exception:
            return None
    return None


def is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def start_process(name: str, env: dict = None):
    if read_pid(name) and is_running(read_pid(name)):
        return False, "already running"
    cmd = ["python3", str(APP_DIR / f"{name}.py")]
    env_vars = os.environ.copy()
    if env:
        env_vars.update(env)
    proc = subprocess.Popen(cmd, env=env_vars, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # give it a moment
    time.sleep(0.5)
    pid = proc.pid
    pid_path(name).write_text(str(pid))
    return True, pid


def stop_process(name: str):
    pid = read_pid(name)
    if not pid:
        return False, "not running"
    try:
        os.kill(pid, 2)  # SIGINT
        # wait up to 3s
        for _ in range(30):
            if not is_running(pid):
                break
            time.sleep(0.1)
        if is_running(pid):
            os.kill(pid, 15)  # SIGTERM
        pid_path(name).unlink(missing_ok=True)
        return True, "stopped"
    except Exception as e:
        return False, str(e)


@app.route('/')
def index():
    producer_pid = read_pid('producer')
    consumer_pid = read_pid('consumer')
    status = {
        'producer': producer_pid and is_running(producer_pid),
        'producer_pid': producer_pid,
        'consumer': consumer_pid and is_running(consumer_pid),
        'consumer_pid': consumer_pid,
        'csv_log_enabled': os.getenv('CSV_LOG_ENABLED', '1'),
        'dry_run': os.getenv('DRY_RUN', '0'),
        'events_per_second': os.getenv('EVENTS_PER_SECOND', '10'),
    }
    return render_template('index.html', status=status)


@app.route('/start/<name>', methods=['POST'])
def start(name):
    if name not in ('producer', 'consumer'):
        return jsonify({'ok': False, 'error': 'invalid name'}), 400
    env = {}
    # apply control options if provided
    if name == 'producer':
        eps = request.form.get('events_per_second') or os.getenv('EVENTS_PER_SECOND')
        if eps:
            env['EVENTS_PER_SECOND'] = str(eps)
    if name == 'consumer':
        # propagate CSV_LOG_ENABLED and DRY_RUN from form
        csv = request.form.get('csv_log_enabled')
        dry = request.form.get('dry_run')
        if csv is not None:
            env['CSV_LOG_ENABLED'] = str(int(bool(int(csv)))) if csv != '' else os.getenv('CSV_LOG_ENABLED', '1')
        if dry is not None:
            env['DRY_RUN'] = str(int(bool(int(dry))))
    ok, info = start_process(name, env=env)
    return jsonify({'ok': ok, 'info': info})


@app.route('/stop/<name>', methods=['POST'])
def stop(name):
    if name not in ('producer', 'consumer'):
        return jsonify({'ok': False, 'error': 'invalid name'}), 400
    ok, info = stop_process(name)
    return jsonify({'ok': ok, 'info': info})


@app.route('/toggle-csv', methods=['POST'])
def toggle_csv():
    cur = os.getenv('CSV_LOG_ENABLED', '1')
    new = '0' if cur != '0' else '1'
    # write to .env for persistence
    env_file = APP_DIR / '.env'
    lines = []
    if env_file.exists():
        lines = env_file.read_text().splitlines()
    found = False
    for i, l in enumerate(lines):
        if l.startswith('CSV_LOG_ENABLED='):
            lines[i] = f'CSV_LOG_ENABLED={new}'
            found = True
    if not found:
        lines.append(f'CSV_LOG_ENABLED={new}')
    env_file.write_text('\n'.join(lines) + '\n')
    return redirect(url_for('index'))


@app.route('/set-events', methods=['POST'])
def set_events():
    eps = request.form.get('events_per_second', '10')
    env_file = APP_DIR / '.env'
    lines = []
    if env_file.exists():
        lines = env_file.read_text().splitlines()
    found = False
    for i, l in enumerate(lines):
        if l.startswith('EVENTS_PER_SECOND='):
            lines[i] = f'EVENTS_PER_SECOND={eps}'
            found = True
    if not found:
        lines.append(f'EVENTS_PER_SECOND={eps}')
    env_file.write_text('\n'.join(lines) + '\n')
    return redirect(url_for('index'))


if __name__ == '__main__':
    # Do not use the reloader when running programmatically; it spawns child
    # processes which complicate background management. Run with debug=False
    app.run(host='0.0.0.0', port=5000, debug=False)
