import argparse, json, os
import re, sys
from datetime import datetime, timezone
from dlq import DLQManager


from storage import JobStorage
from worker import WorkerManager
from config import Config

DB_PATH = os.path.join(os.path.dirname(__file__), "queue.db")
STOP_FLAG = os.path.join(os.path.dirname(__file__), "workers.stop")


def iso_now():
    return datetime.now(timezone.utc).isoformat()


def smart_parse_job(raw: str) -> dict:
    raw = raw.strip()
    try:
        return json.loads(raw)
    except Exception:
        pass

    try:
        fixed = raw.replace("'", '"')
        return json.loads(fixed)
    except Exception:
        pass

    data = {}
    id_match = re.search(r'"?id"?\s*:\s*"?([\w\-]+)"?', raw)
    cmd_match = re.search(r'"?command"?\s*:\s*"?(.*?)"?(?:,|\})', raw)
    maxr_match = re.search(r'"?max_retries"?\s*:\s*(\d+)', raw)
    if id_match:
        data["id"] = id_match.group(1)
    if cmd_match:
        data["command"] = cmd_match.group(1).strip()
    if maxr_match:
        data["max_retries"] = int(maxr_match.group(1))

    if not data.get("id") or not data.get("command"):
        raise ValueError("Unable to parse job input. Please check syntax.")

    return data


def cmd_enqueue(args):
    raw = args.json
    try:
        job = smart_parse_job(raw)
    except Exception as e:
        print("Invalid job input:", e)
        return 2

    job.setdefault("state", "pending")
    job.setdefault("attempts", 0)
    job.setdefault("max_retries", Config().get("max_retries"))
    now = iso_now()
    job.setdefault("created_at", now)
    job.setdefault("updated_at", now)

    storage = JobStorage(DB_PATH)
    try:
        storage.add_job(job)
        print(f"Enqueued job: {job.get('id')} | Command: {job.get('command')}")
        return 0
    except Exception as e:
        print("Failed to enqueue:", e)
        return 3


def cmd_status(args):
    storage = JobStorage(DB_PATH)
    stats = storage.counts_by_state()
    workers_running = os.path.exists(STOP_FLAG) and "stopping file present" or "no stop file"
    print("Job counts:")
    for s, c in stats.items():
        print(f"  {s:10s}: {c}")
    print("Workers stop-flag:", workers_running)
    return 0


def cmd_list(args):
    storage = JobStorage(DB_PATH)
    rows = storage.list_jobs(state=args.state, limit=args.limit)
    for r in rows:
        print(json.dumps(r, default=str))
    return 0


def cmd_dlq_list(args):
    dlq = DLQManager(DB_PATH)
    dlq.print_dlq(limit=args.limit)
    return 0


def cmd_dlq_retry(args):
    dlq = DLQManager(DB_PATH)
    ok = dlq.retry_job(args.job_id)
    if ok:
        print("Job moved back to pending:", args.job_id)
        return 0
    else:
        print("Could not retry job:", args.job_id)
        return 4


def cmd_config(args):
    cfg = Config()
    if args.set:
        key, val = args.set
        try:
            v = int(val)
        except:
            try:
                v = float(val)
            except:
                if val.lower() in ("true", "false"):
                    v = val.lower() == "true"
                else:
                    v = val
        cfg.set(key, v)
        print(f"✅ Set {key} = {v}")
        return 0
    else:
        for k, v in cfg.all().items():
            print(f"{k}: {v}")
        return 0


def cmd_worker_start(args):
    count = args.count or 1
    print(f"Starting {count} worker(s). Press Ctrl+C to stop or run 'python queuectl.py worker stop'")
    try:
        if os.path.exists(STOP_FLAG):
            os.remove(STOP_FLAG)
    except:
        pass
    mgr = WorkerManager(DB_PATH, count=count)
    try:
        mgr.run_forever()
    except KeyboardInterrupt:
        print("Stopping workers (KeyboardInterrupt). Waiting graceful shutdown...")
        mgr.stop()
    return 0


def cmd_worker_stop(args):
    open(STOP_FLAG, "w").write("stop")
    print("Stop flag created. Running workers will finish current job and stop.")
    return 0


def cmd_init(args):
    storage = JobStorage(DB_PATH)
    storage.init_db()
    print("Initialized DB at", DB_PATH)
    return 0


def main():
    parser = argparse.ArgumentParser(prog="queuectl")
    sub = parser.add_subparsers(dest="cmd")

    p_enqueue = sub.add_parser("enqueue")
    p_enqueue.add_argument("json", help="Job JSON string")
    p_enqueue.set_defaults(func=cmd_enqueue)

    p_status = sub.add_parser("status")
    p_status.set_defaults(func=cmd_status)

    p_list = sub.add_parser("list")
    p_list.add_argument("--state", default=None)
    p_list.add_argument("--limit", type=int, default=100)
    p_list.set_defaults(func=cmd_list)

    p_dlq = sub.add_parser("dlq")
    dlq_sub = p_dlq.add_subparsers(dest="dlq_cmd")
    dlq_list = dlq_sub.add_parser("list")
    dlq_list.add_argument("--limit", type=int, default=100)
    dlq_list.set_defaults(func=cmd_dlq_list)
    dlq_retry = dlq_sub.add_parser("retry")
    dlq_retry.add_argument("job_id")
    dlq_retry.set_defaults(func=cmd_dlq_retry)

    p_worker = sub.add_parser("worker")
    wsub = p_worker.add_subparsers(dest="worker_cmd")
    w_start = wsub.add_parser("start")
    w_start.add_argument("--count", type=int, default=1)
    w_start.set_defaults(func=cmd_worker_start)
    w_stop = wsub.add_parser("stop")
    w_stop.set_defaults(func=cmd_worker_stop)

    p_config = sub.add_parser("config")
    p_config.add_argument("set", nargs="*", help="set key value", default=None)
    p_config.add_argument("--get", action="store_true", help="show config")
    p_config.set_defaults(func=lambda a: cmd_config(a) if a.set else cmd_config(a))

    p_init = sub.add_parser("init-db")
    p_init.set_defaults(func=cmd_init)

    args_raw = sys.argv[1:]
    if len(args_raw) >= 2 and args_raw[0] == "config" and args_raw[1] == "set":
        args = argparse.Namespace(set=(args_raw[2], args_raw[3]) if len(args_raw) >= 4 else None)
        return cmd_config(args)

    args = parser.parse_args()
    if not hasattr(args, "func") or args.func is None:
        parser.print_help()
        return 1
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
