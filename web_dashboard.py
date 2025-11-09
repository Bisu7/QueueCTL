from flask import Flask, jsonify, render_template_string, request
from storage import JobStorage
import os

app = Flask(__name__)
DB_PATH = os.path.join(os.path.dirname(__file__), "queue.db")


@app.route("/")
def home():
    s = JobStorage(DB_PATH)
    counts = s.counts_by_state()
    state_filter = request.args.get("state")
    jobs = s.list_jobs(state=state_filter, limit=50)

    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>QueueCTL Dashboard</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
        <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
        <style>
            body { background-color: #f8f9fa; font-family: 'Segoe UI', sans-serif; }
            h1 { margin-top: 20px; }
            .badge-pending { background-color: #ffc107; }
            .badge-processing { background-color: #17a2b8; }
            .badge-completed { background-color: #28a745; }
            .badge-failed { background-color: #dc3545; }
            .badge-dead { background-color: #6c757d; }
            .refresh-info { font-size: 0.9rem; color: gray; }
        </style>
    </head>
    <body>
    <div class="container mt-4">
        <div class="d-flex justify-content-between align-items-center">
            <h1>🧩 QueueCTL Dashboard</h1>
            <button id="refresh-btn" class="btn btn-outline-primary btn-sm">🔄 Refresh</button>
        </div>
        <p class="refresh-info">Auto-refresh every 5 seconds | Click column headers to filter by state</p>

        <div class="row mt-4">
            {% for k,v in counts.items() %}
            <div class="col-md-2 mb-3">
                <div class="card text-center shadow-sm">
                    <div class="card-body">
                        <h6 class="card-title text-uppercase">{{k}}</h6>
                        <h3>{{v}}</h3>
                    </div>
                </div>
            </div>
            {% endfor %}
        </div>

        <div class="mt-4">
            <h4>Recent Jobs{% if state_filter %} (Filtered: {{state_filter}}){% endif %}</h4>
            <table class="table table-hover table-bordered shadow-sm bg-white">
                <thead class="table-light">
                    <tr>
                        <th>ID</th>
                        <th>Command</th>
                        <th>State</th>
                        <th>Attempts</th>
                        <th>Max Retries</th>
                        <th>Updated At</th>
                        <th>Last Error</th>
                    </tr>
                </thead>
                <tbody>
                    {% for j in jobs %}
                    <tr>
                        <td>{{j['id']}}</td>
                        <td>{{j['command']}}</td>
                        <td>
                            <span class="badge badge-{{j['state']}} text-light px-2 py-1">{{j['state']}}</span>
                        </td>
                        <td>{{j['attempts']}}</td>
                        <td>{{j['max_retries']}}</td>
                        <td>{{j['updated_at']}}</td>
                        <td style="max-width:300px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">
                            {{j.get('last_error','')}}
                        </td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>

        <div class="mt-4 text-center">
            <a href="/" class="btn btn-outline-secondary btn-sm">All</a>
            <a href="/?state=pending" class="btn btn-outline-warning btn-sm">Pending</a>
            <a href="/?state=processing" class="btn btn-outline-info btn-sm">Processing</a>
            <a href="/?state=completed" class="btn btn-outline-success btn-sm">Completed</a>
            <a href="/?state=failed" class="btn btn-outline-danger btn-sm">Failed</a>
            <a href="/?state=dead" class="btn btn-outline-dark btn-sm">DLQ</a>
        </div>
    </div>

    <script>
    function refreshPage() {
        location.reload();
    }
    $("#refresh-btn").click(refreshPage);
    setInterval(refreshPage, 5000);
    </script>
    </body>
    </html>
    """

    return render_template_string(html, counts=counts, jobs=jobs, state_filter=state_filter)


@app.route("/api/jobs")
def api_jobs():
    s = JobStorage(DB_PATH)
    return jsonify(s.list_jobs(limit=50))


if __name__ == "__main__":
    print("Starting QueueCTL Dashboard at http://localhost:5000")
    app.run(port=5000, debug=False)
