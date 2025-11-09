# Basic test script for Windows PowerShell
python queuectl.py init-db
python queuectl.py enqueue '{"id":"job_test_ok","command":"python -c \"print(\\\"ok\\\")\""}'
python queuectl.py enqueue '{"id":"job_test_fail","command":"python -c \"import sys; sys.exit(1)\"","max_retries":2}'
Start-Process -NoNewWindow -FilePath "python" -ArgumentList "queuectl.py", "worker", "start", "--count", "2"
# Wait some seconds to let workers process
Start-Sleep -Seconds 8
python queuectl.py status
python queuectl.py dlq list
