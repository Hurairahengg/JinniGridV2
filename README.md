## Run Mother

cd main
pip install -r ../requirements.txt
uvicorn main:app --host 0.0.0.0 --port 5000

Then open http://localhost:5000

# Per-VM configs live in `main/configs/<worker_id>.json`.
# Edit via the dashboard Configs tab — saving auto-pushes to the worker if online.
# `auth_token` in each config file is the worker's bearer token.