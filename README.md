## Run Mother

cd main
pip install -r ../requirements.txt
uvicorn main:app --host 0.0.0.0 --port 5000

Then open http://localhost:5000

# Per-VM configs live in `main/configs/<worker_id>.json`.
# Edit via the dashboard Configs tab — saving auto-pushes to the worker if online.
# `auth_token` in each config file is the worker's bearer token.## Run a VM Worker

1. Provision a Windows VM with MT5 installed + logged into your broker
2. Copy the `vm/` folder onto the VM
3. Edit `vm/config.json`:
   • set `worker_id` (must match a file in `main/configs/<worker_id>.json` on Mother)
   • set `mother_url` (e.g. `wss://grid.yourdomain.com/ws/worker/vm1`)
   • set `token` (must match `auth_token` in Mother's config for this worker)
4. `pip install -r requirements.txt`
5. Run:   `python vm/main.py`

### Install as a Windows service (auto-restart on crash/reboot):

   nssm install JinniWorker "C:\Python311\python.exe" "C:\jinni-grid\vm\main.py"
   nssm set JinniWorker AppDirectory "C:\jinni-grid\vm"
   nssm set JinniWorker AppStdout "C:\jinni-grid\vm\service.out.log"
   nssm set JinniWorker AppStderr "C:\jinni-grid\vm\service.err.log"
   nssm start JinniWorker

### What happens on boot
- If `auto_start: true` and `fallback_config` is set → strategy starts immediately, even if Mother is unreachable.
- When Mother becomes reachable, worker connects and Mother's config wins (strategy restarts with new config if needed).
- All events accumulate locally during Mother outage (cap 1000); flushed on reconnect.