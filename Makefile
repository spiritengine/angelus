# Angelus release path -- the one blessed deploy entrypoint (brief-20260613-3spy).
#
# Operators and agents type `make deploy`, never pip/systemctl/sqlite directly.
# These are thin wrappers over deploy/deploy.py (stdlib-only; imports nothing
# from the angelus package, so it works even when the install is broken). Run
# from the engine repo root; the daemon's own working directory is the lodging
# root (see CLAUDE.md), which deploy.py resolves via env.
#
#   make deploy                 # deploy master HEAD
#   make deploy REF=<sha>       # deploy (or roll back to) an explicit ref
#   make deploy REF=<sha> RESTORE_BACKUP=1
#                               # roll back past applied migrations (restores
#                               # the freshest pre-deploy backup first)
#   make deploy-check           # preflight only; prod untouched
#   make deploy-status          # installed version vs master + recent deploys
#
# A deploy that died anywhere is fixed by rerunning the same `make deploy REF=`
# (every step is idempotent). Override touchpoints via ANGELUS_DEPLOY_* env
# (systemctl binary/scope, unit, lodging root, dev repo, pip python/prefix).

PYTHON ?= python3
DEPLOY := $(PYTHON) deploy/deploy.py
REF    ?= master

.PHONY: deploy deploy-check deploy-status

deploy:
	$(DEPLOY) deploy $(REF) $(if $(RESTORE_BACKUP),--restore-backup,)

deploy-check:
	$(DEPLOY) check $(REF)

deploy-status:
	$(DEPLOY) status
