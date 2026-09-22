#!/usr/bin/env python3
"""
SCRUM-6311_update_ateam_resource_descriptor_and_vocabulary.py  (one-off, reusable)

Idempotently ensure entries exist at the A-team curation API. Re-run whenever
new entries need adding: edit the data list(s) below and run again.

Currently handles ResourceDescriptorPage entries. For each (prefix, name, urlTemplate):
  * look up the parent ResourceDescriptor by prefix
  * if a page with that name already exists under it:
        - same urlTemplate  -> skip
        - different url      -> ALERT (do NOT modify)
  * if it does not exist    -> create it (only with --create; default is dry-run)

Vocabulary at A-team is deliberately NOT implemented here (the name is kept for
when/if we revisit it). SCRUM-6311 evaluated storing the person/lab controlled
vocabularies (lab_position, person-person relationship) in the A-team curation
vocabulary / vocabularyterm endpoints, but chose an ABC-local design instead:
A-team does not expose a stable identifier we can persist as the durable
reference, and we need to store an id on our own rows. So the terms live in
ABC-local tables (vocabulary_abc / vocabulary_term_abc / vocabulary_term_synonym_abc),
each row stores the vocabulary_term_abc id as an FK, and they are served
source-opaquely via GET /vocabulary/{name}. The check-then-create pattern below
would extend cleanly to the A-team vocabulary/vocabularyterm endpoints if that
decision is ever revisited — which is why this script keeps its original name.

Target is chosen by --env-file (default .env.devserver_4002 -> beta). Point it at a
prod env file to run on prod after UAT. Auth uses a Cognito admin (client_credentials)
token via agr_cognito_py, exactly like the ABC app.

Usage:
  python SCRUM-6311_update_ateam_resource_descriptor_and_vocabulary.py              # dry-run (beta)
  python SCRUM-6311_update_ateam_resource_descriptor_and_vocabulary.py --create     # create
  python SCRUM-6311_update_ateam_resource_descriptor_and_vocabulary.py --env-file <f>
"""
import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request

# --- pages to ensure exist: (prefix, name, urlTemplate, pageDescription|None) ---
PAGES = [
    ("ZFIN", "laboratory", "https://zfin.org/[%s]", None),
    ("ZFIN", "person", "https://zfin.org/[%s]", None),
    ("Xenbase", "laboratory", "https://www.xenbase.org/entry/[%s]", None),
    ("Xenbase", "person", "https://www.xenbase.org/entry/[%s]", None),
    ("WB", "laboratory", "https://www.wormbase.org/db/get?name=[%s];class=Laboratory", None),
    ("WB", "person", "https://www.wormbase.org/db/get?name=[%s];class=Person", None),
]


def load_env(path):
    if not os.path.isfile(path):
        sys.exit(f"env file not found: {path}")
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip()
            v = v.strip().strip("'\"")
            if re.match(r"^[A-Z0-9_]+$", k):
                os.environ.setdefault(k, v)


def _post(url, headers, body):
    data = json.dumps(body).encode() if body is not None else b"{}"
    req = urllib.request.Request(url, data=data, method="POST")
    for k, v in headers.items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", default=".env.devserver_4002")
    ap.add_argument("--create", action="store_true",
                    help="actually create missing pages (default: dry-run)")
    args = ap.parse_args()

    load_env(args.env_file)
    base = os.environ["ATEAM_API_URL"].rstrip("/")
    from agr_cognito_py import get_authentication_token, generate_headers
    headers = {**generate_headers(get_authentication_token()),
               "Content-Type": "application/json"}
    mode = "CREATE" if args.create else "DRY-RUN"
    print(f"target : {base}\nenvfile: {args.env_file}\nmode   : {mode}\n")

    # one call returns every descriptor with its id and resourcePages
    url = f"{base}/resourcedescriptor/findForPublic?limit=5000&page=0&view=ResourceDescriptorView"
    results = _post(url, headers, {}).get("results", [])
    by_prefix = {d.get("prefix"): d for d in results}

    created = skipped = alerts = errors = 0
    for prefix, name, url_tmpl, desc in PAGES:
        d = by_prefix.get(prefix)
        if not d:
            print(f"  ERROR   {prefix}/{name}: no resource descriptor with prefix {prefix!r}")
            errors += 1
            continue
        pages = {p.get("name"): p for p in (d.get("resourcePages") or [])}
        existing = pages.get(name)
        if existing:
            if existing.get("urlTemplate") == url_tmpl:
                print(f"  EXISTS  {prefix}/{name}: url matches -> skip")
                skipped += 1
            else:
                print(f"  ALERT   {prefix}/{name}: exists with a DIFFERENT url -> NOT modifying")
                print(f"            have: {existing.get('urlTemplate')}")
                print(f"            want: {url_tmpl}")
                alerts += 1
            continue
        body = {"name": name, "urlTemplate": url_tmpl, "internal": False,
                "obsolete": False, "resourceDescriptor": {"id": d["id"]}}
        if desc:
            body["pageDescription"] = desc
        if not args.create:
            print(f"  WOULD-CREATE {prefix}/{name} -> {url_tmpl}")
            continue
        try:
            resp = _post(f"{base}/resourcedescriptorpage", headers, body)
            new_id = (resp.get("entity") or {}).get("id") if isinstance(resp, dict) else None
            print(f"  CREATED {prefix}/{name} -> id={new_id or resp}")
            created += 1
        except urllib.error.HTTPError as e:
            print(f"  ERROR   {prefix}/{name}: HTTP {e.code}: {e.read().decode()[:400]}")
            errors += 1

    print(f"\nsummary: created={created} skipped={skipped} alerts={alerts} "
          f"errors={errors}  (mode={mode})")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
