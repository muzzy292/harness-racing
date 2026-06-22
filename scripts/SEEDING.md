# One-time database seeding

The pipeline keeps its SQLite database in a **private** repo
(`harness-racing-data`) as a release asset, so it is never publicly
downloadable. Before the first automated run, the *current* database has to be
uploaded there once. After that, the daily workflow keeps it updated
automatically — you never touch this again.

## What to upload
A bundle has already been built for you at the repo root:

    harness-data.tar.gz   (~11 MB — contains data/harness.db + data/track_pars.json)

## Steps (GitHub web UI — no command line needed)
1. Go to your **private** repo: `https://github.com/muzzy292/harness-racing-data`
2. On the right-hand side, click **Releases** → **Create a new release**
   (or **Draft a new release**).
3. In **Choose a tag**, type exactly:

       db-latest

   and click **Create new tag: db-latest on publish**.
4. **Release title:** anything, e.g. `Latest database`.
5. Drag **`harness-data.tar.gz`** (from this project folder) into the
   **Attach binaries** box and wait for the upload to finish.
6. Click **Publish release**.

That's it. The daily workflow's restore step downloads this asset at the start
of each run, and the backup step overwrites it (plus dated snapshots under a
`db-backups` release) at the end.

## Notes
- The `harness-data.tar.gz` file is gitignored — it will never be committed to
  the public repo.
- You can delete the local `harness-data.tar.gz` after uploading; it's only the
  seed copy.
- If you ever need to reset, rebuild the bundle with:

      tar -czf harness-data.tar.gz data/harness.db data/track_pars.json

  and re-upload it to the `db-latest` release (replacing the existing asset).
