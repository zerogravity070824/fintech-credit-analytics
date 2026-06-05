# Rotate and Remove Exposed Secrets

If a service account key or other secret was accidentally committed, follow these steps immediately to secure your project.

1) Revoke or delete the exposed key in Google Cloud Console
   - Go to IAM & Admin → Service Accounts → select the service account → Keys → Delete the exposed key.

2) Create a new service account key (if needed) and store it securely outside the repo.

3) Remove the file from the repository working tree (already done) and prevent future commits by ensuring `.gitignore` contains `*.json`.

4) Purge the secret from the Git history (recommended). Two common tools:

- Using `git filter-repo` (preferred):

  ```powershell
  pip install git-filter-repo
  git clone --mirror <repo-url> repo-mirror.git
  cd repo-mirror.git
  git filter-repo --path potent-catwalk-484317-n0-1d7b5e1ab639.json --invert-paths
  git push --force
  ```

- Using BFG Repo-Cleaner:

  ```bash
  # download bfg.jar
  java -jar bfg.jar --delete-files "potent-catwalk-484317-n0-1d7b5e1ab639.json"
  git reflog expire --expire=now --all && git gc --prune=now --aggressive
  git push --force
  ```

5) Communicate the rotation to collaborators and update any CI secrets with the new key using GitHub Secrets (do NOT store key in repo).

6) Verify the repository no longer contains the secret:

```powershell
git grep -n "potent-catwalk-484317-n0-1d7b5e1ab639.json" || echo "No occurrences found"
```

If you want, I can prepare a small PR that removes the file (already removed locally) and adds this guidance to the README. If you need help running `git filter-repo` or BFG, tell me and I can provide exact commands for your setup.
