---
title: Running Spark Tests in GitHub Actions
rank: 80
---

# Running Spark Tests in GitHub Actions

The Spark tests are triggered in GitHub Actions for pull requests in any of the following events:

- The pull request is opened.
- A commit is pushed with a commit message containing `[spark tests]` (case-insensitive).
- A commit is pushed and the pull request has the label **`run spark tests`**.


The Spark tests are always run when the pull request is merged into the `main` branch.
