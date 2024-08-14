---
title: Running Spark Tests in GitHub Actions
rank: 80
---

# Running Spark Tests in GitHub Actions

The Spark tests are triggered in GitHub Actions for pull requests,
either when the pull request is opened or when the commit message contains `[spark tests]` (case-insensitive).

The Spark tests are always run when the pull request is merged into the `main` branch.
