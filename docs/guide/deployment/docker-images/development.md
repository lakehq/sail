---
title: Development
rank: 3
---

# Development

If you make changes to Sail source code locally, you can build a development Docker image with your local copy of the Sail repository.
To do this, run the following command from the root of the project directory.

```bash
docker build -t sail:latest -f docker/dev/Dockerfile .
```
