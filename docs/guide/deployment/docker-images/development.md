---
title: Development
rank: 3
---

# Development

If you make changes to Sail source code locally, you can build a Docker image with your local copy of the Sail repository.
To do this, run the following command from the root of the project directory.

```bash
docker/dev/build.sh
```

This command will build the Docker image with the tag `sail:latest` using the Dockerfile located at `docker/dev/Dockerfile`.
