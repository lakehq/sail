---
title: Hugging Face
rank: 3
---

# Hugging Face

Files in Hugging Face datasets are cached locally once downloaded.
The cache is shared with other Hugging Face Python tools.
The cache directory is `$HF_HOME/hub` where `$HF_HOME` is an environment variable with the default value `~/.cache/huggingface`.
You can set the `HF_HOME` environment variable to use a different cache directory.

You can set the `HF_ENDPOINT` environment variable to use a different Hugging Face API endpoint (e.g. a mirror). The default endpoint is `https://huggingface.co`.
