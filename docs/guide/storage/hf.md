---
title: Hugging Face
rank: 4
---

# Hugging Face

You can use the `hf://` URI to read data stored as a Hugging Face dataset.

For example, `hf://datasets/username/dataset@~parquet/train` can be used to read the `train` split of the `username/dataset` dataset in Parquet format.

## File Cache

Files in Hugging Face datasets are cached locally once downloaded.
The cache is shared with other Hugging Face Python tools.
The cache directory is `$HF_HOME/hub` where `$HF_HOME` is an environment variable with the default value `~/.cache/huggingface`.
You can set the `HF_HOME` environment variable to use a different cache directory.

## Endpoint

You can set the `HF_ENDPOINT` environment variable to use a different Hugging Face API endpoint (e.g. a mirror). The default endpoint is `https://huggingface.co`.
