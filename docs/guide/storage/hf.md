---
title: Hugging Face
rank: 4
---

# Hugging Face

Sail supports reading datasets directly from Hugging Face Hub using the `hf://` URI scheme. This provides easy access to thousands of public datasets without manual downloading.

## URI Format

```
hf://datasets/{username}/{dataset}@{revision}~{format}/{split}
```

### URI Components

- **`hf://datasets/`**: Required prefix for Hugging Face datasets
- **`{username}`**: Dataset owner's username or organization
- **`{dataset}`**: Dataset name
- **`@{revision}`**: (Optional) Git revision - branch, tag, or commit hash. Default: `main`
- **`~{format}`**: (Optional) File format to read. Default: auto-detected
- **`/{split}`**: Dataset split name (e.g., `train`, `test`, `validation`)

### Examples

```python
# Basic usage - read train split
df = spark.read.parquet("hf://datasets/squad/plain_text/train")

# With specific format
df = spark.read.parquet("hf://datasets/username/dataset@~parquet/train")

# With revision/branch
df = spark.read.parquet("hf://datasets/username/dataset@v1.0~parquet/train")

# Different splits
train_df = spark.read.parquet("hf://datasets/imdb/plain_text/train")
test_df = spark.read.parquet("hf://datasets/imdb/plain_text/test")
```

## Popular Datasets

### Text Datasets

```python
# IMDB movie reviews
df = spark.read.parquet("hf://datasets/imdb/plain_text/train")
df.select("text", "label").show(5, truncate=False)

# SQuAD question answering
df = spark.read.parquet("hf://datasets/squad/plain_text/train")
df.select("context", "question", "answers").show(5)

# Wikipedia
df = spark.read.parquet("hf://datasets/wikipedia/20220301.en/train")
```

### Structured Datasets

```python
# Titanic dataset
df = spark.read.parquet("hf://datasets/phihung/titanic/train")
df.printSchema()

# Iris dataset
df = spark.read.parquet("hf://datasets/scikit-learn/iris/train")
df.groupBy("species").count().show()
```

## Authentication

For private datasets or when hitting rate limits, configure authentication:

```bash
# Set your Hugging Face token
export HF_TOKEN="hf_..."

# Or use the Hugging Face CLI
huggingface-cli login
```

You can get your token from [Hugging Face Settings](https://huggingface.co/settings/tokens).

## File Formats

Hugging Face datasets can be stored in various formats:

```python
# Parquet (most common and efficient)
df = spark.read.parquet("hf://datasets/username/dataset@~parquet/train")

# JSON Lines
df = spark.read.json("hf://datasets/username/dataset@~jsonl/train")

# CSV
df = spark.read.option("header", "true").csv("hf://datasets/username/dataset@~csv/train")

# Arrow
df = spark.read.format("arrow").load("hf://datasets/username/dataset@~arrow/train")
```

::: info
If format is not specified with `~format`, Sail attempts to auto-detect based on file extensions in the dataset repository.
:::

## Cache Management

### Cache Location

Downloaded files are cached locally for faster subsequent access:

```bash
# Default cache location
~/.cache/huggingface/hub/

# Set custom cache directory
export HF_HOME="/path/to/cache"
```

### Cache Structure

```
$HF_HOME/hub/
├── datasets--username--dataset/
│   ├── refs/
│   ├── blobs/
│   └── snapshots/
│       └── {revision}/
│           └── data/
```

### Managing Cache

```bash
# Check cache size
du -sh ~/.cache/huggingface/hub/

# Clear specific dataset cache
rm -rf ~/.cache/huggingface/hub/datasets--username--dataset/

# Clear all cache
rm -rf ~/.cache/huggingface/hub/
```

## Working with Dataset Metadata

### Exploring Dataset Structure

```python
# Read dataset info
df = spark.read.parquet("hf://datasets/squad/plain_text/train")

# Check schema
df.printSchema()

# Sample data
df.show(5)

# Basic statistics
df.describe().show()
```

### Available Splits

Most datasets have standard splits:

```python
# Common split names
splits = ["train", "validation", "test"]

# Read all splits
dfs = {}
for split in splits:
    try:
        dfs[split] = spark.read.parquet(f"hf://datasets/imdb/plain_text/{split}")
        print(f"{split}: {dfs[split].count()} rows")
    except:
        print(f"{split}: not available")
```

## Configuration

### API Endpoint

Use a different Hugging Face endpoint (e.g., for mirrors or private instances):

```bash
# Default endpoint
export HF_ENDPOINT="https://huggingface.co"

# Use a mirror
export HF_ENDPOINT="https://hf-mirror.com"
```

### Additional Settings

```bash
# Disable progress bars
export HF_HUB_DISABLE_PROGRESS_BARS="1"

# Set download timeout (seconds)
export HF_HUB_DOWNLOAD_TIMEOUT="300"

# Enable offline mode (use cache only)
export HF_HUB_OFFLINE="1"
```

## Common Use Cases

### Data Analysis

```python
# Analyze sentiment distribution in IMDB
df = spark.read.parquet("hf://datasets/imdb/plain_text/train")
df.groupBy("label").count().show()

# Text length statistics
df.selectExpr("length(text) as text_length", "label") \
  .groupBy("label") \
  .agg({"text_length": "avg"}) \
  .show()
```

### Machine Learning Preparation

```python
# Prepare data for ML
from pyspark.ml.feature import Tokenizer, HashingTF

df = spark.read.parquet("hf://datasets/imdb/plain_text/train")

# Tokenize text
tokenizer = Tokenizer(inputCol="text", outputCol="words")
wordsData = tokenizer.transform(df)

# Feature extraction
hashingTF = HashingTF(inputCol="words", outputCol="features")
featurizedData = hashingTF.transform(wordsData)
```

### Combining Multiple Datasets

```python
# Combine similar datasets
datasets = [
    "hf://datasets/dataset1/config/train",
    "hf://datasets/dataset2/config/train"
]

dfs = [spark.read.parquet(ds) for ds in datasets]
combined = dfs[0].unionByName(*dfs[1:], allowMissingColumns=True)
```

## Limitations

- **Read-only**: Hugging Face storage is read-only; you cannot write data back
- **Rate limits**: Public access may be rate-limited; use authentication for higher limits
- **Dataset size**: Very large datasets may take time to download initially
- **Format support**: Not all datasets are available in all formats

## Troubleshooting

### Authentication Issues

```python
# Verify token is set
import os
print(f"HF_TOKEN is {'set' if 'HF_TOKEN' in os.environ else 'not set'}")
```

### Cache Issues

```bash
# Clear corrupted cache for specific dataset
rm -rf ~/.cache/huggingface/hub/datasets--{username}--{dataset}/
```

### Connection Issues

```bash
# Test connection to Hugging Face
curl -I https://huggingface.co

# Use a different endpoint if blocked
export HF_ENDPOINT="https://hf-mirror.com"
```
