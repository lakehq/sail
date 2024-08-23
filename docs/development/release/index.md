---
title: Release Process
rank: 5
---

# Release Process

::: info
This guide is for the project maintainers.
:::

## Overview

We use Git tags to automate the release of Sail Python packages and the Sail documentation site.
Pushing tags to the GitHub repository triggers GitHub Actions workflows for the release process.

### Creating Release Tags

To create tag `$TAG`, create it locally and push it to the remote repository.

```bash
git tag $TAG
git push origin $TAG
```

::: info
You should generally create tags for commits in the `main` branch.
:::

### Deleting Release Tags

Sometimes you may need to move the tag to a different commit. To do this, delete the tag and create it again at the new commit.

To delete tag `$TAG`, delete it locally and push the deletion to the remote repository.
Note that this does not delete the artifacts released to external systems.

```bash
git tag -d $TAG
git push --delete origin $TAG
```

::: info
The GitHub repository has a few tag rulesets in place so that only the project maintainers can manage the release tags.
:::

## Python Package Release Tags

`v<version>`

: This publishes the Python package to PyPI.
`<version>` is a [Python version specifier](https://packaging.python.org/en/latest/specifications/version-specifiers/).
It must be the same as the version defined in the source code, otherwise the release workflow will fail.

`test/v<version>`

: This publishes the Python package to Test PyPI.
This is for testing purpose only and the tags can be created and deleted as needed.

::: info
The Python version specifier is different from the [semantic versioning](https://semver.org/) scheme,
though the two schemes are compatible to some extent.
We follow the `<major>.<minor>.<patch>` aspects of semantic versioning when releasing Sail versions.
:::

After the GitHub Actions workflow is triggered by the tag push and the Python package build is successful,
the project maintainer needs to manually approve the "Release" job in GitHub Actions to actually publish the package
to PyPI or Test PyPI.

## Documentation Release Tags

`docs/latest`

: This publishes the documentation site for the latest version. The published site is available at the URL
`/sail/latest/`.

`docs/v<version>`

: This publishes the documentation site for an older version. `<version>` is the version specifier
in the format `<major>.<minor>`, which covers all patch versions with the same major and minor versions.
The published site is available at the URL `/sail/<version>/`.
Note that we do not publish the versioned site for the latest version.
Also, since we do not maintain sites for older `0.<minor>` versions, this release tag will be used only after Sail 1.0.

::: info
The documentation site at the URL `/sail/main/` is published when the `main` branch is updated.
There is no corresponding release tag for this site.
:::
