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

::: info

- The tags should generally point to commits on the `main` branch.
- The GitHub repository has a few tag rulesets in place so that only the project maintainers can manage the tags
  for the release process.

:::

### Creating Tags

To create tag `$TAG`, create it locally and push it to the remote repository.

```bash
git tag $TAG
git push origin tag $TAG
```

::: info
The `tag $TAG` refspec is the same as `refs/tags/$TAG:refs/tags/$TAG`.
Please refer to the [git-push documentation](https://git-scm.com/docs/git-push) for more details.
:::

### Updating Tags

To point tag `$TAG` to a different commit, force update the tag locally and force push it to the remote repository.

```bash
git tag -f $TAG
git push origin -f tag $TAG
```

### Deleting Tags

To delete tag `$TAG`, delete it locally and push the deletion to the remote repository.
Note that this does not delete the artifacts released to external systems.

```bash
git tag -d $TAG
git push -d origin $TAG
```

## Python Package Release Tags

`v<version>`

: This publishes the Python package to PyPI.
`<version>` is a [Python version specifier](https://packaging.python.org/en/latest/specifications/version-specifiers/).
It must be the same as the version defined in the source code, otherwise the release workflow will fail.

::: info
The Python version specifier is different from the [semantic versioning](https://semver.org/) scheme,
though the two schemes are compatible to some extent.
We follow the `<major>.<minor>.<patch>` aspects of semantic versioning when releasing Sail versions.
:::

After the GitHub Actions workflow is triggered by the tag push and the Python package build is successful,
the project maintainer needs to manually approve the "Release" job in GitHub Actions to actually publish the package.

::: info
The Python package release workflow can also be triggered manually for testing purposes.
Manually triggered workflow runs publish the Python package to Test PyPI.
:::

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
