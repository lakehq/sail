#!/bin/bash

if [ -z "${RELEASE_VERSION:-}" ]; then
  echo "Missing environment variable: RELEASE_VERSION"
  exit 1
fi

printf 'More information about the release can be found in the [documentation](%s).\n\n' \
  "https://docs.lakesail.com/sail/latest/reference/changelog/"

printf '* **PyPI**: [pysail %s](https://pypi.org/project/pysail/%s/)\n' \
  "${RELEASE_VERSION}" \
  "${RELEASE_VERSION}"

if [ -n "${PREVIOUS_RELEASE_VERSION:-}" ]; then
  printf '* **Changelog**: [v%s...v%s](%s)\n' \
    "${PREVIOUS_RELEASE_VERSION}" \
    "${RELEASE_VERSION}" \
    "https://github.com/lakehq/sail/compare/v${PREVIOUS_RELEASE_VERSION}...v${RELEASE_VERSION}"
fi
