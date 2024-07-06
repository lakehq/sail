function apply_git_patch() {
  local tag="$1"
  local file="$2"

  status="$(git status --porcelain)"
  if [ -n "${status}" ]; then
    echo "The working directory is not clean: $(pwd)"
    exit 1
  fi

  head_commit="$(git rev-parse HEAD)"
  tag_commit="$(git rev-parse "${tag}")"
  if [ "${head_commit}" != "${tag_commit}" ]; then
    export __GIT_PATCH_PREVIOUS_COMMIT="${head_commit}"
    git checkout "${tag}"
  else
    export __GIT_PATCH_PREVIOUS_COMMIT=""
  fi
  export __GIT_PATCH_FILE="${file}"

  echo "Applying the patch..."
  git apply "${file}"

  trap 'revert_git_patch' EXIT
}

function revert_git_patch() {
  git apply -R "${__GIT_PATCH_FILE}"
  if [ -n "${__GIT_PATCH_PREVIOUS_COMMIT}" ]; then
    git checkout "${__GIT_PATCH_PREVIOUS_COMMIT}"
  fi
  echo "Reverted the patch."
}
