function apply_git_patch() {
  local directory="$1"
  local tag="$2"
  local file="$3"

  export __GIT_PATCH_DIRECTORY="${directory}"

  status="$(git -C "${__GIT_PATCH_DIRECTORY}" status --porcelain)"
  if [ -n "${status}" ]; then
    echo "The working directory is not clean: $(pwd)"
    exit 1
  fi

  head_commit="$(git -C "${__GIT_PATCH_DIRECTORY}" rev-parse HEAD)"
  tag_commit="$(git -C "${__GIT_PATCH_DIRECTORY}" rev-parse "${tag}")"
  if [ "${head_commit}" != "${tag_commit}" ]; then
    export __GIT_PATCH_PREVIOUS_COMMIT="${head_commit}"
    git -C "${__GIT_PATCH_DIRECTORY}" checkout "${tag}"
  else
    export __GIT_PATCH_PREVIOUS_COMMIT=""
  fi
  export __GIT_PATCH_FILE="${file}"

  echo "Applying the patch..."
  git -C "${__GIT_PATCH_DIRECTORY}" apply --ignore-whitespace "${file}"

  trap 'revert_git_patch' EXIT
}

function revert_git_patch() {
  git -C "${__GIT_PATCH_DIRECTORY}" apply -R "${__GIT_PATCH_FILE}"
  if [ -n "${__GIT_PATCH_PREVIOUS_COMMIT}" ]; then
    git -C "${__GIT_PATCH_DIRECTORY}" checkout "${__GIT_PATCH_PREVIOUS_COMMIT}"
  fi
  echo "Reverted the patch."
}
