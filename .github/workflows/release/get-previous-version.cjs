module.exports = async ({ github, context, core }) => {
  const tags = await github.paginate(
    github.rest.repos.listTags.endpoint.merge({
      owner: context.repo.owner,
      repo: context.repo.repo,
      per_page: 100,
    }),
  );
  const pattern = /^v(\d+)\.(\d+)\.(\d+)$/;
  const versions = tags
    .map((tag) => tag.name)
    .filter((tag) => pattern.test(tag))
    .sort((a, b) => {
      const [, majorA, minorA, patchA] = a.match(pattern).map(Number);
      const [, majorB, minorB, patchB] = b.match(pattern).map(Number);
      if (majorA !== majorB) {
        return majorB - majorA;
      }
      if (minorA !== minorB) {
        return minorB - minorA;
      }
      return patchB - patchA;
    });
  // We want to get the previous release version, excluding pre-releases.
  // 1. For push events, if the tag is a normal release, the previous version
  //    is the second item (offset 1) in the sorted list.
  // 2. For push events, if the tag is a pre-release, the previous version
  //    is the first item (offset 0) in the sorted list.
  // 3. For other events (test release run), no new tag has been created yet,
  //    and the previous version is the first item (offset 0) in the sorted list.
  const offset =
    context.event_name === "push" && pattern.test(context.ref_name) ? 1 : 0;
  if (versions.length <= offset) {
    core.setOutput("version", "");
  } else {
    core.setOutput("version", versions[offset].replace(/^v/, ""));
  }
};
