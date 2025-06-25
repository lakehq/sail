module.exports = async ({ github, context }) => {
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
  const offset = context.event_name === "push" ? 1 : 0;
  if (versions.length <= offset) {
    return "";
  } else {
    return versions[offset].replace(/^v/, "");
  }
};
