module.exports = async ({ github, context, core }) => {
  const version = process.env.RELEASE_VERSION;
  const previousVersion = process.env.PREVIOUS_RELEASE_VERSION;
  const text = [];
  const summaryUrl = `https://docs.lakesail.com/sail/latest/reference/changelog/#_${version.replace(/\./g, "-")}`;
  const pypiUrl = `https://pypi.org/project/pysail/${version}/`;
  text.push("## Overview\n");
  text.push(
    `You can find the release summary in the [documentation](${summaryUrl}).\n\n`,
  );
  text.push(
    `The PySail Python package is available on [PyPI](${pypiUrl}).\n\n`,
  );

  if (previousVersion) {
    const { data } = await github.rest.repos.generateReleaseNotes({
      owner: context.repo.owner,
      repo: context.repo.repo,
      tag_name: `v${version}`,
      target_commitish: context.ref,
      previous_tag_name: `v${previousVersion}`,
    });
    text.push(data.body);
  }
  core.setOutput("content", text.join(""));
};
