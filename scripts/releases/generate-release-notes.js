module.exports = async ({ github, context, core }) => {
  const version = process.env.RELEASE_VERSION;
  const previousVersion = process.env.PREVIOUS_RELEASE_VERSION;
  const text = [];
  const url = `https://docs.lakesail.com/sail/latest/reference/changelog/#_${version.replace(/\./g, "-")}`;
  text.push("## Overview\n");
  text.push(
    `Please refer to the [documentation](${url}) for a summary of the release.\n\n`,
  );
  text.push(
    `PySail ${version} is available on [PyPI](https://pypi.org/project/pysail/${version}/).\n\n`,
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
