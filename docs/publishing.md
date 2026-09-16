# Packages and releases

RB packages are published only to GitHub Packages. No workflow publishes to nuget.org.

Feed: `https://nuget.pkg.github.com/stellayazilim/index.json`

## Packages

- `Stella.RosenBridge`
- `Stella.RosenBridge.Hosting`
- `Stella.RosenBridge.Hosting.AspNetCore`

All three packages share the release version. Samples and the executable smoke suite are not packable. Package metadata includes the repository URL and README. The default local development version is `0.0.1-e`; a release tag overrides it for both build and pack.

## CI

The build workflow runs on branch pushes and pull requests. Windows and Linux runners build Release, run the executable smoke suite and both hosting samples, and verify library packaging. The suite must be invoked with `dotnet run`; it is not discoverable by `dotnet test`.

The workflows are adapted from Ergosfare's build and GitHub Packages release workflows. RB uses .NET 10 only and does not yet have coverage or NativeAOT gates.

## Publish a version

The current release line is early access: use tags such as `v0.0.1-e` and `v0.0.2-e`. The `e` suffix means early access; no stable or preview release has been published. NuGet requires a hyphen before the suffix, so `v0.0.1e` is not used. The leading `v` belongs to the Git tag and is stripped from the package version.

After the desired commit passes CI, create and push a version tag:

```shell
git tag v0.0.1-e
git push origin v0.0.1-e
```

The release workflow validates the version, resolves the existing tag to a commit, runs Windows/Linux verification for that commit, and then builds and publishes its three packages. A manual dispatch accepts an existing version tag and follows the same gates. It does not create tags or select an arbitrary branch. Keep published version tags immutable; duplicate package versions are skipped to allow retries after partial publication.

Publishing authenticates with the workflow's `GITHUB_TOKEN` and job-level `packages: write` permission. No personal token or nuget.org API key is required in repository secrets. Organization policy must permit GitHub Actions to publish packages. No GitHub Release or package attachments are created; package distribution uses the feed only.

A normal branch push runs CI but does not publish a package.

## Consume from Charlotte

Add the feed to Charlotte's NuGet sources:

```shell
dotnet nuget add source https://nuget.pkg.github.com/stellayazilim/index.json --name github-stella
```

Configure credentials separately in local NuGet credentials or a CI secret. GitHub's NuGet registry requires authentication: local consumers use a classic personal access token with `read:packages` and access to the package. Never commit the token. Charlotte's Actions workflow may use its `GITHUB_TOKEN` after its repository is granted package read access.

Once a version has been published, reference the appropriate package and explicit version. Standard dependencies continue to restore from nuget.org; it is not a publishing destination for RB.

See [GitHub's NuGet registry documentation](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-nuget-registry) for authentication and package access settings.
