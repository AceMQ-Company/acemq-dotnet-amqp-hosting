# Releasing

Packages are published to the **Git-hosted NuGet feed** at
[AceMQ-Company/nuget](https://github.com/AceMQ-Company/nuget), served over GitHub
Pages at <https://acemq.org/nuget/>. It is the same feed `acemq-dotnet-amqp`
publishes the library to, and the one `NuGet.config` in this repository already
reads. Consumers add one source and need no credentials:

```bash
dotnet nuget add source https://acemq.org/nuget/index.json --name acemq
dotnet add package AceMq.Amqp.Hosting
```

There is no push endpoint. The feed is a static directory tree — a flat container
under `/v3/flatcontainer/` — so publishing is a commit to another repository
rather than a `dotnet nuget push`. Everything below follows from that.

## Why not nuget.org

The same trade `acemq-dotnet-amqp` makes: a package on nuget.org can be unlisted
but never removed or replaced, which is the wrong permanence before 1.0 while the
configuration keys and the builder surface are still moving. Here a bad release
can be deleted.

The cost is real and belongs in the open: an application consuming this package
has to add the AceMQ source, and a *library* consuming it obliges its own
consumers to do the same. That is fine for applications, awkward for libraries,
and it is the reason 1.0 moves to nuget.org.

## Which number

**This package versions independently of the library. It is at `0.1.0` while
`AceMq.Amqp` is at `0.7.x`, and that is not drift.**

It tracks two release trains, not one. A change in `Microsoft.Extensions.Hosting`
is as likely to force a release here as a change in AceMQ is, and a version number
shared with the library could only ever say *one of the two moved* — which tells a
consumer nothing about which, and makes every AceMQ release look like a change to
this package whether or not anything here changed. That is the reasoning the
Spring Boot starter's changelog gives for the same arrangement, and the reasoning
`Directory.Build.props` records at the top.

So `AceMq.Amqp` is a **dependency with a version range**, not a sibling. The
packed nuspec says `version="0.7.2"` — whatever `<AceMqVersion>` holds — which
NuGet reads as *0.7.2 or newer*, so a consumer already on a later library keeps
it.

While the version is `0.x` the public surface may change in any release, which is
what semver means by leaving `0.y.z` outside its compatibility guarantees.
**Anything depending on this package before 1.0 should pin an exact version.**

### The release line, and the guard

The workflow refuses to publish anything outside the current line:

```sh
case "$VERSION" in
  0.1.*) ;;
  *)
    echo "::error::this package releases 0.1.x only; ..."
    exit 1
    ;;
esac
```

A published version is permanent even on a feed we control, because somebody's
lockfile already has it. A mistyped tag — `v1.0.0` for `v0.1.0`, a fat-fingered
`v0.10.0` — would ship something unrecallable, and the guard is what makes that a
failed run instead. `0.1.*` also covers `0.1.1`, so a patch on top of a release
that is already out needs no edit.

**Moving the line is a deliberate edit, in the same commit that moves
`<Version>` in `Directory.Build.props`.** Change the `case` to `0.2.*`, and the
error message with it.

## The version source

`<Version>` in `Directory.Build.props`, one property covering both packages:

```xml
<Version>0.1.0</Version>
```

At release time the **tag is the authority**: the workflow strips the leading `v`,
refuses anything that is not a plain version number, checks it against the guard,
and passes it explicitly to `dotnet build` and `dotnet pack` as `-p:Version=`. So
the tag and the artifacts cannot disagree, whatever the file says.

Keep the file in step anyway. It is what a local `dotnet pack` produces, what CI
packs and checks on every push, and what the README badge and the status line
claim — and a file that says `0.1.0` while `v0.2.0` is out is a trap for the next
person, not a saved edit.

`AceMqVersion` beside it names the released `AceMq.Amqp` this is built and tested
against. Moving it is a change worth a changelog entry, because it moves the
minimum a consumer resolves.

## Cutting a release

1. **Roll the changelog.** Move `[Unreleased]` to the new version with today's
   date, and leave a fresh empty `[Unreleased]` in place.
2. **Check `Directory.Build.props`.** `<Version>` should already be the version
   being cut; if it is not, this is the commit that fixes it.
3. **Check the README.** The version badge and the status line both name the
   version, and neither is rewritten by the release — nothing here is. Update them
   in the same commit that rolls the changelog, because a line that is only
   slightly wrong is one nobody comes back to.
4. **Commit everything**, and check `git status` is clean.
5. **Pull first.** `git pull --rebase`. Tagging a commit that is not on `main`
   still builds correctly — the release builds from the tag — but leaves the
   released commit outside the branch history. If it happens anyway, **merge
   rather than rebase**: rebasing moves the commit the tag points at, and a
   release tag unreachable from `main` is worse than a merge commit.
6. **Tag it.**

   ```bash
   git tag -a v0.1.0 -m 0.1.0 && git push origin v0.1.0
   ```

   Annotated: `git tag` alone opens an editor that fails in a non-interactive
   shell, and the workflow is written for a tag that carries a message.

That is the whole release. The tag triggers `.github/workflows/release.yml`,
which builds at that version, runs the unit suite and the integration suite
against a real broker, packs, **checks the packed artifacts before anything
leaves the runner**, pushes them into the `nuget` repository, **verifies the new
version resolves from an empty package cache**, creates the GitHub release and
announces it in Slack.

### Re-running a release without moving a tag

`workflow_dispatch` takes the version as an input and builds from whichever ref it
is run on:

```bash
gh workflow run release.yml --ref main -f version=0.1.0
```

That is the route to use when a release failed after the tag was pushed — a
missing secret, a flaked verification, a Pages deploy that had not caught up.
**Moving a tag to re-run a release is the thing not to do**: the tag would then
point at a different commit from the one whose artifacts are already on the feed.

Re-running against a version already in the feed is safe. The push step finds
nothing to commit, reports `published=true` anyway — from a consumer's point of
view that version *is* published — and the verification runs as usual.

## What a release contains

Two packages, each multi-targeted:

| Package | `lib/` |
| --- | --- |
| `AceMq.Amqp.Hosting` | `netstandard2.0`, `net8.0` |
| `AceMq.Amqp.Hosting.OpenTelemetry` | `netstandard2.0`, `net8.0` |

`dotnet pack` also writes a `.snupkg` symbol package beside each. Those are not
published: the publish step globs `*.nupkg`, which does not match `.snupkg`, and a
static flat container has nowhere to serve a symbol package from anyway.

The `Check what was packed` step asserts three things about each package before
the feed is touched, each of which has shipped wrong somewhere in this
organisation:

- the **nuspec version** is the version the tag asked for, not just the filename;
- both `lib/netstandard2.0/` and `lib/net8.0/` are present — multi-targeting that
  reaches `bin` and not the package is a promise the consumer never sees;
- `AceMq.Amqp` is a **package dependency** in the nuspec. A project reference
  slipped in where a package reference belongs would make this whole repository
  stop proving what it exists to prove: that the *published* library is usable
  from an application.

## Verifying a release

The workflow does it, from an empty package cache against the public feed, which
is the only check that asks what a consumer actually gets. By hand:

```bash
mkdir /tmp/verify && cd /tmp/verify
dotnet new classlib -n Verify -o .
dotnet nuget add source https://acemq.org/nuget/index.json --name acemq
dotnet add package AceMq.Amqp.Hosting --version 0.1.0 \
  --package-directory /tmp/verify-packages
```

`AceMq.Amqp` and `AceMq.Amqp.RabbitMq` should come down with it. GitHub Pages
takes a minute or two to serve newly pushed files, which is why the workflow polls
the flat-container URL for up to five minutes before restoring.

### The URLs redirect

The organisation site carries the custom domain `acemq.org`, and GitHub redirects
every project page beneath it, so `https://acemq-company.github.io/nuget/...`
answers **301** to `https://acemq.org/nuget/...` whatever the `nuget` repository's
own Pages settings say. NuGet follows the redirect; a bare `curl` does not. The
verification step uses `curl -L` for exactly this reason — comparing a 301 against
200 once reported a correctly published version as never published. Anything else
that checks these URLs by hand needs `-L` too.

### `dotnet package search` does not work against this feed

A static flat container serves package content and nothing else — no search
service resource. `dotnet package search` fails, and `dotnet tool install` against
it dies with an unhandled `NullReferenceException` rather than a message. Neither
affects `dotnet add package` or `dotnet restore`, which are all this package needs.

## Credentials the release uses

**Before a tag can be cut, this repository needs one secret it does not have:**

| Secret | Needs | Required |
| --- | --- | --- |
| `NUGET_REPO_DEPLOY_KEY` | write access to `AceMQ-Company/nuget` | **yes — the release cannot publish without it** |
| `SLACK_DELIVERY_WEBHOOK` | the delivery channel's incoming webhook | no |
| `GITHUB_TOKEN` | this repository | built in, nothing to add |

`NUGET_REPO_DEPLOY_KEY` is a **deploy key**, not a token. It writes to exactly one
repository and nothing else — a scope a personal access token cannot express. It
belongs to the repository rather than to a person, so it survives whoever made it
leaving and cannot be used as them, and it does not expire, so a release cannot
fail at three in the morning because a token quietly aged out.

To create it:

```bash
ssh-keygen -t ed25519 -N '' -C 'acemq-dotnet-amqp-hosting release -> nuget' -f /tmp/k
gh api repos/AceMQ-Company/nuget/keys -X POST \
  -f title='acemq-dotnet-amqp-hosting release' -f key="$(cat /tmp/k.pub)" -F read_only=false
gh secret set NUGET_REPO_DEPLOY_KEY -R AceMQ-Company/acemq-dotnet-amqp-hosting < /tmp/k
rm /tmp/k /tmp/k.pub          # and delete the old key from the target repository when rotating
```

Rotating is the same three commands, then deleting the superseded key from
`AceMQ-Company/nuget`'s Settings -> Deploy keys. Deploy keys are an
organisation-level permission (`deploy_keys_enabled_for_repositories`), off by
default on a new organisation and enabled here.

**A missing deploy key fails the run in its first minute, deliberately.** The
`Check the credentials the publish needs` step runs before the build, because the
alternatives are both worse: the feed repository is readable without a key, so
`actions/checkout` would fall back to `GITHUB_TOKEN`, clone happily, and fail only
at the push — twelve minutes in, with a message about permissions on another
repository rather than about a secret nobody added here.

A missing `SLACK_DELIVERY_WEBHOOK` is the opposite case: the notification step
does nothing and succeeds. A missing webhook must never fail a release that
worked.

## What Slack hears, and what it does not

Through the shared reusable workflow in `acemq-java-amqp`, so the formatting lives
in one place for every AceMQ repository.

**A release that fails before the feed push is not announced.** Nothing reached
the feed, no version exists, and nobody can resolve anything they could not
resolve before — announcing that as a failed release puts a red message in the
channel about an event that did not happen. A channel that cries wolf is one
people stop reading, and that costs the announcement that matters. The failure is
still a red tag build in Actions, and whoever pushed the tag is watching it.

**Everything from the push onwards is announced, success or failure.** A version
that is published but whose release did not finish is the genuinely dangerous
state: consumers can resolve it while the release notes still name the previous
one. That one leads with `:construction:` rather than a red dot — the status
underneath still reads `failure`, because that is what the run was, but the icon
is about what the reader should *do*, and "usable and unfinished" is a different
instruction from "broken".

The switch is the `published` output of the `publish` job, set by the step that
pushes to the feed repository. Job outputs survive the job failing.

## What this release does not do

Unlike the library's, this workflow does **not** update the organisation landing
page. That page carries a card per library and this package is not one of them.
If it gains one, the job to copy is `landing-page` in
`acemq-dotnet-amqp/.github/workflows/release.yml`.

It also does not rewrite versions in the documentation. There is no
`set-documented-version.sh` here, and the pages that name a version mostly name
the *library's* — `AceMq.Amqp` 0.7.2 — which a release of this package does not
move.

So a **library** release leaves stale sentences behind here even though no commit
landed. `<AceMqVersion>` is checked by the `tracks-the-release` CI job and the
README badge by `badge-matches-the-release`; the prose is not checked by
anything. When `<AceMqVersion>` moves, grep for the old number and fix what it
finds:

```bash
grep -rn "0\.7\.[0-9]" README.md RELEASING.md CHANGELOG.md docs/
```

Two kinds of hit come back and only one of them is stale. "Built against
`AceMq.Amqp` 0.7.2" is a claim about the present and moves with the pin; "`held`
is new in `AceMq.Amqp` 0.7.0" is a historical fact about when something appeared
and must not be touched.

## Rules

- **Never rewrite a published version.** Deleting one from the feed is possible
  and is occasionally right; quietly replacing the contents of a version somebody
  has already resolved is not.
- **Never publish from a working tree.** `dotnet nuget push` has nowhere to go
  here anyway, but the point stands: the workflow builds from a pushed commit, so
  what is published and what is readable are the same thing.
- **Release from `main`, green.** The workflow re-runs the whole suite including
  the integration tests, which is the one moment where waiting for a broker is
  obviously worth it. `ci.yml` runs on every push, so green is the normal state
  rather than something to arrange.
