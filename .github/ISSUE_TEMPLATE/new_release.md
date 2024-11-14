---
name: New Release
about: Create a new release of TraceBase
title: ''
labels: 'type:task'
assignees: ''
---
<!-- markdownlint-disable-next-line first-line-heading -->
## New Release

### Dependencies
<!-- E.g. All other issues in a milestone must be completed -->
None

### Checklist

- In a development sandbox
  - [ ] `git checkout main` - or whatever branch you're creating the release on
  - [ ] `git pull` - make sure local repository is up to date
  - [ ] Edit `changelog.md` to get it ready for release
    - [ ] Replace the existing `Unreleased` header with the new tag and current date
    - [ ] `git log [PREVIOUS_TAG]..HEAD` - Review the log to summarize changes since the last release
    - [ ] `markdownlint --config .markdown-lint.yml CHANGELOG.md`
    - [ ] `git commit`
  - [ ] `git push` - commit and push updated changelog
    - [ ] Ensure all tests pass in GitHub actions
  - [ ] `git tag -a v[TAG_NAME]`
    - [ ] Enter a description of the release when prompted
  - [ ] `git push origin v[TAG_NAME]`
  - [ ] Follow steps to [create a release on GitHub](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository#creating-a-release)
  - [ ] Edit `changelog.md` to add back the *Unreleased* section
    - [ ] Replace the existing `Unreleased` section with the new tag and current date
    - [ ] `markdownlint --config .markdown-lint.yml CHANGELOG.md`
    - [ ] `git commit`
  - [ ] `git push` - commit and push updated changelog
  - [ ] If this is a release branch (not `main`), merge into main
    - [ ] `git checkout main`
    - [ ] `git merge [BRANCH]`
    - [ ] `git push`
- [ ] [Deploy to `tracebase-dev.princeton.edu`](https://nplcadmindocs.princeton.edu/index.php/TraceBase#Deploy_Update)
- [ ] [Deploy to `tracebase.princeton.edu`](https://nplcadmindocs.princeton.edu/index.php/TraceBase#Deploy_Update)

See [NPLC Admin Docs:TraceBase:New Release](https://nplcadmindocs.princeton.edu/index.php/TraceBase#New_Release) for more details
