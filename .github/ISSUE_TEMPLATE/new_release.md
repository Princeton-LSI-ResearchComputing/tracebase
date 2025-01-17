---
name: New Release
about: Create a new release of TraceBase
title: ''
labels: 'type:task, priority:3-medium, effort:5-under1hr'
assignees: ''
---
<!-- markdownlint-disable-next-line first-line-heading -->
## New Release

### Dependencies
<!-- This issue cannot be started until the completion of the following:
- #issue_number_1
- #pull_request_1
-->
None

### Checklist

- In a development sandbox
  - [ ] `git checkout main` - or whatever branch you're creating the release on
  - [ ] `git pull` - make sure local repository is up to date
  - [ ] Edit `CHANGELOG.md` to get it ready for release
    - [ ] Replace the existing `Unreleased` header with the new tag and current date
    - [ ] `git log [PREVIOUS_TAG]..HEAD` - Review the log to summarize changes since the last release
      - Tip/example: `git log --reverse [PREVIOUS_TAG]..HEAD | grep "^    " | cut -b 5- | grep . | grep -v -E "^Details:|^Merge pull request|^Files checked in|skip ci"`
    - [ ] `markdownlint --config .markdown-lint.yml CHANGELOG.md`
    - [ ] `npx textlint CHANGELOG.md`
    - [ ] `git commit`
  - [ ] `git push` - commit and push updated changelog
    - [ ] Ensure all tests pass in GitHub actions
  - [ ] `git tag -a [TAG_NAME]`
    - [ ] Enter a description of the release when prompted
  - [ ] `git push origin [TAG_NAME]`
  - [ ] Follow steps to [create a release on GitHub](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository#creating-a-release)
  - [ ] Edit `CHANGELOG.md` to add back the *Unreleased* section
    - [ ] `markdownlint --config .markdown-lint.yml CHANGELOG.md`
    - [ ] `npx textlint CHANGELOG.md`
    - [ ] `git commit`
  - [ ] `git push` - commit and push updated changelog
  - [ ] If this is a release branch (not `main`), merge into main
    - [ ] `git checkout main`
    - [ ] `git merge [BRANCH]`
    - [ ] `git push`
- [ ] [Deploy to dev: `tracebase-dev.princeton.edu`](https://nplcadmindocs.princeton.edu/index.php/TraceBase#Deploy_Update)
- [ ] [Deploy to prod: `tracebase-rabonowitz.princeton.edu`](https://nplcadmindocs.princeton.edu/index.php/TraceBase#Deploy_Update)
- [ ] [Deploy to pub: `tracebase.princeton.edu`](https://nplcadmindocs.princeton.edu/index.php/TraceBase#Deploy_Update)

See [NPLC Admin Docs:TraceBase:New Release](https://nplcadmindocs.princeton.edu/index.php/TraceBase#New_Release) for more details
