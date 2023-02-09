<!-- markdownlint-disable-next-line first-line-heading -->
## Summary Change Description

Briefly describe the changes in this pull request (put details in the
developer section of the linked issue(s)).

## Affected Issue/Pull Request Numbers

- Resolves #<issue_number_1>
- Partially addresses #<issue_number_2>
- Depends on parent #<pull_request_number>

## Merge Requirements

This pull request will be merged once the following checked requirements are
met *(author: edit as desired)*:

- [x] Minimum approvals: 2
- [x] Changes requested: 0
- [ ] Specific approvals: @\<specific reviewers who MUST approve>
- [ ] Child pull requests are merged:
  - #<pull_request_number_1>
  - #<pull_request_number_2>
- Basic requirements
  - [x] Linting passes
  - [x] Tests pass
  - [x] Conflicts resolved

## Code Review Notes

Describe any areas of concern that code reviewers should pay particular
attention to.  E.g. There are significant logic changes in function X.

## Checklist

- [ ] Changes have been added *(or are not necessary to add)* to the
      *Unreleased* section of the [`changelog.md`](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/main/changelog.md).
- [ ] All issue requirements satisfied *(or no linked issues)*
- [ ] [Linting passes](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/main/CONTRIBUTING.md#linting).
- [ ] [Migrations created & committed *(or no model changes)*](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/main/CONTRIBUTING.md#migration-process)
- Tests
  - [ ] [Tests implemented for new methods *(or no new methods)*](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/main/CONTRIBUTING.md#test-implementation)
  - [ ] Tests updated for changed methods *(or no changed methods)*
  - [ ] [All tests pass](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/main/CONTRIBUTING.md#quality-control)
  - [ ] All example load tests pass (remotely) *(or all failures predate this PR)*
