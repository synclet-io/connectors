You are bumping the upstream API version that the connector `connectors/${CONNECTOR}` of this repository pins.

- API: ${API}
- Pinned now: `${FROM}`, in `${PIN_FILE}`
- Bump to: `${TO}`
- Provider documentation: ${DOCS}

Do this:

1. Read the provider's changelog and upgrade notes for every version after `${FROM}` up to and including `${TO}`.
   Start from the documentation link above; WebFetch is limited to the provider's hosts. List each breaking change
   that touches an endpoint, field, parameter, header or pagination scheme this connector uses.
2. Read the connector: `${PIN_FILE}`, the rest of `connectors/${CONNECTOR}/`, its `README.md` and its tests and
   recorded fixtures (`testdata/`). `CONTRIBUTING.md` describes how connectors and their tests are built.
3. Change the pinned version to `${TO}` in `${PIN_FILE}` and in the `api_versions` entry of
   `connectors/${CONNECTOR}/meta.json` (both `pinned` and, if the pin's text changes shape, `pattern`). Adapt the code,
   JSON schemas, README and fixtures to every breaking change from step 1. Keep the change as small as the upgrade
   allows; do not refactor, and do not edit `version` or `CHANGELOG.md`.
4. Run `go test -C connectors/${CONNECTOR} ./... -count=1` and `go run ./scripts validate` and fix what fails. Fixtures
   are recordings of the provider's responses: change one only where the changelog documents the new shape.
5. Write `.github/api-bump/SUMMARY.md`, which becomes the pull request description, with these sections:
   `## Breaking changes` (each with a link to the provider's notes, and whether it affects this connector),
   `## Changes made`, `## Not verified` (anything the recorded tests cannot prove, such as behaviour only a live
   account shows). Be factual and short.

Only change files under `connectors/${CONNECTOR}/` and `.github/api-bump/SUMMARY.md`; changes anywhere else are
rejected. You cannot run git or gh, and you do not need to: the workflow commits your changes and opens the pull
request for a human to review. If the upgrade cannot be done safely, leave the code unchanged and explain why in
SUMMARY.md.
