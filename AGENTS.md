# Eventa

## Database migrations

- Migrations run only in the API's `start.sh`. During a deploy, old and new containers overlap (the API rolls; workers deploy separately), so every migration must work with the previous release's code.
- One release: additive changes (nullable column or one with a server default, table, index).
- Two releases: dropping or renaming a column or table, adding `NOT NULL`, or changing a type. Release 1 stops reading and writing it; a later release runs the migration.
- See [Database migrations](https://github.com/mylinden-tech/infra/blob/main/docs/linden-continuous-delivery.md#database-migrations) and mylinden-tech/infra#260.

## Shared Tessera docs

Cross-service knowledge lives in [tesserahq/docs](https://github.com/tesserahq/docs) (locally `~/sites/tesserahq/docs` when available): the glossary, service ownership, and Custos authorization conventions. Read it before writing or reviewing a PRD, or changing authorization, project scoping, or asset classification.
