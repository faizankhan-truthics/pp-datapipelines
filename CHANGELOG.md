# CHANGELOG

<!-- version list -->

## v7.17.0 (2026-11-06)


## v7.16.0 (2026-11-05)

### Bug Fixes

- Address PR review (unresolved sessions, incremental funnel, bridge guards)
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- Address review comments on the env check and tox
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- Correct stripe refunded and captured amounts in int_stripe_charges
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- Exclude DestinationUrl from ad performance (row-split risk)
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- Keep keyword performance at the keyword grain
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- Match the _sandbox dataset suffix in the local dbt guard
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- Set Fitday GA4 property id in seed_ga4_project_map
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- Use minor_unit_divisor for stripe and solidgate amount conversions
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- **dbt**: Convert solidgate amounts to major units in silver
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- **dbt**: Convert Stripe minor units to major units in silver amount_usd
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- **dbt**: Quote stripe schema description and extend USD unit test
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- **vamp**: Include customer in CE 3.0 fallback card key
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- **vamp**: Use bronze disputes for CE 3.0 flagged-charge exclusion
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

### Build System

- Add tox config to run lint, env, pytest and dbt checks locally
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- Make tox -e dbt load its settings from .env and refuse non-sandbox datasets
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

### Chores

- Drop handoff doc, DAG flag and superseded funnel_stage models
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- Regenerate .env.ci from the current Astro variables
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

### Code Style

- Ruff format microsoft_ads base hook
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- Ruff format microsoft_ads base hook to fix lint check
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

### Continuous Integration

- Add ruff and env var checks to PR checks
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- Fix stale comment on airflow-validate
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- Harden env-check: pinned Astro CLI, token scoped to its steps, refuse symlinked .env
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- Run airflow-validate only on PRs into main, as before
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- Run dbt-test on PRs into develop too
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- Run dbt-test only on PRs into main, as before
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- Speed up dbt-test with 16 threads and a pip cache
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

### Documentation

- Add Stripe product onboarding guide
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- Describe the env check, tox and the checks to run before pushing
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

### Features

- Add Microsoft Ads ad performance endpoint
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- Add Microsoft Ads keyword performance endpoint
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- Add Project and Funnel GA4 dimensions for dashboard filters
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- Add Visa CE 3.0 adjusted columns to stripe_vamp_dashboard_overview
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

- URL-based Funnel spend mart + campaign_id→project bridge
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))

### Refactoring

- **ga4**: Merge seed_ga4_project_map into seed_ga4_payment_account_map
  ([#291](https://github.com/increast-team/pp_datapipelines/pull/291),
  [`9b9c756`](https://github.com/increast-team/pp_datapipelines/commit/9b9c756c93bfa8ba37638599f75a6ace0569e2de))


## v7.15.1 (2026-10-29)

### Bug Fixes

- **starburst**: Set us-east-1-shared workers to 1 so Galaxy accepts cluster updates
  ([`f649e46`](https://github.com/increast-team/pp_datapipelines/commit/f649e4646153c480818ea819c0ab8e795236a607))


## v7.15.0 (2026-10-29)

### Bug Fixes

- Block mixing Daily/Hourly rows for the same account and date range
  ([`256f9a2`](https://github.com/increast-team/pp_datapipelines/commit/256f9a2cd5a12a46808cd6e5d0a5c192580908b7))

- Correct same-day window handling in aggregation
  ([`4c8f6a7`](https://github.com/increast-team/pp_datapipelines/commit/4c8f6a70d96f8f759265b6ae2676c89a640f8707))

- Fetch the previous full UTC day on manual stripe runs
  ([`6bea3c5`](https://github.com/increast-team/pp_datapipelines/commit/6bea3c586fe864d2e970fcf8b65b5eed7933a5fd))

- Guard stripe event merges against stale events and other accounts
  ([`ebaef8d`](https://github.com/increast-team/pp_datapipelines/commit/ebaef8da8202885a4cf5b392507b93654df325c7))

- Handle stripe runs triggered without a data interval
  ([`087e965`](https://github.com/increast-team/pp_datapipelines/commit/087e96537bdf4cd9f64d1fc3a5a7633763259244))

- Keep zero-amount solidgate orders in int_solidgate_card_orders
  ([#269](https://github.com/increast-team/pp_datapipelines/pull/269),
  [`6a05338`](https://github.com/increast-team/pp_datapipelines/commit/6a05338f30c1b07bfbc0d09990d087e30debc266))

- Retry only the failed hours of a stripe events extract
  ([`cb13007`](https://github.com/increast-team/pp_datapipelines/commit/cb13007412fce85e2605464d8a602ebfb1130d3d))

- Stop stripe *.created events from overwriting later state
  ([`19be9c5`](https://github.com/increast-team/pp_datapipelines/commit/19be9c5f09aa35992708ac42ff8eecbc6f68c422))

- Store zero tax for invoices with an empty total_taxes array
  ([`d0556f3`](https://github.com/increast-team/pp_datapipelines/commit/d0556f3ba17665d4de99761e2fe4e3020bd57d84))

- Strip % suffix before casting Ctr/ConversionRate/Ptr columns
  ([`b2c9d19`](https://github.com/increast-team/pp_datapipelines/commit/b2c9d19ce626abb5e63afbab5c1d92e001e8493a))

- Write is_deleted = FALSE on stripe API inserts
  ([`5639a7f`](https://github.com/increast-team/pp_datapipelines/commit/5639a7f25a9577d8fb010cabf9b9b99d8e206166))

- **amplitude**: Map multi-arm experiment variants in int_amplitude_experiment_events
  ([`8e37bee`](https://github.com/increast-team/pp_datapipelines/commit/8e37bee29d405a5ab596ae030ba2f52e13764d52))

- **starburst**: Use pvtlnk id for docuhero catalog, let terraform manage cluster catalog_refs
  ([`61fe9dd`](https://github.com/increast-team/pp_datapipelines/commit/61fe9dde6263e81b01d07c781ea0e469298c1dee))

### Build System

- Add ruff config, apply lint autofixes and noqa the leftovers
  ([`737d8ee`](https://github.com/increast-team/pp_datapipelines/commit/737d8ee56ecdb26f246db159db009a3b948c8498))

### Chores

- Remove dead code and stale config, align module docstrings
  ([`e3f7d8b`](https://github.com/increast-team/pp_datapipelines/commit/e3f7d8baedd0c9dc459a4a9011140deea5a0aa0f))

- Shared naming for backfill params (date_from / date_to)
  ([#268](https://github.com/increast-team/pp_datapipelines/pull/268),
  [`62f4e2f`](https://github.com/increast-team/pp_datapipelines/commit/62f4e2fb8dd8203db0f9eac75f0c3603321b61b6))

### Code Style

- Apply ruff format
  ([`684a6ee`](https://github.com/increast-team/pp_datapipelines/commit/684a6ee8a90babf4e4947ae7463e16a657fe017c))

### Documentation

- Add stripe events sourcing design
  ([`6176e3b`](https://github.com/increast-team/pp_datapipelines/commit/6176e3bbfbff5fd36bb18aef88b5ba4e9b744342))

### Features

- Add Microsoft Ads account performance endpoint + aggregation/accounts run params
  ([`e392f56`](https://github.com/increast-team/pp_datapipelines/commit/e392f5609f00bd6120438b2e2d2aabdb4452d891))

- Add Microsoft Ads ad group performance endpoint
  ([`ee39730`](https://github.com/increast-team/pp_datapipelines/commit/ee39730a981a96b624ea03865c3a9d47927dfced))

- Expose is_deleted in stripe silver models
  ([`5d830af`](https://github.com/increast-team/pp_datapipelines/commit/5d830afda8e4b37738d2fda4b2d037e681d53d66))

- Let stripe events update the real bronze tables
  ([`f9fa562`](https://github.com/increast-team/pp_datapipelines/commit/f9fa5626ac58a37aa887680cdaf0475d0cf3141c))

- Make stripe event merges update-only
  ([`cf2bd0c`](https://github.com/increast-team/pp_datapipelines/commit/cf2bd0cb034c358eef1768530adaac93b4b909e8))

- Manage BigQuery table-level access as code with Terraform
  ([`645bd1c`](https://github.com/increast-team/pp_datapipelines/commit/645bd1c163f345d5d84a2f82f2e358c32cb7e03b))

- Merge stripe objects from events and run events DAG daily
  ([`a9cce54`](https://github.com/increast-team/pp_datapipelines/commit/a9cce54664f9e462c2969d9fcd07a5d1e997e31b))

- Run stripe events after the API load and keep every API endpoint
  ([`4d3813d`](https://github.com/increast-team/pp_datapipelines/commit/4d3813d4a06c1440d4e7e63d498ed9ed404f14b6))

- Support event-sourced targets in stripe merge SQL
  ([`6f23897`](https://github.com/increast-team/pp_datapipelines/commit/6f2389792e1c7cd90964c682b77dad947093883d))

- Tie stripe financial default endpoints to events shadow mode
  ([`06d9901`](https://github.com/increast-team/pp_datapipelines/commit/06d990105b3637d132715a76838ab423b2f79ec1))

- **starburst**: Add docuhero_rds_postgres catalog
  ([`bee56e4`](https://github.com/increast-team/pp_datapipelines/commit/bee56e455753b097db6e07d9d3f796774c1375cc))

### Refactoring

- Drop unreachable .created tiebreak from stripe merges
  ([`bf0315b`](https://github.com/increast-team/pp_datapipelines/commit/bf0315b21c0e50a6bcef9b0796d2c6d56a196275))

- Remove stripe events shadow-table option
  ([`d70c78b`](https://github.com/increast-team/pp_datapipelines/commit/d70c78b5bd1579475fb63b57141a1951e90c860d))

### Testing

- Add column-consistency and grain-uniqueness checks for Microsoft Ads
  ([`879b7b4`](https://github.com/increast-team/pp_datapipelines/commit/879b7b426331693ef471e09917459f4531f71248))

- Cover event-sourced object merges in stripe pipelines
  ([`d822d4e`](https://github.com/increast-team/pp_datapipelines/commit/d822d4eda0d872a0e7fe0395da51f27ebc20620b))


## v7.14.0 (2026-10-26)

### Bug Fixes

- Block false bronze_ready asset emission on total ingest failure
  ([`170a9f1`](https://github.com/increast-team/pp_datapipelines/commit/170a9f1e149100b6e986e82d35c7e58bb5a3ebe3))

- Dedupe repeated GCP budget threshold alerts
  ([`39fadd0`](https://github.com/increast-team/pp_datapipelines/commit/39fadd05290e9dbe0d83d5ef20a2d575a04ed965))

- Key budget alert dedup by immutable billing/budget IDs
  ([`775c5b1`](https://github.com/increast-team/pp_datapipelines/commit/775c5b10d91f40fe1012ab8f642fb82d0790a2e3))

- Pass docuhero Amplitude id to dbt-test CI job, disable pre-launch freshness
  ([`3be3b56`](https://github.com/increast-team/pp_datapipelines/commit/3be3b5601a4224832132d748acfe52e53b93798a))

- Posthog data checks seed path and alert query
  ([#257](https://github.com/increast-team/pp_datapipelines/pull/257),
  [`a6bd909`](https://github.com/increast-team/pp_datapipelines/commit/a6bd9091f63424322aae6139db2dd44628cd9a60))

- Remove ConversionDelay grouping column
  ([`a518c1b`](https://github.com/increast-team/pp_datapipelines/commit/a518c1b2636afbbecca391eb40654d2f8ec8f492))

- Remove correlated subquery from posthog data checks alert
  ([#257](https://github.com/increast-team/pp_datapipelines/pull/257),
  [`a6bd909`](https://github.com/increast-team/pp_datapipelines/commit/a6bd9091f63424322aae6139db2dd44628cd9a60))

- Remove grouping-attribute columns, fix non-midnight date handling
  ([`cbe0b97`](https://github.com/increast-team/pp_datapipelines/commit/cbe0b9703dcc750e941215e4c414e895aabfa863))

- Resolve posthog seed path from the dag bundle
  ([#257](https://github.com/increast-team/pp_datapipelines/pull/257),
  [`a6bd909`](https://github.com/increast-team/pp_datapipelines/commit/a6bd9091f63424322aae6139db2dd44628cd9a60))

- **growthbook**: Tolerate missing tables in dbt tests, split rate limit, strip prerequisite
  conditions
  ([`8edfad1`](https://github.com/increast-team/pp_datapipelines/commit/8edfad1900fae0a63e60593d1db019c6c608ad38))

- **solidgate**: Address order status review comments
  ([`f7e839c`](https://github.com/increast-team/pp_datapipelines/commit/f7e839ce6b0aa276e2361baae70879a284c71a3b))

### Features

- Add docuhero Amplitude, Stripe and product registry entries
  ([`bbb33ba`](https://github.com/increast-team/pp_datapipelines/commit/bbb33ba6a4bdc332b3c2615ec5556a91d63de852))

- Add Microsoft Ads campaign performance ingestion
  ([`0f97a7e`](https://github.com/increast-team/pp_datapipelines/commit/0f97a7e32f1e4ef003546027dd81b4833eba7cb9))

- Onboard docuhero to AnyTrack, GA4 and PostHog connectors
  ([`39199ba`](https://github.com/increast-team/pp_datapipelines/commit/39199bacd063d295f84e735391b650f97dfd5270))

- **growthbook**: Add flags pipeline, feature flags and projects to bronze
  ([`e6aa2b2`](https://github.com/increast-team/pp_datapipelines/commit/e6aa2b2a3dfd5e9305221056aba083324be28176))

- **solidgate**: Add order status pipeline (Card Payments API /status) to bronze and silver
  ([`3b60753`](https://github.com/increast-team/pp_datapipelines/commit/3b60753ecf4d9698d76e28afeffd7668d8444f54))


## v7.13.1 (2026-10-21)

### Bug Fixes

- Resolve posthog seed path from the dag bundle
  ([#256](https://github.com/increast-team/pp_datapipelines/pull/256),
  [`d6e0210`](https://github.com/increast-team/pp_datapipelines/commit/d6e0210042543bdab16d1eb9b7228c51473ff358))


## v7.13.0 (2026-10-21)

### Bug Fixes

- Keep galaxy description of data_warehouse_bigquery
  ([`2868d0d`](https://github.com/increast-team/pp_datapipelines/commit/2868d0dd8797ecfb704f45e6b4d59f95d4650010))

- Run pr terraform plans without credentials
  ([`438601c`](https://github.com/increast-team/pp_datapipelines/commit/438601cbd52629e49ae2e7a622b161cf4305967c))

- Sync starburst catalogs with galaxy
  ([`b00d751`](https://github.com/increast-team/pp_datapipelines/commit/b00d751f6a80343c96effbe6960a1c3dfac48acd))

- **journey**: Net late Stripe refunds, current-only MRR, drop eventless users
  ([`af68042`](https://github.com/increast-team/pp_datapipelines/commit/af68042472c2dd0d1fb6289a42712ef1f0f0f180))

- **posthog**: Keep first-copy event fields on replay, shorten model docs
  ([`dff411f`](https://github.com/increast-team/pp_datapipelines/commit/dff411f6e8492719090fdb69bc612ae610152e6e))

- **posthog**: Pick the first event copy across all bronze partitions
  ([`14eef26`](https://github.com/increast-team/pp_datapipelines/commit/14eef260aadbc73ca0f4c2564e6f530d0ec226d2))

### Documentation

- Secret manager roles needed by terraform-ci
  ([`933ce1a`](https://github.com/increast-team/pp_datapipelines/commit/933ce1a521b58c35fc3623de97ced96cf6d69022))

- Trim int_amplitude_experiment_events comments
  ([`0ee7ec4`](https://github.com/increast-team/pp_datapipelines/commit/0ee7ec4daf0a5095d67a0e84760b0854a7158532))

- **posthog**: Document adding a project and the dev setup
  ([`aa748ed`](https://github.com/increast-team/pp_datapipelines/commit/aa748ed4d2ccc3131700bb432eca8731c8af550c))

- **posthog**: Drop PostHog API and int_posthog_events header comments
  ([`1b6d982`](https://github.com/increast-team/pp_datapipelines/commit/1b6d982df6eb7bb97aba3d5c2975293d043c786a))

### Features

- Add amplitude experiment events silver view for growthbook
  ([`2d5fd72`](https://github.com/increast-team/pp_datapipelines/commit/2d5fd7205102c87ea7aacffc428e12324c782bc8))

- Add Amplitude project IDs to product payment map
  ([`f126c72`](https://github.com/increast-team/pp_datapipelines/commit/f126c7259cfbbcc89b07cc44f744c617359ba7f7))

- Grant catalog_rw_role on new mongo catalogs, shared variables, refresh state on apply
  ([`2231798`](https://github.com/increast-team/pp_datapipelines/commit/2231798d7ced6c1d25fddcd651139775453fbf39))

- Manage stage and uat mongo catalogs in terraform
  ([`a5e3566`](https://github.com/increast-team/pp_datapipelines/commit/a5e3566aa02ada021cf25e9d2c1d0171fbfd47f6))

- Read experiment projects from shared amplitude source
  ([`4a359fc`](https://github.com/increast-team/pp_datapipelines/commit/4a359fcb383ed19b986bb52d0ca0869845e9ce11))

- Terraform for starburst galaxy
  ([`b004450`](https://github.com/increast-team/pp_datapipelines/commit/b004450858d38692e17fdf8c19671979bb2cf7d6))

- **journey**: Add publicregistry, reversly and fitday to the user journey mart
  ([`ac32c0e`](https://github.com/increast-team/pp_datapipelines/commit/ac32c0edc5833feeb7dc9bdf8c21c3d73d8fd8e8))

- **posthog**: Dedup PostHog export in silver, posthog_id in dim, export data checks
  ([`5e050a7`](https://github.com/increast-team/pp_datapipelines/commit/5e050a7c0a8d58d44a0c7ec7d98d42282af9e9d4))

### Refactoring

- Read all amplitude projects through one wildcard source
  ([`d2c5ac1`](https://github.com/increast-team/pp_datapipelines/commit/d2c5ac1ec6857c9e1a3c60f1a7bcf2f382e8d0c8))

- **posthog**: Replace merge with partition overwrite in int_posthog_events
  ([`53f5be8`](https://github.com/increast-team/pp_datapipelines/commit/53f5be8f88a9a6dafe10c00176e9f836fcde3ffd))

### Testing

- **posthog**: Check int_posthog_events has one row per bronze key
  ([`858146d`](https://github.com/increast-team/pp_datapipelines/commit/858146da1c716d497c15f663ceaec8d378e445a4))


## v7.12.0 (2026-10-13)

### Bug Fixes

- **journey**: Dedupe Solidgate orders routed to Stripe; link declined-only customers
  ([`618c608`](https://github.com/increast-team/pp_datapipelines/commit/618c6088e73c8e50d77e693134ce8afd52852e0d))

- **revenue,journey**: Count collected Stripe charges and server-only app users
  ([`36825e7`](https://github.com/increast-team/pp_datapipelines/commit/36825e7d1b072305305df6a0133d2c176b1fd0f6))

### Features

- Add AnyTrack reconciliation DAG + dbt models for files_editor billing data
  ([`f8e3be5`](https://github.com/increast-team/pp_datapipelines/commit/f8e3be55f0dfe55d448a5e4d1d942367f2dc1b5d))

- **journey**: Add gold user journey mart and reconciliation tests
  ([`4cf52f3`](https://github.com/increast-team/pp_datapipelines/commit/4cf52f31ce0d3a18717537e2ed35bb20993c55bb))

- **journey**: Add product registry seed, macros and amplitude source
  ([`acb6fb5`](https://github.com/increast-team/pp_datapipelines/commit/acb6fb59a2938153de601e496f9fb10a46410d5d))

- **journey**: Add silver journey and identity stitching models
  ([`3fa15c2`](https://github.com/increast-team/pp_datapipelines/commit/3fa15c2568b7cc3dec6c0f83d5e921aa30eedff0))

- **journey**: Payment_attempted stage; fix Stripe refunds and future churn
  ([`3696a60`](https://github.com/increast-team/pp_datapipelines/commit/3696a60c1b2f59d09b1848d861f7ce4c9e56f4ed))


## v7.11.0 (2026-10-09)

### Bug Fixes

- Add events source
  ([`ebd0556`](https://github.com/increast-team/pp_datapipelines/commit/ebd0556e96b01151cffdaa126f3e03413a03d437))

- Missing commit
  ([`e8a0b12`](https://github.com/increast-team/pp_datapipelines/commit/e8a0b12337665e7757f5e594090c9f728bf1561c))

- Update schema desc and seed has_ga4_export field
  ([`73d3a60`](https://github.com/increast-team/pp_datapipelines/commit/73d3a6077630918ca956ace588985ee7f50250c4))

### Chores

- Gcs landing bucket cleanup manifest and migrate script
  ([`d8592ce`](https://github.com/increast-team/pp_datapipelines/commit/d8592ce21117627b59d290f36b650b8d309cca7b))

### Features

- Add new stripe and solidgate subscribers models
  ([`878452a`](https://github.com/increast-team/pp_datapipelines/commit/878452a7dac7ee3fed517c53093e85f6648867cc))

- Ga4 property id in dim_product_payment_map
  ([`f819c0b`](https://github.com/increast-team/pp_datapipelines/commit/f819c0bf818488c8acbd4a859a091de674c74965))

### Testing

- Close DAG, config and AnyTrack backfill test coverage gaps
  ([`4c9a72f`](https://github.com/increast-team/pp_datapipelines/commit/4c9a72f1a8d2f04c903ce9fbd76bb1adbfb674d3))

- Parse the DagBag once in test_dag_example
  ([`785aa62`](https://github.com/increast-team/pp_datapipelines/commit/785aa62429afc7eb1ef13328b315f8b5ea10791b))


## v7.10.0 (2026-10-05)

### Bug Fixes

- Add missing on_schema_change='append_new_columns' to incremental models
  ([`a84caac`](https://github.com/increast-team/pp_datapipelines/commit/a84caac97c5c56971f62eb21f8e6fcffc1a63f75))

- Add pools for local deployments
  ([`9333df4`](https://github.com/increast-team/pp_datapipelines/commit/9333df4f1dec81b6df2d25bc6a099b10f21c1e64))

- Align solidgate_financial_entries start_date to 2024-01-01 to match sibling DAGs
  ([`46c3582`](https://github.com/increast-team/pp_datapipelines/commit/46c3582d0750da679d87db7a5ea1ca7e85c97c5c))

- Assign zendesk_support_pipeline to standard_mem_pool and align execution_timeout to 120m
  ([`80d6919`](https://github.com/increast-team/pp_datapipelines/commit/80d69198c1725f4e7c462b2d0f08094c80832708))

- Logging, handle missing threshold gracefully
  ([`ad5c768`](https://github.com/increast-team/pp_datapipelines/commit/ad5c76879dc05fdf0639d1579cdf596e738aac00))

- Make ChargebackhitHook fail fast on non-retryable HTTP status
  ([`b570fcf`](https://github.com/increast-team/pp_datapipelines/commit/b570fcfc5109c541614caa89a0eb2a105dbe310c))

- Only alert on real threshold crossings, ignore routine cost-sync messages
  ([`5ef1109`](https://github.com/increast-team/pp_datapipelines/commit/5ef1109f0a57fc5722068c681c726ce742e034b0))

- Redact HMAC signature from SolidgateBaseHook debug log
  ([`5276115`](https://github.com/increast-team/pp_datapipelines/commit/52761157d7bd4df2aaec3a1d09dd0900c1cb6a18))

- Resolve duplicate dbt resource, deprecated freshness syntax, stale test assertions, and invalid
  partition_type default
  ([`4217a27`](https://github.com/increast-team/pp_datapipelines/commit/4217a2797ac05365e63baa1c818f696e9e870e3e))

- Response.json() inside the try block
  ([`ac468b6`](https://github.com/increast-team/pp_datapipelines/commit/ac468b62bc63a6244b3719f8cda92f228de6fd40))

- Run PR checks only on main
  ([`9627cef`](https://github.com/increast-team/pp_datapipelines/commit/9627cefc973423867e37398be66a44f959f287ba))

- Stripe add non-retry and raise error
  ([`3f344c9`](https://github.com/increast-team/pp_datapipelines/commit/3f344c9b094da299d07a852b77648ee04b1f5670))

### Chores

- Remove dead code
  ([`32a4346`](https://github.com/increast-team/pp_datapipelines/commit/32a434628c628fc1be86650d1cb7a598ab0a1553))

### Documentation

- Document .env.ci regeneration step in README
  ([`2f85a3f`](https://github.com/increast-team/pp_datapipelines/commit/2f85a3f53d01fa7c248e21f155f720a4f4e50bfd))

### Features

- Add Cloud Function to relay GCP budget alerts to Slack
  ([`10bc558`](https://github.com/increast-team/pp_datapipelines/commit/10bc558de34787fca797f96fc56a42c1d721dd90))

- Add dbt source freshness checks to bronze sources
  ([`a6c7ead`](https://github.com/increast-team/pp_datapipelines/commit/a6c7eade4c21dda54848a3d9f4c849b6cfc469f4))

- Bronze self-healing schema migration
  ([`d292061`](https://github.com/increast-team/pp_datapipelines/commit/d292061bf79c279fa7a893a73412890c3fc890af))

- Fix secret embedding in serialised DAGs
  ([`94bf85d`](https://github.com/increast-team/pp_datapipelines/commit/94bf85dbde03fc42adfa362c5543a63d0f75ec56))

- Onboard 12 new AnyTrack products
  ([`079ec9b`](https://github.com/increast-team/pp_datapipelines/commit/079ec9bc36efe838ab6cae33d865934ac9ae6460))

- Solidgate historical subscriptions model
  ([`8adb183`](https://github.com/increast-team/pp_datapipelines/commit/8adb18353b79b0c974c65c4db43dfb36143fc824))

### Testing

- Add 5 verified relationships tests across the gold layer
  ([`5c18c04`](https://github.com/increast-team/pp_datapipelines/commit/5c18c04a141b0557946a4c14c1a38d9d80322df7))

- Add missing schema.yml docs/tests
  ([`e078562`](https://github.com/increast-team/pp_datapipelines/commit/e0785628ca05327910e69a2411044d00459d2552))

- Add unit tests for rate_limiter, queue_policy, and release_to_bigquery
  ([`3487ce4`](https://github.com/increast-team/pp_datapipelines/commit/3487ce412cf01abf0bcfea53b643b68e94b3c3f3))


## v7.9.0 (2026-09-28)

### Features

- Add transaction-grain GA4-Ads-payment attribution model
  ([`202affa`](https://github.com/increast-team/pp_datapipelines/commit/202affa5ff8680cb2b59ad8f9b34d32e7b16dc95))

- Align all AnyTrack products on one all-events merge
  ([`9907f92`](https://github.com/increast-team/pp_datapipelines/commit/9907f923cac2cf92a08798340aeb70f5289e3207))

- Consolidate AnyTrack webhook Cloud Run services, fix broken test import
  ([`c275cc9`](https://github.com/increast-team/pp_datapipelines/commit/c275cc9571dd9612dbbb229669d550c9c1bb3f82))


## v7.8.0 (2026-09-23)

### Bug Fixes

- Align CSV backfill click-id shape with live AnyTrack payloads
  ([`8e1f8d1`](https://github.com/increast-team/pp_datapipelines/commit/8e1f8d1f1b1341674224cecb81faa28c74f1a3b5))

- Remove duplicate dbt model entries blocking DAG import
  ([`51ee9e1`](https://github.com/increast-team/pp_datapipelines/commit/51ee9e1e0303b4c247c0f91526ea25ff1560cf36))

- Route AnyTrack backfill merges through the correct SQL by product
  ([`8f2faf1`](https://github.com/increast-team/pp_datapipelines/commit/8f2faf18eb505ae1caf5e9e67880cbdc4f41ab2f))

### Chores

- Remove test files and fix: dbt package perm issue
  ([`f2c580a`](https://github.com/increast-team/pp_datapipelines/commit/f2c580a335d4e15499487f072c0cccc5a0447f56))

- Run Stripe new endpoints for all accounts by default
  ([`1eb39f2`](https://github.com/increast-team/pp_datapipelines/commit/1eb39f263154ce9b2f57908070e7db6f996cfe50))

### Features

- Add gold.gross_profit_daily dbt model
  ([`b86a8d3`](https://github.com/increast-team/pp_datapipelines/commit/b86a8d30a19e05adf37ec2f4805eb48a48329f37))

- AnyTrack landing→release Avro step with native JSON load
  ([`9d22c3b`](https://github.com/increast-team/pp_datapipelines/commit/9d22c3b710ecab52a0784d35ad920b444865646b))

- Customer level MRR Stitching
  ([`2641a8d`](https://github.com/increast-team/pp_datapipelines/commit/2641a8dea9d4edca93a1d19aa4bf13285eb7bc0c))

- Ga4_payment_match_daily column type fixes
  ([`4115248`](https://github.com/increast-team/pp_datapipelines/commit/4115248372f31fbabaf3eab10e13d5d55e914c1f))

- Log staging row count for AnyTrack ingestion
  ([`e6bde86`](https://github.com/increast-team/pp_datapipelines/commit/e6bde865df0ad31bc0c6658def5714512f296f90))

- User subscription mart addition
  ([`98c0c30`](https://github.com/increast-team/pp_datapipelines/commit/98c0c30b984acdd060fe533cc8449db1ae56f763))


## v7.7.0 (2026-09-17)

### Bug Fixes

- Make dbt packages path CI-safe, disable Cosmos's DB-backed dbt-ls cache
  ([`fe82418`](https://github.com/increast-team/pp_datapipelines/commit/fe8241801779a0e4b01e9bd4b2dd4656725219e7))

- Scope stripe_customers unique test to (id, account)
  ([`e29310e`](https://github.com/increast-team/pp_datapipelines/commit/e29310e7874b3e1d30ccb128a25ce10e75086647))

### Chores

- Remove throwaway models used to verify DAG-only deploy fix
  ([`07153f5`](https://github.com/increast-team/pp_datapipelines/commit/07153f5df9ceabf2fbd7896e07ac272ee8c9d6e6))

### Features

- Add dedicated Cloud Run webhook service for files-editor AnyTrack tracking
  ([`4123109`](https://github.com/increast-team/pp_datapipelines/commit/41231091051ed9f568b19f59687579fc138ac3ea))

- Add net_revenue_daily dbt model
  ([`e9a87cf`](https://github.com/increast-team/pp_datapipelines/commit/e9a87cf15e54525918c4e3360adb73afcf95d108))

- Attribute real Solidgate/Stripe revenue to campaign_id for ROAS
  ([`877ab04`](https://github.com/increast-team/pp_datapipelines/commit/877ab0492c065802c70ad4749962e5719bb837b3))

- Carry AnyTrack click ids and campaign fields through the CSV backfill
  ([`ba7664c`](https://github.com/increast-team/pp_datapipelines/commit/ba7664cb8ac56429e69c1cd539e802388ada6acf))

- Ingest files-editor AnyTrack products into bronze via Airflow
  ([`3d36a27`](https://github.com/increast-team/pp_datapipelines/commit/3d36a2753d9e3cf18c89c471deb72cd502f488c6))

### Testing

- Add second throwaway model to verify DAG-only deploy fix
  ([`71ada32`](https://github.com/increast-team/pp_datapipelines/commit/71ada32b60b0559d4f9114edd3649ae67fe015f8))

- Add throwaway model to reproduce stale dbt task graph on DAG-only deploy
  ([`f786b14`](https://github.com/increast-team/pp_datapipelines/commit/f786b143493a0b45495ffe07e700583d52a71afa))


## v7.6.0 (2026-09-09)

### Features

- Add account level backfill in solidgate and chargebackhit pipelines
  ([`800578e`](https://github.com/increast-team/pp_datapipelines/commit/800578e9a5ba6f8ab3b984dc2d5495b11df9c1b3))

- Add combined MRR and ARR daily mart
  ([`0445a1c`](https://github.com/increast-team/pp_datapipelines/commit/0445a1cfd934582797a91b98fc69e2a78a75ab4e))

- Add dataset to solidgate, dbt, zendesk, anytrack, fx, chargebackhit, ga4
  ([`16b9801`](https://github.com/increast-team/pp_datapipelines/commit/16b98017f9cf85498c8bda692218ee3de3ddb81b))

- Add dataset to solidgate, dbt, zendesk, anytrack, fx, chargebackhit, ga4
  ([`7675dbd`](https://github.com/increast-team/pp_datapipelines/commit/7675dbdb015411d63c98ec27a9937e0065046b1e))

- Add dataset to solidgate, dbt, zendesk, anytrack, fx, chargebackhit, ga4
  ([`82fa63a`](https://github.com/increast-team/pp_datapipelines/commit/82fa63adff151a692548f5538a5df131a8f671d0))

- Add dataset to stripe and dbt
  ([`35fbe68`](https://github.com/increast-team/pp_datapipelines/commit/35fbe68e7ebec23db3f382b9c3219da72985bda8))

- Add silver solidgate_invoices_attempts
  ([`09acbcf`](https://github.com/increast-team/pp_datapipelines/commit/09acbcf76f5e065656f2765eb501326c0912bcd6))

- Stripe MRR Waterfall churn and reactivation addition
  ([`a7d6cb2`](https://github.com/increast-team/pp_datapipelines/commit/a7d6cb23a7762ca14909d6dd5c92dda69a40fc38))

### Testing

- Add schema and tests for MRR ARR mart
  ([`c96ba21`](https://github.com/increast-team/pp_datapipelines/commit/c96ba21e8f2ae1ca692f8b05e1c11f976ef7a664))


## v7.5.0 (2026-09-03)

### Bug Fixes

- Compute missing bgn currency from daily euro time constant
  ([`4b50e0f`](https://github.com/increast-team/pp_datapipelines/commit/4b50e0fa2078679d0d9bc69c38a77ad01ccbcf91))

- Each day landing file. fix OOM
  ([`35ba999`](https://github.com/increast-team/pp_datapipelines/commit/35ba99944d843c570c7ec9478df1f16ca5ae7656))

- Filter vamp transactions to succeeded and with captured dates
  ([`d909fba`](https://github.com/increast-team/pp_datapipelines/commit/d909fba619aca69692cfb4b588375d9ed1b09a5e))

- For daily runs, get last 5d created charges to capture updates
  ([`51f2c72`](https://github.com/increast-team/pp_datapipelines/commit/51f2c720fbf7093ba1e4e0548f244c655d9fd9ad))

- Increase vamp count threshold for alerts
  ([`bf4d8f9`](https://github.com/increast-team/pp_datapipelines/commit/bf4d8f93516bb9cb1aa9eac39ee90192bd393a47))

- Judge dbt test step from console summary instead of run_results.json
  ([`09b8021`](https://github.com/increast-team/pp_datapipelines/commit/09b8021e0006f904cd2872ba23c81588c61af952))

- Merge int_ga4_sessions on the session key instead of overwriting partitions
  ([`6c07745`](https://github.com/increast-team/pp_datapipelines/commit/6c07745165d133568f3754f26a00005a4da9e004))

- Pass dbt-test whenever tests themselves pass, regardless of post-test errors
  ([`fdb871a`](https://github.com/increast-team/pp_datapipelines/commit/fdb871a5b2a064e15b9d1c51539da312fabc7bf0))

- Rmv check table test hotfix
  ([`b3cc4a4`](https://github.com/increast-team/pp_datapipelines/commit/b3cc4a491e1c1fa836eea1045bbb2aed27cefbdb))

- Surface account fields
  ([`025b930`](https://github.com/increast-team/pp_datapipelines/commit/025b930d7bc298e023633e42e14c71385480540f))

- Surface account fields
  ([`03db441`](https://github.com/increast-team/pp_datapipelines/commit/03db4411f54dd19d7c639eb74cfa5cc4b8a397a5))

- Vamp 4dp
  ([`8f6dc78`](https://github.com/increast-team/pp_datapipelines/commit/8f6dc78f45c057cb3686af1f6c71e35e530daf52))

### Features

- Added requested fields into marts
  ([`8cc06c5`](https://github.com/increast-team/pp_datapipelines/commit/8cc06c5c681d1b79506f5d80e52edf832e596258))

- Added requested fields into marts
  ([`2544695`](https://github.com/increast-team/pp_datapipelines/commit/254469578c2af9033eeb1651d53cd8c05dff4c41))

- Added requested fields into marts
  ([`cab4a45`](https://github.com/increast-team/pp_datapipelines/commit/cab4a45ea4305e2b62d6e2407a2180addf2bff3e))

- Wait for Stripe bronze via ExternalTaskSensor
  ([`1e17dc8`](https://github.com/increast-team/pp_datapipelines/commit/1e17dc8c081a979e922a628c083e771736277db6))


## v7.4.0 (2026-08-28)

### Bug Fixes

- Pin gitpython version to fix removed author email param
  ([`cc33823`](https://github.com/increast-team/pp_datapipelines/commit/cc338230ba1f16478c2111255647baf2161e4053))

- Resolve marts issues
  ([`b315318`](https://github.com/increast-team/pp_datapipelines/commit/b31531847f14a376628889b7ab4a4eca3bac003a))

- Resolve marts issues
  ([`7376aee`](https://github.com/increast-team/pp_datapipelines/commit/7376aeea30c3361f6da99e921f5535525c15b2e3))

- Rmv test. fraud count may be counted double hence more than transactions
  ([`51e23bb`](https://github.com/increast-team/pp_datapipelines/commit/51e23bb0c92be9f5ddd5338f67b2554fd0c6bb67))

- Stub every env_var in the dbt manifest build, not just AIRFLOW_VAR_*
  ([`01edba4`](https://github.com/increast-team/pp_datapipelines/commit/01edba4fd378d1bbab36b76813b57d9551129de4))

- Undo astro run requirement change
  ([`68db099`](https://github.com/increast-team/pp_datapipelines/commit/68db099aa9dccc24286e4bb6a312408638e298c4))

- Undo wait pause
  ([`b142249`](https://github.com/increast-team/pp_datapipelines/commit/b142249ad4328cac393fe4dbaa65352a5291e2aa))

### Features

- Add 1-to-1 GA4 to Solidgate transaction stitch
  ([`cbb489d`](https://github.com/increast-team/pp_datapipelines/commit/cbb489deac3e1121309099738e51ded5487edadc))

- Add historical product/plan to Solidgate invoices and orders
  ([`1c874f4`](https://github.com/increast-team/pp_datapipelines/commit/1c874f4fc6128eb52a7a3957dba18f4f2fcb5c5d))

- Add merchant columns for refund rate
  ([`7275fb3`](https://github.com/increast-team/pp_datapipelines/commit/7275fb3ac7049f2249d5f65609db56c81c80bb9a))

- Add MRR by acquisition channel for Solidgate
  ([`887e934`](https://github.com/increast-team/pp_datapipelines/commit/887e93492a613be38dc472420e457f15c5e46ee3))

- Update charges from last 5d ago to update captured_dates
  ([`bc947ba`](https://github.com/increast-team/pp_datapipelines/commit/bc947ba13a359a924c4881d04302d02c58c4f1ba))

- Update data marts with requested columns
  ([`d43a8bf`](https://github.com/increast-team/pp_datapipelines/commit/d43a8bf19f9c06079c86cd70452c50bee9c37d7d))

- Update data marts with requested columns
  ([`5e0e896`](https://github.com/increast-team/pp_datapipelines/commit/5e0e896363114c380f3a6b09c03c33339f13dd58))

- Update data marts with requested columns
  ([`16545b4`](https://github.com/increast-team/pp_datapipelines/commit/16545b4c53016b391384a87f538de7f92c5f4cf3))

- Update data marts with requested columns
  ([`1e04241`](https://github.com/increast-team/pp_datapipelines/commit/1e042419667088cacfbf55539d36b7a76601a0da))

- Update datamarts with requested columns
  ([`f63d3d3`](https://github.com/increast-team/pp_datapipelines/commit/f63d3d38204bfd7a50189b929fe62bef9412fc04))


## v7.3.2 (2026-08-25)

### Bug Fixes

- Rmv balance transactions dupes
  ([`93eb050`](https://github.com/increast-team/pp_datapipelines/commit/93eb050dd911ec036feb45ef529792b0dbe3fb40))


## v7.3.1 (2026-08-25)

### Bug Fixes

- Compute dbt manifest during docker build
  ([`59d5e45`](https://github.com/increast-team/pp_datapipelines/commit/59d5e4585a338e44ba4d0e9a606885700dc19e3a))

- Stop relying on Docker-baked manifest for model detection
  ([`3f633a1`](https://github.com/increast-team/pp_datapipelines/commit/3f633a1d7e03ea7e9de418201061652f9e0ca95b))

- Update unique column for incremental
  ([`d8717a8`](https://github.com/increast-team/pp_datapipelines/commit/d8717a86b4edfe3e4f6a03786a8186da3055d874))

- Vamp alert message with PROD or DEV label
  ([`cdfbd5d`](https://github.com/increast-team/pp_datapipelines/commit/cdfbd5d3c98f04e2ed66850ce65f71d095f49fc6))

### Documentation

- Add view description
  ([`31ee4a6`](https://github.com/increast-team/pp_datapipelines/commit/31ee4a6b426ecdd6a9df97146d4722f150cc56f3))

- Clean comments, rmv unused config
  ([`ba8ba57`](https://github.com/increast-team/pp_datapipelines/commit/ba8ba57a28fa9a2af36daed4d35086d118555d63))

- Clean comments, rmv unused config
  ([`4022786`](https://github.com/increast-team/pp_datapipelines/commit/4022786af4cb3dd5b8c5e993a7c678f250b4b79f))


## v7.3.0 (2026-08-20)

### Bug Fixes

- Coalesce captured date with created date
  ([`d1bf01d`](https://github.com/increast-team/pp_datapipelines/commit/d1bf01d0c7dccc373118a76c914a179bd0b68bf6))

- Cosmos render manifest not whole project
  ([`d728a0d`](https://github.com/increast-team/pp_datapipelines/commit/d728a0df38a3688198bdd8e8d32ffe4d1e83c9b1))

- Exclude stripe charges without captured_date
  ([`69d33ed`](https://github.com/increast-team/pp_datapipelines/commit/69d33ed208d800ad44a30fcd5b8e5ef974955fd5))

- Exclude stripe charges/refunds/disputes without captured_date
  ([`6e1f6ac`](https://github.com/increast-team/pp_datapipelines/commit/6e1f6ac6cdb2c0084a0bcf4156ae72763f12f5f0))

- Exclude stripe charges/refunds/disputes without captured_date
  ([`452da8a`](https://github.com/increast-team/pp_datapipelines/commit/452da8a4f46eec5d2a8e34551c6d68fa5491ea1a))

- Format bugs
  ([`910952e`](https://github.com/increast-team/pp_datapipelines/commit/910952e8c1b2a08fd8878f990d060e55176f9e7d))

- Handle nulls before coalesce
  ([`b53aae5`](https://github.com/increast-team/pp_datapipelines/commit/b53aae5c5e4b2c2763e73f0f0cecfa138111ef33))

- Handle nulls before coalesce
  ([`87ad7b3`](https://github.com/increast-team/pp_datapipelines/commit/87ad7b312d875df24da1280e188465df5cb8a9c4))

- Rmv vamp tests. disputes can be double counted
  ([`aca650c`](https://github.com/increast-team/pp_datapipelines/commit/aca650cd3a431e15421430c51e5fcdb10513aac5))

- Round divisions
  ([`6a900b7`](https://github.com/increast-team/pp_datapipelines/commit/6a900b77600c70ded43d22249d2789f2d5a20daf))

- Slack only when vamp is not green
  ([`cba90ee`](https://github.com/increast-team/pp_datapipelines/commit/cba90ee12a44f1fa2503f8c139233cb631de3a3a))

- Slack only when vamp is not green
  ([`ac9e304`](https://github.com/increast-team/pp_datapipelines/commit/ac9e3041d36c4d5e65626cbdbb987fd06307807a))

- Use captured at frm balance transactions table for charges, disputes, refunds and marts
  ([`3ba9b63`](https://github.com/increast-team/pp_datapipelines/commit/3ba9b6308628d5b828d73acf804b37620909d7b6))

- Use captured at frm charges table for disputes, refunds and marts
  ([`96859f2`](https://github.com/increast-team/pp_datapipelines/commit/96859f2af60d4df632fb3c92667ce8719b0de816))

### Documentation

- Solidgate_refund_rate yml
  ([`9026de6`](https://github.com/increast-team/pp_datapipelines/commit/9026de609f7158370cccc1f251f41d62a7de72ab))

- Solidgate_refund_rate yml
  ([`40eb253`](https://github.com/increast-team/pp_datapipelines/commit/40eb2537f7d9e261a978125375be309fafd1cd96))

### Features

- Add charges captured at frm balance transactions table
  ([`b1b7481`](https://github.com/increast-team/pp_datapipelines/commit/b1b748106751bb5958c62b231f10b500f569182a))

- Add churn and reactivation to the MRR waterfall
  ([`781d644`](https://github.com/increast-team/pp_datapipelines/commit/781d644f941da9e991e10694ddcfdf3b7b6cdc28))

- Add expansion and contraction to the MRR waterfall
  ([`67a8027`](https://github.com/increast-team/pp_datapipelines/commit/67a8027ea4456dd4637ae97d29aa406b9232aa12))

- Add geo_country in solidgate orders
  ([`0a93bba`](https://github.com/increast-team/pp_datapipelines/commit/0a93bbab2df08408dc6cac2ba30d18487d54920a))

- Add mastercard refund analysis models
  ([`333b876`](https://github.com/increast-team/pp_datapipelines/commit/333b8761446fc419c7c4511191c53f8bf4b33f3e))

- Add solidgate error codes seed + surfaced more solidgate columns in silver
  ([`47fe2bf`](https://github.com/increast-team/pp_datapipelines/commit/47fe2bf6408a38e81fd9b526887f869db1250914))

- Add Stripe dbt bronze and silver layer
  ([`7049ce7`](https://github.com/increast-team/pp_datapipelines/commit/7049ce72beca56f382129cca2b83fa8de5d5e00b))

- Add Zendesk dbt bronze and silver layer
  ([`880be7b`](https://github.com/increast-team/pp_datapipelines/commit/880be7bf7cc17056f3e95bd6cfc577a4d0d46f23))

- Create refund rate table
  ([`08bb2f2`](https://github.com/increast-team/pp_datapipelines/commit/08bb2f24aa6301a3e282910f2ef2900b21f77359))

- Generalize GA4 to Solidgate stitching across products
  ([`01ebae1`](https://github.com/increast-team/pp_datapipelines/commit/01ebae1a4a8feefe31844690faa3dfbfb0675302))

- Map solidgate error codes to readable values
  ([`233827e`](https://github.com/increast-team/pp_datapipelines/commit/233827e097126dd51f9e9dc117775029a42626cf))

- Precompile dbt manifest
  ([`e081409`](https://github.com/increast-team/pp_datapipelines/commit/e0814092370863113d7fe82f54c7f8ff8e410176))

- Precompile dbt manifest
  ([`cfd7fb0`](https://github.com/increast-team/pp_datapipelines/commit/cfd7fb0e9c77933ad400a5897af06d2b961629ab))

- Precompile dbt manifest
  ([`0b85c6b`](https://github.com/increast-team/pp_datapipelines/commit/0b85c6b50437c746698a70486ff6e6dd5dff0af2))

### Refactoring

- Stitch GA4 to subscription invoice orders
  ([`c5aa12f`](https://github.com/increast-team/pp_datapipelines/commit/c5aa12f12b655a76aef8b4350cf3f166fd65bcef))


## v7.2.0 (2026-08-14)

### Bug Fixes

- Exclude rdr from vamp disputes
  ([`16da6e8`](https://github.com/increast-team/pp_datapipelines/commit/16da6e8ddc2dc8f57d33ac98049d02d807b70a28))

- Filter visa in disputes for vamp
  ([`03cdc77`](https://github.com/increast-team/pp_datapipelines/commit/03cdc7749b1704e3cd6698eca4b54c2cbe348006))

- Filter visa in farud warnings+refunds for vamp
  ([`c8f3a87`](https://github.com/increast-team/pp_datapipelines/commit/c8f3a8744015e826b63020be88ae26653eb2de31))

- Filter visa in farud warnings+refunds for vamp
  ([`40c9ba1`](https://github.com/increast-team/pp_datapipelines/commit/40c9ba122630332a022c1528793d60b209a71b04))

- Readd rate limiting
  ([`341185e`](https://github.com/increast-team/pp_datapipelines/commit/341185e1154183375ee190600d49e6bc694e0e46))

- Rmv test slack notification workflow
  ([`4c4b463`](https://github.com/increast-team/pp_datapipelines/commit/4c4b4635c7cfdc0210bb7221d720b7918cbc0cd0))

- Rmv tests coz VAMP is calculated twice if it does have fraud warning and dispute
  ([`e46d08a`](https://github.com/increast-team/pp_datapipelines/commit/e46d08ad9fd01588695e30a9ead0fb8d4640f43f))

- Rmv unused import
  ([`4f63c40`](https://github.com/increast-team/pp_datapipelines/commit/4f63c40de0e9d878e7b2b6948a85459881e9231b))

- Ticket_comments include sideload
  ([`21981aa`](https://github.com/increast-team/pp_datapipelines/commit/21981aa517c146b477fb2639977b4517ee2061e2))

### Chores

- Cleanup
  ([`5ae804e`](https://github.com/increast-team/pp_datapipelines/commit/5ae804eab952d846906fa35ef599edf8db1878ce))

### Documentation

- Comment for stripe backfill
  ([`05c1009`](https://github.com/increast-team/pp_datapipelines/commit/05c10099c88fb6dc17652ca79eec0c5e14152278))

- Updated readme git best practices
  ([`e209e29`](https://github.com/increast-team/pp_datapipelines/commit/e209e29e3c63fcb8ea4ea2462f8f7dd0dbaa985a))

- Updated readme git best practices
  ([`3b988d9`](https://github.com/increast-team/pp_datapipelines/commit/3b988d9e8cf1e0c62e14aaf13d603a7d64d87ea5))

- Updated readme git best practices
  ([`03cb8eb`](https://github.com/increast-team/pp_datapipelines/commit/03cb8eb29827b6bd74a7b178b5ffae05d9a25bc5))

- Updated readme git best practices
  ([`1afa332`](https://github.com/increast-team/pp_datapipelines/commit/1afa3329901f850b05a48713d50990e5ae9fdac1))

- Updated readme git best practices
  ([`eb716e8`](https://github.com/increast-team/pp_datapipelines/commit/eb716e8c254f1f431e26c238a47c60c95073a454))

- Updated readme git best practices
  ([`90863d4`](https://github.com/increast-team/pp_datapipelines/commit/90863d4022d85eac2565b6ad0bb50fc4176d4ebe))

- Updated readme git best practices
  ([`0c3f3e0`](https://github.com/increast-team/pp_datapipelines/commit/0c3f3e0c2d3895f0fb87a0f6e033bd693fa77bd7))

- Updated readme git best practices
  ([`5c4e180`](https://github.com/increast-team/pp_datapipelines/commit/5c4e1804b6891c089555e504f7217fe2b4dc7dc8))

### Features

- Accounts param for Stripe Ingestion
  ([`e1c63ba`](https://github.com/increast-team/pp_datapipelines/commit/e1c63baafb2d4dde9876d10c6e0d92ef6d727ce7))

- Add endpoint param to Stripe pipeline
  ([`b3b994b`](https://github.com/increast-team/pp_datapipelines/commit/b3b994b9360fa35a5467a10ef6328672cbbccd9e))

- Add zendesk support pipeline DAG
  ([`d7803bd`](https://github.com/increast-team/pp_datapipelines/commit/d7803bda9d38dc8ccfdf60d974348de8ebd01f93))

- Daily vamp alerts to slack v1 + update requirements
  ([`0cff1da`](https://github.com/increast-team/pp_datapipelines/commit/0cff1da7d7caa11f80bcd62636b87f6ac2776e09))

- Daily vamp alerts to slack v1 + update requirements
  ([`da164c3`](https://github.com/increast-team/pp_datapipelines/commit/da164c346cc143c6a40e509aa739a9347cc327fe))

- Daily vamp alerts to slack v1 + update requirements
  ([`d7d8e7a`](https://github.com/increast-team/pp_datapipelines/commit/d7d8e7a2d7bdff6dd6ac0b72019bb18cc34033ef))

- Daily vamp alerts to slack v1 + update requirements
  ([`608b8fa`](https://github.com/increast-team/pp_datapipelines/commit/608b8faca1ccccd5bf20f1128e30a72fb578b968))

- Daily vamp alerts to slack v1 + update requirements
  ([`f161297`](https://github.com/increast-team/pp_datapipelines/commit/f16129723334e56c21f730b5b772524680d30aec))

- Daily vamp alerts to slack v1 + update requirements
  ([`5567bb0`](https://github.com/increast-team/pp_datapipelines/commit/5567bb0f5efa187558f0342a1d0bb176f2510a71))

- Observability changes and readme
  ([`8bce0a3`](https://github.com/increast-team/pp_datapipelines/commit/8bce0a348d52c10781a71d26356550782da65d47))

- Ticket_comments param changes
  ([`9f5dbe1`](https://github.com/increast-team/pp_datapipelines/commit/9f5dbe1473c4889c0d1b677832ad1862fa3cc024))

- Update vamp status indicator metric
  ([`7e15d27`](https://github.com/increast-team/pp_datapipelines/commit/7e15d27d3016a6663a84598f4c4d492aecd024b7))


## v7.1.0 (2026-08-11)

### Features

- New Business MRR metric
  ([`40a9772`](https://github.com/increast-team/pp_datapipelines/commit/40a977248422c7f7e0fb8bd2ae3c9fea630ee97b))


## v7.0.0 (2026-08-11)

- Initial Release
