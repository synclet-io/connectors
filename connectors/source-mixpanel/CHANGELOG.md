# Changelog

## 0.1.0 (2026-10-30)


### Features

* **source-mixpanel:** add Mixpanel source ([a4e4b14](https://github.com/synclet-io/connectors/commit/a4e4b149af53b930247e1da00fb2d1f140d416ca))


### Bug Fixes

* **source-mixpanel:** accept Airbyte credentials without option_title ([09eb443](https://github.com/synclet-io/connectors/commit/09eb443c1410d82d2a94dcbe8f0e17c828371891))
* **source-mixpanel:** cap Retry-After at an hour ([ab9684f](https://github.com/synclet-io/connectors/commit/ab9684f64ca848250b2bca4aecc2af9f763f8b1a))
* **source-mixpanel:** export resumes from cursor minus export_lookback_window ([ea869db](https://github.com/synclet-io/connectors/commit/ea869dbfc44bc86bcbbd3481ae4d2099ad08c8fe))
* **source-mixpanel:** export starts at the UTC day of the cursor when it is earlier ([26a0fd0](https://github.com/synclet-io/connectors/commit/26a0fd01f08b586fc810f38ed769b8722ff1fdf6))
* **source-mixpanel:** keep a cohort_members cursor per cohort so a new cohort is read in full ([0e7470c](https://github.com/synclet-io/connectors/commit/0e7470c4abeadb741d56335e1d337b3771d331ae))
* **source-mixpanel:** no total timeout on the export stream of a date window ([dc4c719](https://github.com/synclet-io/connectors/commit/dc4c719aaba254745e9ab6880b398b9131aeaaf8))
