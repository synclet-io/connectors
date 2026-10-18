# source-google-sheets

Google Sheets source on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk): one full-refresh stream per grid sheet, every column a nullable string. Authenticates with OAuth, a service account key or Application Default Credentials.

```bash
task test CONNECTOR=source-google-sheets
task build CONNECTOR=source-google-sheets
task spec CONNECTOR=source-google-sheets
```
