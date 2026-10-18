# destination-google-sheets

Google Sheets destination on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk): one sheet per stream, headers from the first record, overwrite or append. Authenticates with OAuth, a service account key or Application Default Credentials.

```bash
task test CONNECTOR=destination-google-sheets
task build CONNECTOR=destination-google-sheets
task spec CONNECTOR=destination-google-sheets
```
