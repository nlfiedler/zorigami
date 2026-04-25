# Deploying

## Deploy via Docker

The base directory contains a `Dockerfile` file which is used to build the application in stages and produce a relatively small final image.

On the build host:

```shell
docker build -t zorigami-app .
docker image rm 192.168.1.4:5000/zorigami
docker image tag zorigami-app 192.168.1.4:5000/zorigami
docker push 192.168.1.4:5000/zorigami
```

On the server, with a production version of the `docker-compose.yml` file:

```shell
docker compose down
docker compose up --build -d
```

## Deploy to macOS

This assumes that you are building on the Mac computer in question, hence `localhost`. Feel free to change the port `8000` to whatever works best for you.

### Build

```shell
cargo build --release
bun run codegen
bunx vite build
```

Server binary is `target/release/zorigami` and web contents are in `dist`

### Install / Update

Create the plist file as shown below then run the following commands.

```shell
ps -ef | grep -i zorigami
launchctl kill SIGTERM "gui/$(id -u)/zorigami"
ps -ef | grep -i zorigami
mkdir -p ~/Applications/Zorigami
mv target/release/zorigami ~/Applications/Zorigami
rsync -vcr dist ~/Applications/Zorigami/
launchctl enable "gui/$(id -u)/zorigami"
launchctl kickstart -p "gui/$(id -u)/zorigami"
ps -ef | grep -i zorigami
```

May need to run the `enable` and `kickstart` commands twice due to code signing error.

Recommended set of excludes that ignores a bunch of Mac stuff and directories that tend to have large binary files:

```
.Trash, .bun, .cache, .cargo, .npm, .rustup, .tmp, Library, **/Downloads, **/node_modules, **/target, fvm
```

### plist file

An example launch agent plist file for macOS that goes in `~/Library/LaunchAgents/zorigami.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
    <dict>
        <key>Label</key>
        <string>zorigami</string>
        <key>Program</key>
        <string>/Users/USERNAME/Applications/Zorigami/zorigami</string>
        <key>WorkingDirectory</key>
        <string>/Users/USERNAME/Applications/Zorigami</string>
        <key>RunAtLoad</key>
        <true/>
        <key>EnvironmentVariables</key>
        <dict>
            <key>DB_PATH</key>
            <string>/Users/USERNAME/Library/Application Support/Zorigami/dbase</string>
            <key>ERROR_DB_PATH</key>
            <string>/Users/USERNAME/Library/Application Support/Zorigami/errors.db</string>
            <key>HOST</key>
            <string>0.0.0.0</string>
            <key>PORT</key>
            <string>8000</string>
            <key>RUST_LOG</key>
            <string>server=info</string>
        </dict>
        <key>StandardErrorPath</key>
        <string>/Users/USERNAME/Library/Application Support/Zorigami/error.log</string>
        <key>StandardOutPath</key>
        <string>/Users/USERNAME/Library/Application Support/Zorigami/output.log</string>
    </dict>
</plist>
```

## Configuration

Configuration of the application is partly done via environment variables. Defining the data sets, pack stores, and bucket naming policy is done through the web interface (or GraphQL if you like).

- **DB_PATH**
  - Path for the RocksDB database files; defaults to `./tmp/database`
- **ERROR_DB_PATH**
  - Path for the SQLite database that records errors; defaults to `./tmp/errors.db`
- **HOST**
  - Host address on which to listen for incoming HTTP connections; defaults to `127.0.0.1`
- **PORT**
  - Port on which to bind for incoming connections; defaults to `3000`
- **PASSPHRASE**
  - Passphrase for encrypting the pack files and database snapshots; defaults to `keyboard cat`
- **RUST_LOG**
  - Logging level as defined by the [env_logger](https://crates.io/crates/env_logger) crate. For example, `RUST_LOG=info` logs everything at the `info`, `warn`, or `error` logging levels, while excluding anything that is `debug` or `trace` level.

## Cloud Storage

### Amazon S3 Setup

Note that prior to 2024, AWS accounts were limited to 100 buckets. Since then, the limit has been raised to 10,000 buckets per account. As such, it is advisable to select the **random pool** _bucket naming policy_ with a number no larger than 10,000.

1. Navigate to the **IAM** console
1. Create user that will act on behalf of zorigami
1. Choose _Attach policies directly_, do not assign to a group
1. Add **AmazonS3FullAccess** permission (search for _s3_)
1. Add **AmazonDynamoDBFullAccess** permission (search for _dynamo_)
1. Click **Next**, review the details, then click **Create user**
1. Click on the user's name in the list
1. Find the **Security credentials** tab
1. Add a new **Access key** for this user
1. Select _Application running outside AWS_ when asked
1. Copy the access key and secret key and save them in a safe place

### Azure Blob Storage

Note that Azure seems to have little in the way of limits on the number of buckets or objects. As such, any bucket naming policy should be compatible.

How to create a new storage account and get the access key.

1. From the Azure portal, find **Storage accounts** and select it
1. Find and click the **Create** button
1. Create a new resource group, choose a storage account name
1. Select a suitable region
1. Select the lowest cost redundancy (LRS)
1. Click the **Advanced** button
1. Select the _Cool_ option under **Access tier**
1. Click the **Networking** button and review the default selections
1. Click the **Data protection** button and turn off the _soft delete_ options
1. Click the **Encryption** button and review the default selections
1. Click the **Review** button and then click **Create**
1. Once the deployment is done, click the button to view the resource.
1. Find the **Access keys** option on the left panel
1. Copy the _Storage account name_ and _Key_ value from **key1**

### Google Cloud Setup

Note that Google seems to have little in the way of limits on the number of buckets or objects. As such, any bucket naming policy should be compatible.

How to create a new project and get the service account credentials file.

1. Create a new project in Google Cloud Platform
1. Navigate to the **Firestore** page under _DATABASES_
   - Do **not** select _Filestore_ under _STORAGE_, that is a different service
1. Create a _Standard Edition_ with _Native mode_ Firestore database (there can be only one)
1. Navigate to the **Credentials** page under _APIs & Services_
1. Click _CREATE CREDENTIALS_ and select **Service account**
1. Enter an account name and optional description
1. Click **Create and continue** button
1. In the _Permissions_ section find _Cloud Storage_ category and select _Storage Admin_
   - The service account needs to be able to create buckets and objects.
1. Click **Add another role** button
1. This time find the _Firebase_ category and select _Firebase Admin_
   - The service account needs to be able to create and update documents.
1. Click **Done** button
1. Navigate to **IAM & Admin / Service Accounts**
1. Click on the _Actions_ 3-dot button (next to the new account) and select _Manage keys_
1. Open the **Add key** dropdown and choose _Create new key_
1. Choose _JSON_ and click **Create** button
