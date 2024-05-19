# Deploying

## Deploy via Docker

The base directory contains a `docker-compose.yml` file which is used to build the application in stages and produce a relatively small final image.

On the build host:

```shell
docker compose build --pull --build-arg BASE_URL=http://192.168.1.2:8080
docker image rm 192.168.1.2:5000/zorigami
docker image tag zorigami-app 192.168.1.2:5000/zorigami
docker push 192.168.1.2:5000/zorigami
```

On the docker host, with a production version of the `docker-compose.yml` file:

```shell
docker compose down
docker compose up --build -d
```

## Deploy to macOS

This assumes that you are building on the Mac computer in question, hence `localhost`. Feel free to change the port `8000` to whatever works best for you.

### Build

```shell
fvm flutter clean
fvm flutter pub get
env BASE_URL=http://localhost:8000 fvm flutter pub run environment_config:generate
fvm flutter build web
cargo build --release
```

Server binary is `target/release/server` and web contents are in `build/web`

### Install / Update

Create the plist file as shown below then run the following commands.

```shell
ps -ef | grep -i zorigami
launchctl kill SIGTERM "gui/$(id -u)/zorigami"
ps -ef | grep -i zorigami
mkdir -p ~/Applications/Zorigami
mv target/release/zorigami ~/Applications/Zorigami
rsync -vcr build/web ~/Applications/Zorigami/
launchctl enable "gui/$(id -u)/zorigami"
launchctl kickstart -p "gui/$(id -u)/zorigami"
ps -ef | grep -i zorigami
```

May need to run the `enable` and `kickstart` commands twice due to code signing error.

Recommended set of excludes that ignores a bunch of Mac stuff, directories that tend to have huge binary files no one cares about, Dart/Flutter, and Rust files:

```
.Trash, .android, .rustup, .cargo, Library, **/Downloads, **/node_modules, **/target, fvm
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

## Cloud Storage

### Amazon S3 Setup

1. Create user that will act on behalf of zorigami
1. Give specific permissions, not assign to a group
1. Add **AmazonS3FullAccess** permission (search for _s3_)
1. Add **AmazonDynamoDBFullAccess** permission (search for _dynamo_)
1. View the newly created user
1. Find the **Security credentials** tab
1. Add a new **Access key** for this user
1. Select _Application running outside AWS_ when asked
1. Download the `.csv` file of the newly created key

### Azure Blob Storage

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

How to create a new project and get the service account credentials file.

1. Create a new project in Google Cloud Platform
1. Navigate to the **Firestore** page under _DATABASES_
    * Do **not** select _Filestore_ under _STORAGE_, that is a different service
1. Create a _native_ Firestore database (there can be only one)
1. Navigate to **APIs & Services**
1. Open **Credentials** screen
1. Click _CREATE CREDENTIALS_ and select _Service_ account
1. Enter an account name and optional description
1. Click **CREATE** button
1. Navigate to **IAM & Admin / IAM** and click the **GRANT ACCESS** button
1. Under the _Assign roles_ section of the dialog...
1. Start typing the name of the service account and select the result
1. Under the _Cloud Storage_ category and select _Storage Admin_
    * The service account needs to be able to create buckets and objects.
1. Click **ADD ANOTHER ROLE** button
1. Under the _Firebase_ category select _Firebase Admin_
    * The service account needs to be able to create and update documents.
1. Click **SAVE** button
1. Navigate to **IAM & Admin / Service Accounts**
1. Click on the _Actions_ 3-dot button and select _Create key_
1. Choose *JSON* and click **CREATE** button
