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
.Trash, .bun, .cache, .cargo, .npm, .rustup, .tmp, .vscode, Library, **/Downloads, **/node_modules, **/target, **/tmp
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

Configuration of the application is partly accomplished using environment variables. Defining the data sets, pack stores, local time zone, and bucket naming policy is done through the web interface (or GraphQL if you like).

- **DATABASE_TYPE**
  - Either `rocksdb` (the default) or `sqlite` to choose between RocksDB and SQLite
- **DB_PATH**
  - Path for the database files; defaults to `./tmp/database`
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

1. From the default directory page, copy the **Tenant ID**, will need that later for connecting.
1. Navigate to **App registrations** and register a new application (such as `zorigami-server-backup`).
1. From the **Overview** page of the application, copy the **Application ID** for later.
1. Navigate to **Certificates & secrets** and create a new _shared secret_, copy the **Value** for later.
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
1. Navigate to the **Access Control (IAM)** page and click **Add role assignment**
1. On the **Members** tab, click **Select members** and enter the name of the _application_ created earlier into the search field, select that entry.
1. Click **Review and assign** and now you can test the connection.

### Google Cloud Setup

Note that Google seems to have little in the way of limits on the number of buckets or objects. As such, any bucket naming policy should be compatible.

How to create a new project and get the service account credentials file.

1. Create a new project in Google Cloud Platform
1. Navigate to the **Firestore** page under _DATABASES_
   - Do **not** select _Filestore_ under _STORAGE_, that is a different service
1. Create a _Standard Edition_ with _Native mode_ Firestore database (there can be only one)
1. Navigate to the **Credentials** page under _APIs & Services_
1. Click _Create credentials_ and select **Service account**
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

## Immutable Backups (Object Lock / WORM)

Immutable backups defend against a ransomware scenario in which the machine
running the zorigami server is compromised: even with full store credentials,
an attacker cannot delete or overwrite pack objects that are under a storage-side
retention lock until that lock expires. This is _Tier 1_ of
[the ransomware protection plan](specs/0009-Ransomware-Protection.md).

Object lock is configured per store with a single property, **lock_days**:

- **lock_days** — the number of days each uploaded pack (and database archive)
  is held under a compliance-mode object-lock retention. Absent or `0` means no
  lock (the default; existing stores are unaffected). Compliance mode means no
  principal — not even the cloud account root — can delete or overwrite the
  object before its retention expires.

Currently supported on the **Amazon S3**, **MinIO**, **Azure Blob Storage**, and
**Google Cloud Storage** stores. The SFTP and local stores have no WORM primitive
and are explicitly _unprotected_; setting `lock_days` on any store other than
Amazon, MinIO, Azure, or Google is rejected.

In the web interface the setting appears as the **Lock Days** field on the store
form for those four store types, just below the retention policy; the local and
SFTP forms instead note that they offer no object-lock protection.

The mechanism per backend: S3/MinIO use compliance-mode Object Lock; Azure uses a
**locked, time-based immutability policy** on each blob; Google uses a **locked
per-object retention** (`retention.mode = Locked`). All three are compliance-grade:
the lock cannot be shortened or removed before it expires.

### Requirements and constraints

- **The lock capability must be provisioned before use.** On **S3/MinIO** this is
  Object Lock, and on **Google** it is object retention: both can only be enabled
  when a bucket is created, and zorigami turns them on automatically for the buckets
  it creates once `lock_days > 0`, so an object-locking store should start from
  freshly created buckets. (S3 Object Lock also enables versioning; Google object
  retention does not.) On **Azure** the capability is _version-level immutability_
  (immutable storage with versioning), which is an account/container provisioning
  step that the blob API cannot enable — turn it on for the storage account (or
  container) out of band first. In all cases, if zorigami finds an existing
  bucket/container that lacks the capability, it fails the upload immediately with
  an actionable error rather than silently uploading unprotected objects.
- **A storage lifecycle rule is required to reclaim space.** zorigami does **not**
  delete locked pack objects itself — an app-issued delete cannot remove a
  still-locked object, and on the versioned backends it would not truly reclaim
  space even once unlocked (on S3 a delete merely writes a delete marker; on Azure
  a base-blob delete retains prior versions). Reclamation is therefore delegated to
  a storage lifecycle rule that expires objects after they age out of the lock
  window: on **S3**, a rule expiring current + noncurrent versions and expired
  delete markers; on **Azure**, a lifecycle-management rule expiring blob
  **versions** (Azure has no delete-marker concept); on **Google**, an Object
  Lifecycle Management rule with an age condition (retention is honored, so locked
  objects are not deleted early). Without such a rule, aged-out locked objects
  accumulate and are never reclaimed. The pruner leaves the pack records for locked
  stores in place rather than claiming a deletion it did not perform.
- **lock_days must be ≥ the store's pack retention (DAYS).** zorigami rejects a
  store whose `lock_days` is shorter than its `PackRetention::DAYS` value, so the
  lock never outlives the point at which a pack ages out. With retention set to
  `ALL` (never prune), any `lock_days` is accepted.
- **Compliance mode is unforgiving.** A mistaken `lock_days` cannot be shortened
  for objects already written — they remain locked for the full window. Choose the
  value deliberately; it is a direct cost/rigidity trade-off, since locked objects
  cannot be deleted early no matter what.
- **lock_days cannot be reduced for a locked store.** Because `lock_days` lives in
  the free-form store properties, an update that omitted it would silently drop
  protection for future packs. zorigami rejects an update that lowers or removes
  `lock_days` on a store that currently has it set; raise it freely, but a
  reduction must be a deliberate, explicit change (and does not affect objects
  already written).

### Setup notes

- **Amazon S3** — no extra console steps beyond the
  [Amazon S3 Setup](#amazon-s3-setup) above; set `lock_days` on the store and let
  zorigami create lock-enabled buckets. (Credential separation so the backup
  identity cannot delete at all is _Tier 2_ and is not yet implemented.)
- **MinIO** — the deployment must run a MinIO/S3-compatible server that honors S3
  Object Lock. Verify against your specific server before relying on it.
- **Azure** — enable _version-level immutability_ before setting `lock_days`, then set
  `lock_days` on the store; each uploaded blob is committed under a locked, time-based
  immutability policy. The Entra ID app used by the store still needs the **Storage
  Blob Data Contributor** role (see [Azure Blob Storage](#azure-blob-storage) above).

  **Recommended: use a dedicated storage account** with **account-level**
  version-level immutability (turn on **blob versioning** and **version-level
  immutability support** under the account's Data protection settings). Account-level
  provisioning means every container zorigami creates automatically supports
  immutability, so the store works out of the box. Two caveats make a dedicated
  account the safe choice:

  - _Do not enable it at the container level and let zorigami create the container._
    The blob data-plane API cannot turn on version-level immutability, so zorigami
    would create a plain, non-immutable container and then fail every upload against
    it. If you must use container-level immutability, **pre-create** the immutable
    container yourself before pointing a store at it.
  - _Do not mix locked and non-locked stores in an account that has account-level
    version-level immutability._ That setting forces blob versioning account-wide, and
    on a versioned account a non-locked store's pruning cannot truly reclaim space (a
    base-blob delete leaves prior versions behind). Keep object-locked stores in their
    own account, governed by a lifecycle rule as above.
- **Google Cloud Storage** — no extra console steps beyond the
  [Google Cloud Setup](#google-cloud-setup) above; set `lock_days` on the store and
  let zorigami create the bucket with object retention enabled. Each uploaded object
  gets a locked per-object retention. Object retention does not enable versioning, so
  a straightforward Object Lifecycle Management rule (age-based deletion) reclaims
  space once the retention expires. The service account still needs permission to
  create buckets and objects (see the Google setup above).
