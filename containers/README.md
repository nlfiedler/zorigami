# Test Containers

Several docker containers are defined here for use in testing the application.

## Minio

Build and start, login to console (http://docker-host:9001) using the credentials found in the compose file, create two access keys, copying the values to `.env` in the parent directory. Define the environment variables needed by the test code, as shown below:

```
MINIO_ENDPOINT=http://docker-host:9000
MINIO_REGION=us-west-1
MINIO_ACCESS_KEY_1=ijeoG4wjVC52yWYi
MINIO_SECRET_KEY_1=WVMq0H2tDuu6l9araNBRaVhfElexfHPr
MINIO_ACCESS_KEY_2=TiRWLuOIVwpRFwxJ
MINIO_SECRET_KEY_2=cVQkhGZ1ybiGC0rXYtJcpOmsaAGDRABf
```

## SFTP

Build and start, use S/FTP as usual with the credentials found in the compose file. Define the environment variables needed by the test code in the `.env` file as shown below:

```
SFTP_ADDR=docker-host:2222
SFTP_USER=foo
SFTP_PASSWORD=pass
SFTP_BASEPATH=/upload
```
