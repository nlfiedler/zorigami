# Diagnose and Repair

### Repair

#### Copy missing packs from one store to another

```shell
curl -g -X POST -H 'Content-Type: application/json' \
     -d '{"query":"mutation{restorePacks(sourceId:\"SOURCE_PACK\",targetId:\"TARGET_PACK\"){checksum}}"}' \
     http://192.168.1.2:8080/graphql
```
