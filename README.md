ORS+ Transfer Service


Endpoints
  - /data
    - GET
    - POST
    - DELETE
  - /bucket
    - GET
    - DELETE
  - /run-job
    -post

# Examples

## data endpoint

### POST /data

```
{
  "files": [open("file a"),open("file b")],
  "metadata":{
                "@type":"Dataset"
                ....
              }
}
```



### GET /data/{ark}
  Downloads file of given ark
  
### DELETE /data/{ark}
  Deletes file of given ark off of minio

## bucket endpoint


### GET /bucket/{bucketName}
  creates bucket with bucketName
### DELETE /bucket/{bucketName}
  deletes bucket
  


## run-job endpoint

### POST /run-job

```
{
 {
    "Job Identifier":"ark:99999/54c75a4e-850b-47a0-a638-19e647f2a1eb",
    "Dataset Identifier":"ark:99999/6521e5fd-5ce7-4681-b35e-dd575c7c293a"
}
}
```
