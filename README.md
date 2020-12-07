# Transfer API Documentation 

The transfer service uploads and downloads data to/from FAIRSCAPE. The data is stored in an S3 compatiable object store (default is [Minio](https://min.io/)). The service mints automatically mints identifiers associated with the uploaded object. 

# Endpoints
 - **/data**
 - **/data/{PID}**

# /data

Takes data from the user and uploads the given file to minio and mints an identifier in mongo associated with the file. 

## POST

The user passes in a file to upload as well as json-ld metadata. The service uploads the file to minio and mints an identifier with the given metadata as well as a distribution tag tying the file to the identifier. The service returns the minted identifier to the user. 

### Parameters 

 - **Files**
	 - **Metadata**
	 - **Files**
	 - **sha-256**

```python:
metadata = {
	'name':'Example Dataset',
	'@type':'Dataset',
	'author':'Madeup Person',
	'description':'Sample'
	}
upload_response = requests.post(
        'http://clarklab.uvarc.io/transfer/data',
        files = {
            'files':open(file_path,'rb'),
            'metadata':json.dumps(metadata),
            'sha-256':'abcdefghi'
        },
        headers = {"Authorization": token}
    )
```

## /data/{PID}

Endpoint for download, updating, or deleting previously uploaded file. 

## GET

Download file associated with given PID. 

```console
$ curl http://clarklab.uvarc.io/data/ark:99999/test-id 
``` 

## PUT

This endpoint is used to upload a new version of a previously uploaded file. The user passes in a file to upload.  The service uploads the file to minio and mints a new distribution tag tying the file to the identifier. The service returns the minted identifier to the user. 

### Parameters 

 - **Files**
	 - **Files** (can be list or single file) 
	 - **sha-256**

```python:
upload_response = requests.put(
        'http://clarklab.uvarc.io/transfer/data/ark:99999/test-id',
        files = {
            'files':open(file_path,'rb'),
            'sha-256':'abcdehgia'
        },
        headers = {"Authorization": token}
    )
```

## DELETE

Delete a file associated with given PID from FAIRSCAPE.

```console
$ curl --request DELETE 
       --url http://clarklab.uvarc.io/data/ark:99999/test-id 
``` 
