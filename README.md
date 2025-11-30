# Cloud-Computing-Assignment-3-Backend

Name: Samrudhi Prashant Baldota
NetID:sb10212

Name: Debika Piriya Dharma Lingam
NetID:dd3873

This backend implements all serverless components required for a fully functional photo album search system.
It exposes two Lambda functions:

1️.index_photos (LF1)

Triggered by S3 ObjectCreated events
This function:

Extracts labels using Amazon Rekognition

Reads custom labels from x-amz-meta-customLabels

Stores metadata in OpenSearch (photos index)

Saves:

objectKey

bucket

createdTimestamp

labels (rekognition + custom labels)

2️. search_photos (LF2)

Invoked by API Gateway /search?q=xxx
This function:

Sends the user query to Amazon Lex V2

Lex extracts multi-value slot "keywords"

Performs OR-based search in OpenSearch

Returns matching photo metadata + S3 public URLs
