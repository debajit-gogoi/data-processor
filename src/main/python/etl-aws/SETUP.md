## Steps for setting up AWS cost and usage pipeline

### Setup Python

- Install required libraries
    - `pip3 install boto3 'elasticsearch<7.10.2' pandas`

### Setup configuration files

- AWS profile and Elasticsearch information in `config.ini`

### Prepare Elasticsearch indexes

- Create index `skiff-nextgen-aws-usage`

    1. Add pipeline settings
        ```
        PUT skiff-nextgen-aws-usage
        {
          "settings": {
            "default_pipeline": "skiff-nextgen-aws-usage-pipeline"
          }
        }
        ```

    2. Specify the creation of `_id` field
        ```
        PUT _ingest/pipeline/skiff-nextgen-aws-usage-pipeline
        {
          "description": "Updates _id with field values at the time of ingestion",
          "processors": [
            {
              "set": {
                "field": "_id",
                "value": "{{startDate}}{{account}}{{service}}"
              }
            }
          ]
        }
        ```
    3. Update Mapping
       ```
       POST skiff-nextgen-aws-usage/_mapping
       {
         "properties": {
           "startDate": {
             "type": "date"
           },
           "endDate": {
             "type": "date"
           },
           "account": {
             "type": "text",
             "fields": {
               "keyword": {
                 "type": "keyword",
                 "ignore_above": 256
               }
             }
           },
           "service": {
             "type": "text",
             "fields": {
               "keyword": {
                 "type": "keyword",
                 "ignore_above": 256
               }
             }
           },
           "description": {
             "type": "text",
             "fields": {
               "keyword": {
                 "type": "keyword",
                 "ignore_above": 256
               }
             }
           },
           "amount": {
             "type": "float"
           },
           "currency": {
             "type": "text",
             "fields": {
               "keyword": {
                 "type": "keyword",
                 "ignore_above": 256
               }
             }
           }
         }
       }
       ```


- Create index `skiff-nextgen-aws-account`

    1. Create default pipeline `skiff-nextgen-aws-account-pipeline`
    ```
    PUT _ingest/pipeline/skiff-nextgen-aws-account-pipeline
    {
      "description": "Updates _id with fields at the time of ingestion",
      "processors": [
        {
          "set": {
            "field": "_id",
            "value": "{{Id}}"
          }
        }
      ]
    }
    ```

    2. Specify default pipeline
    ```
    PUT skiff-nextgen-aws-account
    {
      "settings": {
        "default_pipeline": "skiff-nextgen-aws-account-pipeline"
      }
    }
    ```

    3. Update Mapping
    ```
    POST skiff-nextgen-aws-account/_mapping
    {
      "properties": {
        "Id": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "Arn": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "Email": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "Name": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "Status": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "JoinedMethod": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "JoinedTimestamp": {
          "type": "date",
          "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
        }
      }
    }
    ```

### Run script

- `python3 aws.py --config config.ini --debug --start-datetime 2022-07-11 --end-datetime 2022-07-12`