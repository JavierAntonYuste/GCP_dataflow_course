#! /bin/bash -ex


python -m pipeline_read --project=$PROJECT_NAME \
    --region=$REGION \
    --staging_location=$BUCKET_NAME/staging \
    --temp_location=$BUCKET_NAME/temp \
    --input=$BUCKET_NAME/input.csv \
    --output=$PROJECT_NAME:customers.information
