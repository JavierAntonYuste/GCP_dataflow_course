#! /bin/bash -ex


python -m join_tables --project=$PROJECT_NAME \
    --region=$REGION \
    --staging_location=$BUCKET_NAME/staging \
    --temp_location=$BUCKET_NAME/temp \
    --input-customers=$BUCKET_NAME/customers.csv \
    --input-orders==$BUCKET_NAME/orders.csv \
    --output=$PROJECT_NAME:customers.information_advanced
