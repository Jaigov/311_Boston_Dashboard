######
## simple script for now to deploy functions
## deploys all, which may not be necessary for unchanged resources
######

# setup the project
gcloud config set project group2-ba882

# schema setup
echo "======================================================"
echo "deploying the schema setup"
echo "======================================================"

gcloud functions deploy mlops-caseduration-schema-setup \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source /home/gunjan21/BA882-Team02-project/ml/pipeline/functions/schema-setup \
    --stage-bucket group2-ba882-functions \
    --service-account group2-ba882@group2-ba882.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB 

# the training module
echo "======================================================"
echo "deploying the trainer"
echo "======================================================"

gcloud functions deploy mlops-caseduration-trainer \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source /home/gunjan21/BA882-Team02-project/ml/pipeline/functions/trainer \
    --stage-bucket group2-ba882-functions \
    --service-account group2-ba882@group2-ba882.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1GB

# the predictions function
echo "======================================================"
echo "dynamic prediction endpoint"
echo "======================================================"

gcloud functions deploy mlops-caseduration-prediction \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source /home/gunjan21/BA882-Team02-project/ml/pipeline/functions/prediction \
    --stage-bucket group2-ba882-functions \
    --service-account group2-ba882@group2-ba882.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1GB

# the offline batch function
echo "======================================================"
echo "bulk/batch prediction job"
echo "======================================================"

gcloud functions deploy mlops-caseduration-batch \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source /home/gunjan21/BA882-Team02-project/ml/pipeline/functions/batch \
    --stage-bucket group2-ba882-functions \
    --service-account group2-ba882@group2-ba882.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1GB