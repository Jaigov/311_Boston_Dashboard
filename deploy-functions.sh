########################################
## Script for now to deploy functions ##
########################################

# setup the project
gcloud config set project group2-ba882

# schema setup
echo "======================================================"
echo "deploying the schema setup"
echo "======================================================"

gcloud functions deploy group2-schema-department_assignment \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source /home/gunjan21/BA882-Team02-project/functions/schema-department_assignment \
    --stage-bucket group2-ba882-project \
    --service-account group2-ba882@group2-ba882.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB \
	--timeout 540s
	
gcloud functions deploy group2-schema-location \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source /home/gunjan21/BA882-Team02-project/functions/schema-location \
    --stage-bucket group2-ba882-project \
    --service-account group2-ba882@group2-ba882.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB \
	--timeout 540s
	
gcloud functions deploy group2-schema-requests \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source /home/gunjan21/BA882-Team02-project/functions/schema-requests \
    --stage-bucket group2-ba882-project \
    --service-account group2-ba882@group2-ba882.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB \
	--timeout 540s

gcloud functions deploy group2-schema-response_time \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source /home/gunjan21/BA882-Team02-project/functions/schema-response_time \
    --stage-bucket group2-ba882-project \
    --service-account group2-ba882@group2-ba882.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB \
	--timeout 540s

gcloud functions deploy group2-schema-status_history \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source /home/gunjan21/BA882-Team02-project/functions/schema-status_history \
    --stage-bucket group2-ba882-project \
    --service-account group2-ba882@group2-ba882.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB \
	--timeout 540s

# extract data
echo "======================================================"
echo "deploying the extractor"
echo "======================================================"

gcloud functions deploy group2-extract \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point main \
    --source /home/gunjan21/BA882-Team02-project/functions/extract \
    --stage-bucket group2-ba882-project \
    --service-account group2-ba882@group2-ba882.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 4GB \
	--timeout 540s


# load data into raw and changes into stage
echo "======================================================"
echo "deploying the loader"
echo "======================================================"

gcloud functions deploy group2-load \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point main \
    --source /home/gunjan21/BA882-Team02-project/functions/load \
    --stage-bucket group2-ba882-project \
    --service-account group2-ba882@group2-ba882.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1GB \
	--timeout 540s