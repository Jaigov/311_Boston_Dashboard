# set the project
gcloud config set project group2-ba882


echo "======================================================"
echo "deploying the case duration training function"
echo "======================================================"

gcloud functions deploy ml-case-duration-train \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point main \
    --source /home/gunjan21/BA882-Team02-project/ml/functions/ml-case_duration_train \
    --stage-bucket group2-ba882-functions \
    --service-account group2-ba882@group2-ba882.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1GB  \
    --timeout 300s 

echo "======================================================"
echo "deploying the case duration inference function"
echo "======================================================"

gcloud functions deploy ml-case-duration-serve \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source /home/gunjan21/BA882-Team02-project/ml/functions/ml-case_duration_serve \
    --stage-bucket group2-ba882-functions \
    --service-account group2-ba882@group2-ba882.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1GB  \
    --timeout 300s 