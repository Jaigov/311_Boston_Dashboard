# set the project
gcloud config set project group2-ba882


echo "======================================================"
echo "deploying the ml dataset: case duration"
echo "======================================================"

gcloud functions deploy ml-case-duration \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source /home/gunjan21/BA882-Team02-project/prefect/functions/case-duration \
    --stage-bucket group2-ba882-functions \
    --service-account group2-ba882@group2-ba882.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1GB  \
    --timeout 300s 