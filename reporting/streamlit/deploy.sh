gcloud config set project group2-ba882

echo "======================================================"
echo "build (no cache)"
echo "======================================================"

docker build --no-cache -t gcr.io/group2-ba882/streamlit-poc .

echo "======================================================"
echo "push"
echo "======================================================"

docker push gcr.io/group2-ba882/streamlit-poc

echo "======================================================"
echo "deploy run"
echo "======================================================"


gcloud run deploy streamlit-poc \
    --image gcr.io/group2-ba882/streamlit-poc \
    --platform managed \
    --region us-central1 \
    --allow-unauthenticated \
    --service-account group2-ba882@group2-ba882.iam.gserviceaccount.com \
    --memory 1Gi