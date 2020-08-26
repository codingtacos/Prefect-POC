AWS_ACCESS_KEY_ID="$(aws configure get aws_access_key_id --profile tempus-bioinformatics-pipeline-development)"
AWS_SECRET_ACCESS_KEY="$(aws configure get aws_secret_access_key --profile tempus-bioinformatics-pipeline-development)"
AWS_DEFAULT_REGION="$(aws configure get region --profile tempus-bioinformatics-pipeline-development)"
AWS_SESSION_TOKEN="$(aws configure get aws_session_token --profile tempus-bioinformatics-pipeline-development)"

export PREFECT__CONTEXT__SECRETS__AWS_CREDENTIALS=$( jq -n \
                  --arg key_id "$AWS_ACCESS_KEY_ID" \
                  --arg secret_key "$AWS_SECRET_ACCESS_KEY" \
                  '{ACCESS_KEY: $key_id, SECRET_ACCESS_KEY: $secret_key}' )

rm .env
# echo "PREFECT__CONTEXT__SECRETS__AWS_CREDENTIALS='$PREFECT__CONTEXT__SECRETS__AWS_CREDENTIALS'" | tee .env
echo "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" >> .env
echo "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" >> .env
echo "AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN" >> .env
echo "AWS_DEFAULT_REGION=us-east-1" >> .env
