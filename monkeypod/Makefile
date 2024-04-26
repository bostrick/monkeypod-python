.PHONY: deploy test
URL=https://us-central1-nimble-volt-421318.cloudfunctions.net/ps-test

deploy:
	gcloud functions deploy ps-test \
		--gen2 \
		--no-allow-unauthenticated \
		--runtime=python312 \
		--region=us-central1 \
		--source=. \
		--entry-point=my_http_function \
		--trigger-http \
		--set-secrets=STRIPE_API_KEY=stripe_api:1  \
		--set-secrets=/etc/secrets/:service-account.json=nimble-volt-e33:1

test:
	curl -H "Authorization: bearer ${GCLOUD_ID_TOKEN}" ${URL}
