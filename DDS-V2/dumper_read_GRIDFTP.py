import os
import globus_sdk

# Set up authentication
client_id = 'YOUR_CLIENT_ID'
client_secret = 'YOUR_CLIENT_SECRET'
redirect_uri = 'https://auth.globus.org/v2/web/auth-code'
client = globus_sdk.NativeAppAuthClient(client_id=client_id)
client.oauth2_start_flow(redirect_uri=redirect_uri)

# Get authorization code
print('Please go to this URL and login: {0}'
      .format(client.oauth2_get_authorize_url()))
auth_code = input('Enter the code you get after login here: ').strip()
token_response = client.oauth2_exchange_code_for_tokens(auth_code)

# Set up transfer client
transfer_token = token_response.by_resource_server['transfer.api.globus.org']['access_token']
transfer_client = globus_sdk.TransferClient(authorizer=globus_sdk.AccessTokenAuthorizer(transfer_token))

# Connect to GridFTP directory
endpoint_id = 'YOUR_ENDPOINT_ID'
path = '/path/to/directory'
try:
    transfer_client.endpoint_autoactivate(endpoint_id)
    transfer_client.operation_ls(endpoint_id, path=path)
    print('Successfully connected to GridFTP directory!')
except Exception as e:
    print('Error connecting to GridFTP directory:', e)