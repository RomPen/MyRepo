import requests
import msal
from datetime import datetime, timedelta
import json
import pytz

class PowerBIError(Exception):
    
    def __init__(self, fail_type, message = None):
        
        self.fail_types = {'Secret Expired' : f"Azure application token has now expired. Please contact Nexus support to request token update. Expiry date: {message}",
                           'Unable to authenticate' : f"Unable to authenticate. Error:\n{message}",
                           'No table input' : "",
                           'Workspace not found': f"The workspace {message} is not found. Please make sure you add PowerBI Python service principal to your working area",
                           'Dataset not found': f"Dataset not found. Please make sure you put in correct dataset name or dashboard name"}
        
        self.message = self.fail_types.get(fail_type, None)
        super().__init__(self.message)
    
    def __str__(self):
        
        return self.message
    
    
class PowerBI:
    
    """
    Custom NewDay class to make refresh dataset calls to api.powerbi.com
    
    :param workspace_name: name of the Power BI workspace
    
    The workflow of the class on init is as follows:
        
        -get credentials from Vault
        -check credentials have not expired
        -authenticate against Azure application (PowerBI-Python)
        -get access token from Azure application
        -get workspace_id using the access token
        -load datasets and reports using workspace_id and the access token
    """
    
    
    _REFRESH_URL_TEMPLATE = "https://api.powerbi.com/v1.0/myorg/groups/{}/datasets/{}/refreshes"
    _TENANT_ID = 'your_org_tenant_id
    _SCOPE = ['https://analysis.windows.net/powerbi/api/.default']

    def __init__(self, workspace_name):
        
        self._get_credentials()
        self._authenticate()
        
        self.workspace_name = workspace_name
        
        self.workspace_id = self._get_workspace_id(workspace_name)

        self._load_datasets_and_reports(self.workspace_id)

    @property
    def header(self):
        
        token_is_valid = self._check_token_still_valid()
        
        if not token_is_valid:
            print(f"PowerBI API: access token expired. obtaining a new one")
            self._authenticate()
            
        return {'Authorization' : f'Bearer {self.access_token}',
                'Content-Type'  : 'application/json'}
    
    def _get_credentials(self):
        
        return client_id, secret, secret_id, secret_expires
                   
    def _authenticate(self):
        
        client_id, secret, secret_id, secret_expires = self._get_credentials()
                   
        print(f"PowerBI API:successfully obtained Power BI secret from Vault")
        print(f"PowerBI API:secret expiry date: {secret_expires}")
        
        #Check secret has not expired
        today = datetime.now().date()
        if today > secret_expires:
            raise PowerBIError('Secret Expired', secret_expires)
        
        
        self.app = msal.ConfidentialClientApplication(client_id,
                                                      authority = 'https://login.microsoftonline.com/'+self._TENANT_ID,
                                                      client_credential = secret)
    
        #acquire_token_for_client
        response = self.app.acquire_token_for_client(scopes = self._SCOPE)
        access_token = response.get('access_token')

        error = response.get('error') or not access_token
        
        if error:
            raise PowerBIError('Unable to authenticate', response)
        else:
            print("PowerBI API:successful authentication")
            
        
        self.access_token = access_token
        self.token_acquired_at = datetime.now().astimezone(pytz.timezone('Europe/London')).replace(microsecond = 0, tzinfo = None)
        self.token_expires_at = self.token_acquired_at + timedelta(seconds = response.get('expires_in', 3600))
        
        print(f"PowerBI API:token acquired at {self.token_acquired_at}")
        print(f"PowerBI API:token expires at {self.token_expires_at}")
    
    def _check_token_still_valid(self):
        
        return datetime.now().astimezone(pytz.timezone('Europe/London')).replace(microsecond = 0, tzinfo = None) + timedelta(seconds = 30) < self.token_expires_at
    
    def _get_refresh_url(self, workspace_id, dataset_id):
        
        return self._REFRESH_URL_TEMPLATE.format(workspace_id, dataset_id)
    
    def _load_datasets_and_reports(self, workspace_id):
        
        self.datasets = self.list_datasets_in_workspace(workspace_id)
        self.reports = self.list_reports_in_workspace(workspace_id)
        
        self.datasets_map = {x['name']:x['id'] for x in self.datasets}
        self.reports_map = {x['name']:x['datasetId'] for x in self.reports}
    
    def _get_workspace_id(self, workspace_name):
        
        groups = self.list_workspaces()
        groups_map = {x['name']:x['id'] for x in groups}
        
        if workspace_name not in groups_map:
            print('Service principal has access to the following workspaces only:\n',',\n'.join(groups_map.keys()))
            raise PowerBIError('Workspace not found', workspace_name)
        
        workspace_id = groups_map.get(workspace_name, '')
        
        print(f"PowerBI API: workspace_id for {workspace_name} - {workspace_id}")
        
        return workspace_id
        
    def _get_dataset_id(self, dataset_or_report_name):
        
        #from reports
        dataset_id = self.reports_map.get(dataset_or_report_name, '')
        
        if not dataset_id:
            #from datasets
            dataset_id = self.datasets_map.get(dataset_or_report_name, '')
            
        if not dataset_id:
            raise PowerBIError('Dataset not found')
        
        print(f"PowerBI API: dataset_id for {dataset_or_report_name} - {dataset_id}")
        
        return dataset_id
    
    def list_datasets_in_workspace(self, workspace_id):
        
        list_datasets = requests.request("GET", f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets", headers = self.header).json().get('value', [])
        
        return list_datasets
    
    def list_reports_in_workspace(self, workspace_id):

        list_reports = requests.request("GET", f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports", headers = self.header).json().get('value', [])
        
        return list_reports
    
    def list_workspaces(self):
        
        return requests.request("GET", "https://api.powerbi.com/v1.0/myorg/groups", headers=self.header, timeout = 30).json().get('value', [])
    
    def refresh_dataset(self,
                        dataset_or_report_name,
                        payload = {}):
        

        dataset_id = self._get_dataset_id(dataset_or_report_name)
        
        refresh_url = self._get_refresh_url(self.workspace_id, dataset_id)
        
        payload_final = {'refreshRequest': 'y'}.update(payload)
        
        response = requests.request("POST", refresh_url, headers = self.header, data = payload_final)
        
        print(f"PowerBI API: response status code - {response.status_code}")
        
        return response.status_code
