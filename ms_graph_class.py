import os
import msal
import requests



class digital_process:
    def __init__(self):
        self.tenant_id = 'nope'
        self.authority = 'https://login.microsoftonline.com/' + self.tenant_id
        self.client_id = 'client id'
        self.client_secret = 'secret'
        self.scopes = ['https://graph.microsoft.com/.default']
        self.scopes1 = ['https://graph.microsoft.com/Mail.ReadWrite']
        self.user_id = 'email@email.com'
        self.endpoint = f'https://graph.microsoft.com/v1.0/users/{self.user_id}'
        
    def get_access_token(self):
        
        """
        Gets access token from Azure to connect to MS Graph.
        """
    
        app = msal.ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=self.authority)
        
        result = None
        result = app.acquire_token_silent(self.scopes, account=None)
        
        if not result:
            print(
                "No suitable token exists in cache. Let's get a new one from Azure Active Directory.")
            result = app.acquire_token_for_client(scopes=self.scopes)
        
        if "access_token" in result:
             print("Ok, we got a new token")
             return result
         
    def send_mail(self, to_user_email: str, subject: str, message: str, 
                                cc_user_email='email@email.com'):
        """
        Connects to MS Graph via client secret and token.  Gets a new token if neccessary.
        Sends mail.
        """
        
        global email_msg
        
        assert isinstance(to_user_email, str), 'to_user_email should be string'
        assert isinstance(subject, str), 'subject should be string'
        assert isinstance(message, str), 'message should be string'
        assert isinstance(cc_user_email, str), 'cc_user_email should be string'
        
        result = digital_process().get_access_token()
        
        if "access_token" in result:
            #endpoint = self.endpoint + '/sendMail'
            
            email_msg = {
              "message": {
                "subject": f"{subject}",
                "body": {
                  "contentType": "Text",
                  "content": f"{message}"
                },
                "toRecipients": [
                  {
                    "emailAddress": {
                      "address": f"{to_user_email}"
                    }
                  }
                ],
                "ccRecipients": [
                  {
                    "emailAddress": {
                      "address": f"{cc_user_email}"
                    }
                  }
                ]
              },
              "saveToSentItems": "false"
            }
    
            r = requests.post(self.endpoint + '/sendMail',
                              headers={'Authorization': 'Bearer ' + result['access_token']}, 
                              json=email_msg)
            if r.ok:
                print('Sent email successfully')
            else:
                print(r.json())
        else:
            print(result.get("error"))
            print(result.get("error_description"))
            print(result.get("correlation_id"))
            
    def list_folders(self):
        
        result = digital_process().get_access_token()
        headers = {'Authorization': 'Bearer ' + result['access_token']}
        
        response = requests.get(self.endpoint + '/mailFolders', headers=headers)
        if response.status_code != 200:
            raise Exception(response.json())
        
        mailfolder_items = response.json()['value']
        print('The searchable mailfolders are:')
        for mailfolder in mailfolder_items:
            mailfolder = mailfolder['displayName']
            print(mailfolder)
            
    def list_mail_attachments(self, email_limit=10, folder_to_search='inbox',
                              search_term=''):
        
        result = digital_process().get_access_token()
        headers = {'Authorization': 'Bearer ' + result['access_token']}

        
        params = {
            'top': email_limit, # max is 1000 messages per request
            'select': 'subject,hasAttachments',
            'search': f'"attachment: {search_term}"',
            'count': 'true'
        }
        
        response = requests.get(self.endpoint + f'/mailFolders/{folder_to_search}/messages', headers=headers, params=params)
        if response.status_code != 200:
            raise Exception(response.json())
        
        response_json = response.json()
        response_json.keys()
        
        emails = response_json['value']
        attachment_dict = {}
        attachment_list = []
        for email in emails:
            if email['hasAttachments']:
                email_id = email['id']
                try:
                    response = requests.get(
                        self.endpoint + '/messages/{0}/attachments'.format(email_id),
                        headers=headers
                    )
            
                    attachment_items = response.json()['value']
                    for attachment in attachment_items:
                        file_name = attachment['name']
                        attachment_id = attachment['id']
                        attachment_dict['filename'] = file_name
                        attachment_dict['id'] = attachment_id
                        attachment_list.append(attachment_dict.copy())
                
                except Exception as e:
                    print(e)
        return attachment_list
                
            
    def download_email_attachments(self, message_id, headers, save_folder=os.getcwd()):

        """
        Connects to MS Graph and downloads attachments to a specific folder.

        Subquery that is used in:
        get_mail_attachments(), get_specific_attachment()
        """
        #endpoint = 'https://graph.microsoft.com/v1.0/users/email@email.com'
        try:
            response = requests.get(
                self.endpoint + '/messages/{0}/attachments'.format(message_id),
                headers=headers
            )
    
            attachment_items = response.json()['value']
            for attachment in attachment_items:
                file_name = attachment['name']
                attachment_id = attachment['id']
                attachment_content = requests.get(
                    self.endpoint + '/messages/{0}/attachments/{1}/$value'.format(message_id, attachment_id), 
                    headers=headers
                )
                print('Saving file {0}...'.format(file_name))
                with open(os.path.join(save_folder, file_name), 'wb') as _f:
                    _f.write(attachment_content.content)
            return True
        except Exception as e:
            print(e)
            return False
        
    def get_mail_attachments(self, email_limit=10, folder_to_search='inbox', save_folder=os.getcwd()):
        
        """
        Connects to MS Graph and searches and downloads all attachments
        within the email_limit.
        """
        result = digital_process().get_access_token()
        headers = {'Authorization': 'Bearer ' + result['access_token']}

        
        params = {
            'top': email_limit, # max is 1000 messages per request
            'select': 'subject,hasAttachments',
            'filter': 'hasAttachments eq true',
            'count': 'true'
        }
        
        response = requests.get(self.endpoint + f'/mailFolders/{folder_to_search}/messages', headers=headers, params=params)
        if response.status_code != 200:
            raise Exception(response.json())
        
        response_json = response.json()
        response_json.keys()
        
        response_json['@odata.count']
        
        emails = response_json['value']
        for email in emails:
            if email['hasAttachments']:
                email_id = email['id']
                digital_process().download_email_attachments(email_id, headers, save_folder=save_folder)
                
    
    def get_specific_attachment(self, attachment_name:str, folder_to_search='inbox', 
                                save_folder=os.getcwd(), get_path=True):
        
        """
        Connects to MS Graph and searches and downloads a specific attachment to the
        specified folder.
        """
        
        result = digital_process().get_access_token()
        headers = {'Authorization': 'Bearer ' + result['access_token']}

        
        params = {
            'top': 1000, # max is 1000 messages per request
            'select': 'subject,hasAttachments',
            'search': f'"attachment: {attachment_name}"'
        }
        
    
        response = requests.get(self.endpoint + f'/mailFolders/{folder_to_search}/messages', headers=headers, params=params)
        if response.status_code != 200:
            raise Exception(response.json())
        
        response_json = response.json()
        
        emails = response_json['value']
        for email in emails:
            if email['hasAttachments']:
                email_id = email['id']
                digital_process().download_email_attachments(email_id, headers, save_folder=save_folder)
        
                if get_path == True:
                    for filename in os.listdir(save_folder):
                        f = os.path.join(save_folder, attachment_name)
                        if os.path.isfile(f):
                            return f


    def move_message(self,message_id,move_to='Archive'):

        """
        connects to MS Graph and moves message to specified folder based on message ID
        """


        result = digital_process().get_access_token()
        headers = {'Authorization': 'Bearer ' + result['access_token']}

        
        params = {
            'top': 1000, # max is 1000 messages per request
            'select': 'subject,hasAttachments',
            'search': f'"attachment: {attachment_name}"'
        }
        
    
        if "access_token" in result:
            #endpoint = self.endpoint + '/sendMail'
            
            move_msg = {
                "destinationId": f"{move_to}"
            }
              
    
            r = requests.post(self.endpoint + f'/messages/{message_id}/move',
                              headers={'Authorization': 'Bearer ' + result['access_token']}, 
                              json=move_msg)
            if r.ok:
                print('moved message with ID ' + message_id + ' to the ' + move_to + 'folder')
            else:
                print(r.json)
        else:
            print(result.get("error"))
            print(result.get("error_description"))
            print(result.get("correlation_id"))
            
            
            
    def get_message_id(self,attachment_name):

        """
        returns the message ID of an email based on the attachment name
        """
        
        result = digital_process().get_access_token()
        headers = {'Authorization': 'Bearer ' + result['access_token']}
        
        response = requests.get(self.endpoint + f'/messages?$search="attachment:{attachment_name}"', headers=headers)
        if response.status_code != 200:
            raise Exception(response.json())
        
        mailfolder_items = response.json()['value']
        print('Here are the message ID9(s):')
        for mailfolder in mailfolder_items:
            email_id = mailfolder['id']
            print(email_id)

