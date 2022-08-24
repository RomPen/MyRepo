import s3fs, boto3, os

s3resource = boto3.resource('s3')
s3client = boto3.client('s3')
s3fs = s3fs.S3FileSystem()

bucket = 'your_bucket'

class S3FileManagement():
    
    """
    A class that allows to copy/delete/rename a folder in S3 bucket
    Can also provide info about the folder (number of files, folder size, etc.)
    Should not be used on large folders 
    
    #-------------Functions------------------------   

    Step 1 (Compulsory) - Define the class:
    
    -Pass folder that you want to rename/copy/delete as an argument:
    
    >>> test = S3FileManagement(folder = 'folder1/folder')  


    ### Copy folder ###
    
    Need to pass new folder name as an argument:
    
    >>> test.copy_folder('folder1/folder2')


    ### Delete folder ###
    
    >>> test.delete_folder()


    ### Rename folder ###
    -Need new folder name argument
    -Copies original folder with the new name and then deletes it
    
    >>> test.rename_folder('folder1/folder2_new)

    #---------Useful attributes of the class---------

    ### List of all the files detected ###
    >>> test.files

    ### Count of all files in the folder ###
    >>> test.file_count

    ### Folder size in MB (delete by 1024 to get GB) ###
    >>> test.folder_size

    ### Folder size in bytes ###
    >>> test.folder_size_bytes
    """
    
    
    def __init__(self, folder, bucket = bucket): 
        
        self.bucket = bucket
        self.folder = folder if folder.endswith('/') else f"{folder}/"
        self.folder_split = folder.rstrip('/').split('/')
        
        self.get_files()
        
        assert self.files, f"No files detected. Folder {self.folder} possibly does not exists"
        
        self.folders_detected = set([x['Key'].split('/')[0] for x in self.files]) 
        self.subfolders = set([x.split('/')[1] for x in 
                               filter(lambda l: len(l.split("/")) > 2,
                                      [x['Key'] for x in self.files])]) 
        
        assert len(self.folders_detected) == 1, f"Detected {len(self.folders_detected)} folders: {self.folders_detected} Only 1 folder allowed at a time" 
        if 0 < len(self.subfolders) <= 10:
            sub_str = '\n'.join(self.subfolders)
            print(f"Subfolders detected:\n{sub_str}")
    
    def _get_all_s3_objects(self, **kwargs):

        assert 'Prefix' in kwargs, "Please provide folder name as Prefix variable, i.e. Prefix = 'folder'"

        continuation_token = None
        is_truncated = True
        kwargs['Bucket'] = self.bucket
        
        while is_truncated:

            if continuation_token:
                kwargs['ContinuationToken'] = continuation_token

            response = s3client.list_objects_v2(**kwargs)

            yield from response.get('Contents', [])

            is_truncated = response['IsTruncated']

            continuation_token = response.get('NextContinuationToken')
    
    def _get_folder_size(self):
        
        for x in self.files:
            yield x['Size']
             
    def get_files(self):
        
        self.files = []
        
        for file in self._get_all_s3_objects(Prefix = self.folder):
            self.files.append(file)
        
        self.file_keys = [x['Key'] for x in self.files]
        
        self.file_count = len(self.files)
        self.folder_size_bytes = sum(self._get_folder_size())
        self.folder_size = self.folder_size_bytes/1024/1024
        
        print(f"Folder {self.folder!r} contains {self.file_count:,} files")
        print(f"Total folder size: {self.folder_size:,.2f} MB ({self.folder_size/1024:,.2f} GB)")
    
    def delete_folder(self):
        
        assert self.folder, 'No folder specified'
        
        resp = input(f"DELETING {self.folder!r} folder...\nAre you sure you want to proceed [y/n]:\t")
        
        if resp.lower() == 'y':
            
            self.delete_count = 0
            for file in self.files:
                
                s3client.delete_object(Bucket=bucket, Key=file['Key'])
                self.delete_count += 1
                
                print(f"\rStatus: {self.delete_count:,}/{self.file_count:,}\t\t\t", end = "", flush = True)
            

            print(f"Deleted {self.delete_count} item(s)")
        
        else:
            return 
            
    def copy_folder(self, other_folder):
        
        other_folder = other_folder if other_folder.endswith('/') else other_folder + '/' 
        print(f"Copying files from {self.folder}/ to {other_folder}...")
        self.copy_count = 0
        for file in self.files:
            copy_source = {'Bucket' : self.bucket,
                           'Key'    : file['Key']}
            
            other_key = other_folder + '/'.join(file['Key'].split('/')[len(self.folder_split):])
            s3client.copy(copy_source, self.bucket, other_key)
            file['CopiedTo'] = other_key
            self.copy_count += 1
            print(f"\rStatus: {self.copy_count:,}/{self.file_count:,}\t\t\t", end = "", flush = True)
        
        print('DONE')
        
    def rename_folder(self, other_folder):
        
        self.copy_folder(other_folder)
        
        cls = type(self)
        other = cls(other_folder)
        assert other.file_count == self.file_count, f"File count between {self.folder} and {other_folder} does not match. {self.folder!r}: {self.file_count}, {other_folder}: {other.file_count}"
        assert round(other.folder_size, 2) == round(self.folder_size, 2), f"Folder size between {self.folder} and {other_folder} does not match. {self.folder!r}: {self.folder_size}, {other_folder}: {other.folder_size}"
        
        self.delete_folder()
