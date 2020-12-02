### Application Overview 
This application takes two data files i.e. students.csv and teachers.parquet as input and process them to generate a json output file. The output json file consists the list of teachers with their respective students mapped by class_id.

Application supports both local file system and AWS S3 File storage to read the input data files, to use the local file storage, data files should be stored in dataset directory within the application folder and to use S3 storage AWS credential should be stored in environment variables.

Also the code is written by considering an optimal tradeoff between the space and time complexity of the code and the optimal performance of the multiple approaches to perform the task.
 
 
### Modules used 
- os        : to access files from local directory
- json      : to output a proper json formatted data
- pyarrow   : to access parquet files 
- pandas    : for processing csv and parquet files
- boto3     : for accessing S3 bucket files and exception handling corresponding to S3 functionalities
- dotenv    : to load environment variables from .env file

### Application setup

##### Clone repository
```git
git clone <url>
```

##### Create .env file
```
touch .env
```
set environment variables in .env i.e. 
- AWS_SECRET_ACCESS_KEY=""
- AWS_ACCESS_KEY_ID=""
- AWS_REGION_NAME=""

##### Create virtual environment 
```bash
virtualenv -p python3 env
```
 
##### Activate environment 
```bash
source env/bin/activate
```

##### Install requirements
```bash
pip install -r requirements.txt
``` 

##### Command to run app
```python
python src/main.py
```

### Docker commands 
```dockerfile
$ docker build -t my-app .
$ docker run -it --rm --name my-running-app my-app
```
