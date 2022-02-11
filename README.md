ML Engineering Challenge Kueski
Build the Data Pipeline

The feature extraction pipeline got implemented on Pyspark for a more efficient execution, the issue generating the feature “average amount of previous loans” got solved by using Spark’s windows functions, the execution time for the whole dataset got reduced to seconds with the possibility of profiling it if more precision is needed.

This docker image running pyspark was choose as execution environment for the notebook

The following one is the final code, being the executed notebook presented as an attached file. I choose defining the dataframe schema beforehand looking for avoiding possible data type issues when ingesting data.


The features table got stored in two different supports: as a csv file for training purposes and as RDB for serving the features into the model. The csv was chosen as the training process was already built consuming that kind of files, other formats could be useful in further developments depending on the needs of the project (better metadata management, integration with an existing data lake or changes in the volumetry of data).
The RDB was selected for serving the model because of it’s low latency and the batch updating frequency of the features, being them defined in terms of the duration of the loans.
As the model inference only requires the most recent features per customer, a filter is implemented in order to only send into the RDB the registries with the highest number of previous loans per user.

The database got populated with the following lines:
This pipeline lacks quality checks which should be integrated as a process for enhancing data quality and therefore model quality. Unit testing is also required for a good CI/CD process, furthermore I need more context about how data should look like, as during the challenge I saw some negative years values that could probably be errors.

Model Training
The model training pipeline remained mostly the same with the exception of dropping the id column from the dataset, as it didn’t contain any information useful for the model. The trained model got exported as originally proposed on the notebook which enabled us the capability of retraining the model without the need for modifying the API in most cases. The corresponding notebook is attached to this mail.

API proposal
A simple Flask based API is proposed as POC for serving both: features and model inference, the code is the following one:

#-*- coding: utf-8 -*-
import os
from joblib import load
import sqlite3
from flask import Flask, request, jsonify
 
def sql_fetch(con, consulta):
    cursorObj = con.cursor()
    cursorObj.execute("SELECT age, years_on_the_job, nb_previous_loans, avg_amount_loans_previous, flag_own_car FROM features WHERE id = '%s'" % consulta)
    features = cursorObj.fetchall()
    return features
 
app = Flask(__name__)
 
@app.route('/', methods=['POST'])
def makecalc():
    con = sqlite3.connect('features.db')
    data = request.get_json()
    features = sql_fetch(con, data.get("id"))
    print("Esto son los features", features)
    result = model.predict(features)
    print("Este es el resultado de la preddiccion", int(result))
    return jsonify(int(result))
 
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5010))
    model = load('model_risk.joblib')
    print("Starting app on port %d" %port)
    app.run(debug=True, port=port, host="0.0.0.0")

Were the endpoint consumes a simple Json with the ID of the customer:
{
    "id": "5008827"
}

Executes a query in the RDB, with the def sql_fetch function, gathering the corresponding features, and then result = model.predict(features)evaluating the model with those values, finally delivering a 1 or 0 as response.

Automate ML Deployment Pipeline 

In traditional software development DevOps principles have as objective enabling quick and reliable integration of new features on a productive system while maintaining system’s SLA’s along the whole software’s lifecycle.

Data science products lifecycle have the additional complexity of having to deal with not only with a time changing codebase, but also with ever changing data in such a way that exactly the same codebase could deliver broadly differents results on different datasets: phenomena like concept or data drift are an example. MLOps practices aim to help solve these new challenges presented along the whole ML system lifecycle.

CI/CD are a series of processes developed for applying DevOps principles by automating most operations processes and enforcing quality requirements such as versioning, testing, etc. MLOps practices make heavy use of these processes and technologies.

Infrastructure as code (IaC) is the process of automatically provisioning computer resources  from a datacenter by a machine readable text file, a practice that allows the versioning of the whole infrastructure of the system which provides benefits like replicability, reusability, etc.

From my point of view a successful ML pipeline should make use of the former processes and technologies for achieving a more reliable, agile and efficient lifecycle.

In this scenario I’ve made the assumption that the system will be cloud based (AWS in this example but could be others, albeit I only have experience on AWS and Azure).

I also took the former part of the challenge as reference for infrastructure requirements making the following assumptions:
The ground truth of the credit risk model comes weeks after the model inference when the user ends up paying it’s credit or defaulting it, another sources like credit score updates aren’t mentioned in this scenario
Training data came from a well managed data lake, so I’ll assume it doesn’t need additional validations
The models will similar, so there’s no need for specialized hardware
Data volumetry will be stable so feature extraction and training times and computer resources requirements will be the same
Retraining frequency will be low, so no automatic retraining triggering will be required
The whole system should be easily replicable for both: disaster recovery (a whole AWS region crashed) and quick deployment of new instances for different markets
There isn’t any specific compliance requirement neither a defined SLA
The company’s stack includes Python, Spark, Bash and AWS 

According to some experts and ML system should be tested on four different dimensions from which ML infrastructure tests appear to me as the priority for any ML product no matter their scale.
It mostly consist on a infrastructure passing the following requirements:

Training is reproducible. 
Model specs are unit tested. 
The ML pipeline is Integration tested. 
Model quality is validated before serving. 
The model is debuggable. 
Models are canaried before serving. 
Serving models can be rolled back

After the infrastructure proposal I’ll review how this proposal meets those criteria

Considering the former assumptions I propose a pipeline based on a serverless approach,  
which saves the team from a lot of operational overhead while reducing infrastructure costs in most scenarios.
The services proposed are:
AWS Lambda: has a max execution time of 15 min and a max container size of 10 GB, more than enough for running both inference and training
API Gateway: which include versioning and canary release capabilities
AWS Glue:running spark based feature extraction pipeline
Step Functions: Serverless orchestration service 
Cloudwatch/EventBridge: logging, some metrics, triggers and alarms
S3: as data lake
ECR: for container versioning
CloudFormation: as IaC service
CodePipeline: As CI/CD orchestration
Codebuild: As CI/CD server
IAM
SNS: as notification service






The infrastructure diagram could be: 

Where training and inference code are containerized on docker and updated into ECR each commit, this allows consistency of the environment during development and deployment.

Spark’s feature extraction code got executed on a AWS Glue job, a container could be added if needed. The glue job could be configured to be triggered by a cron expression. From my point of view, feature extraction results should be stored in both: in a data lake’s proposed zone along historical features for Data exploration and training purposes, and on a RDS acting as feature storage for online inference. The feature storage could be enhanced by adding a specialized tool, if needed.

The training pipeline will be saving model artifacts on a S3 bucket serving as Model Registry, model traceability could be enhanced by including some tooling like MLFlow. Also folder structure on S3 could easen traceability, for example setting a folder per date with the features and models trained on that date inside.

Eventbrige should keep “waking up” the lambdas every 15 min for reducing it’s latency. Step functions could act as orchestration services between different pipelines.

Depending on the kind of metrics required, those could be calculated directly from CloudWatch (successful API responses, service health checks, etc.) other metrics related to concept and data drift could be calculated on batch from lambda’s logs (Cloudwatch is capable of generating those metrics for SageMaker instances, I’m not sure if that works for lambdas as well). Some examples of metrics could be some statistical test on the distribution if incoming data.
I suggest using AWS stack for CI/CD, using codebuild for building and updating lambda’s containers and CloudFormation’s satcks. I propose using a multi-repository approach, separating the repos in two main classes: one class for container deployment and one for infrastructure deployment as is shown in the diagram

Depending on the IaC framework used (CloudFormation and AWS SAM, or Serverless or Terraform) some configurations and services could not be available or could require additional plugins, in that case I prefer to add those configurations inside CodeBuild using the CLI as needed.

This is an example of a possible Buildspec.yml used for infrastructure management where the yellow highlighted lines are additional configurations 


env:
 parameter-store:
   private_key_github: "/ssh/private_key_github"


phases:
 pre_build:
   commands:
     - if [ "$CODEBUILD_INITIATOR" = "BRANCH-stage" ] ; then
         export GIT_BRANCH="stage";
         export NAME_ENV="stage";
         export DOC_BUCKET="Bucket_docs_stage";
       fi
     - if [ "$CODEBUILD_INITIATOR" = "BRANCH-sandbox" ] ; then
         export GIT_BRANCH="sandbox";
         export NAME_ENV="sandbox";
         export DOC_BUCKET="Bucket_docs_sandbox";
       fi
     - if [ "$CODEBUILD_INITIATOR" = "BRANCH-production" ] ; then
         export GIT_BRANCH="master";
         export NAME_ENV="production";
         export DOC_BUCKET="Bucket_docs-prod";
       fi
     - echo "GIT_BRANCH ========> $GIT_BRANCH"
     - echo Install serverless
     - npm install -g serverless@2.68.0
     - npm install -g json@11.0.0
     - echo "get API_VERSION"
     - export SLS_WARNING_DISABLE="*"
     - export SLS_DEPRECATION_DISABLE="*"
     - export API_VERSION=$(sls print --path provider.environment.API_VERSION --stage $NAME_ENV --format text)
     - echo "API_VERSION ========> $API_VERSION"
 build:
   commands:
     - echo Deploy serverless started on `date`
     - echo Deploy completed on `date`
     - export query="items[?name=='"$NAME_ENV"-api'].[id]"
     - export restApiId=$(aws apigateway get-rest-apis --query $query --output text)
     - cd ..
     - mkdir -p ~/.ssh
     - echo "$private_key_github" > ~/.ssh/id_rsa
     - chmod 0600 ~/.ssh/id_rsa
     - echo Cloning swagger-ui-dist-api Repository  ...
     - git clone REPO/swagger-ui-dist-api.git
     - cd swagger-ui-dist-api
     - git checkout $GIT_BRANCH
     - git pull origin $GIT_BRANCH
     - echo Download swagger documentation
     - aws apigateway get-export --parameters extensions='apigateway' --rest-api-id $restApiId --stage-name $NAME_ENV --export-type swagger swagger.json
     - aws s3 sync ../swagger-ui-dist-api/ "s3://$DOC_BUCKET/$API_VERSION/" --exclude '.git/*'


















































post-build:


 Send some SNS








Where each new model could be versioned along with its model container version and lambda version. Each model is integrated inside the inference container during its build time, so a Step Function could trigger inference update, each time a new training is done in the following way:


Depending on the team's confidence on the new model this could be served totally as a new API/Lambda version generated by CloudFormation, or as Canary or Blue/Green release. This could be achieved in many ways: By leveraging CodeDeploy or by adding an additional configuration on AWS API Gateway which progressively divides traffic between the new and the old API version.

