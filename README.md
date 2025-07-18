# MDF Connect
The Materials Data Facility Connect service is the ETL flow to deeply index datasets into MDF Search. It is not intended to be run by end-users. To submit data to the MDF, visit the [Materials Data Facility](https://materialsdatafacility.org).

# Architecture
The MDF Connect service is a serverless REST service that is deployed on AWS. 
It consists of an AWS API Gateway that uses a lambda function to authenticate
requests against GlobusAuth. If authorised, the endpoints trigger AWS lambda
functions. Each endpoint is implemented as a lambda function contained in a 
python file in the [aws/](aws/) directory. The lambda functions are deployed
via GitHub actions as described in a later section.

The API Endpoints are:
* [POST /submit](aws/submit.py): Submits a dataset to the MDF Connect service. This triggers a Globus Automate flow
* [GET /status](aws/status.py): Returns the status of a dataset submission
* [POST /submissions](aws/submissions.py): Forms a query and returns a list of submissions

# Globus Automate Flow
The Globus Automate flow is a series of steps that are triggered by the POST 
/submit endpoint. The flow is defined using a python dsl that can be found 
in [automate/minimus_mdf_flow.py](automate/minimus_mdf_flow.py). At a high 
level the flow:
1. Notifies the admin that a dataset has been submitted
2. Checks to see if the data files have been updated or if this is a metadata only submission
3. If there is a dataset, it starts a globus transfer
4. Once the transfer is complete it may trigger a curation step if the organization is configured to do so
5. A DOI is minted if the organization is configured to do so
6. The dataset is indexed in MDF Search
7. The user is notified of the completion of the submission


# Development Workflow
Changes should be made in a feature branch based off of the dev branch. Create 
PR and get a friend to review your changes. Once the PR is approved, merge it
into the dev branch. The dev branch is automatically deployed to the dev
environment. Once the changes have been tested in the dev environment, create a
PR from dev to main. Once the PR is approved, merge it into main. The main
branch is automatically deployed to the prod environment.

# Deployment
The MDF Connect service is deployed on AWS into development and production
environments. The automate flow is deployed into the Globus Automate service via
a second GitHub action.

## Deploy the Automate Flow
Changes to the automate flow are deployed via a GitHub action, triggered by the
push of a new GitHub release. If the release is tagged as "pre-release" it will
be deployed to the dev environment, otherwise it will be deployed to the prod
environment.

The flow IDs for dev and prod are stored in 
[automate/mdf_dev_flow_info.json](automate/mdf_dev_flow_info.json) and 
[automate/mdf_prod_flow_info.json](automate/mdf_prod_flow_info.json) 
respectively. The flow ID is stored in the `flow_id` key.

### Deploy a Dev Release of the Flow
1. Merge your changes into the `dev` branch
2. On the GitHub website, click on the _Release_ link on the repo home page.
3. Click on the _Draft a new release_ button
4. Fill in the tag version as `X.Y.Z-alpha.1` where X.Y.Z is the version number. You can use subsequent alpha tags if you need to make further changes.
5. Fill in the release title and description
6. Select `dev` as the Target branch
7. Check the _Set as a pre-release_ checkbox
8. Click the _Publish release_ button

### Deploy a Prod Release of the Flow
1. Merge your changes into the `main` branch
2. On the GitHub website, click on the _Release_ link on the repo home page.
3. Click on the _Draft a new release_ button
4. Fill in the tag version as `X.Y.Z` where X.Y.Z is the version number. 
5. Fill in the release title and description
6. Select `main` as the Target branch
7. Check the _Set as the latest release_ checkbox
8. Click the _Publish release_ button

You can verify deployment of the flows in the 
[Globus Automate Console](https://app.globus.org/flows/library).


## Deploy the MDF Connect Service
The MDF Connect service is deployed via a GitHub action. The action is triggered
by a push to the dev or main branch. The action will deploy the service to the
dev or prod environment respectively.

## Updating Schemas
Schemas and the MDF organization database are managed in the automate branch
of the [Data Schemas Repo](https://github.com/materials-data-facility/data-schemas/tree/automate).

The schema is deployed into the docker images used to serve up the lambda 
functions. 

## Reviewing Logs
- [Dev Logs](https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/$252Faws$252Flambda$252FMDF-Connect2-submit-dev/log-events/)
- [Prod Logs](https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/$252Faws$252Flambda$252FMDF-Connect2-submit-prod/log-events/)


# Running Tests
To run the tests first make sure that you are running python 3.7.10. Then install the dependencies:

    $ cd aws/tests
    $ pip3 install -r requirements-test.txt

Now you can run the tests using the command:

    $ PYTHONPATH=.. python -m pytest --ignore schemas

# Support
This work was performed under financial assistance award 70NANB14H012 from U.S. Department of Commerce, National Institute of Standards and Technology as part of the [Center for Hierarchical Material Design (CHiMaD)](http://chimad.northwestern.edu). This work was performed under the following financial assistance award 70NANB19H005 from U.S. Department of Commerce, National Institute of Standards and Technology as part of the Center for Hierarchical Materials Design (CHiMaD). This work was also supported by the National Science Foundation as part of the [Midwest Big Data Hub](http://midwestbigdatahub.org) under NSF Award Number: 1636950 "BD Spokes: SPOKE: MIDWEST: Collaborative: Integrative Materials Design (IMaD): Leverage, Innovate, and Disseminate".


