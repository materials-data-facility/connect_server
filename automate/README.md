# Deploying a Flow
1. Take a look at `create_new_flow.py` and make and neccessary adjustments to the title and other settings.
2. Run `python create_new_flow.py` to create a new flow.
3. Note the flow id and scope that is printed to the console.
4. Update the `FLOW_ID` and `FLOW_SCOPE` variables in `lambda_environment_vars.tf` in the infra directory with the values from the previous step.

## Deploying a Flow from the Command Line
1. Cd into the `automate` directory.
2. `export $(cat ../secrets.env ) PYTHONPATH=../aws && python deploy_mdf_flow.py dev 1.0.0-rc.10`
3. The first argument is the environment and the second is the version of the flow to deploy.
