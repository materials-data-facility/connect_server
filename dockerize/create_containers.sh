cp ../aws/requirements.txt auth
cp ../aws/requirements.txt submit
cp ../aws/requirements.txt status

cp dockerfile_template auth/DockerFile
cp dockerfile_template submit/DockerFile
cp dockerfile_template status/DockerFile


cp ../aws/globus-auth.py auth/lambda_function.py
cp ../aws/submit_dataset.py submit/lambda_function.py
cp ../aws/submit_status.py status/lambda_function.py
cp ../aws/source_id_manager.py ../aws/utils.py ../aws/dynamo_manager.py ../aws/automate_manager.py ../aws/globus_automate_flow.py ../aws/flow_action.py ../aws/organization.py ../aws/globus_auth_manager.py ../aws/mdf_flow_info.json submit


cp ../aws/source_id_manager.py ../aws/utils.py ../aws/dynamo_manager.py ../aws/automate_manager.py ../aws/globus_automate_flow.py ../aws/flow_action.py ../aws/organization.py ../aws/globus_auth_manager.py ../aws/mdf_flow_info.json status
