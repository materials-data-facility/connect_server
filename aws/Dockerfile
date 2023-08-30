FROM public.ecr.aws/lambda/python:3.10
ARG LAMBDA_SCRIPT
ENV LAMBDA_HANDLER="${LAMBDA_SCRIPT}.lambda_handler"
RUN echo "${LAMBDA_HANDLER}"

# Install the function's dependencies using file requirements.txt
# from your project folder.
COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY *.py ${LAMBDA_TASK_ROOT}

# Problems with passing buuld arg through to the CMD. Use this trick
# to pass in to entrypoint script from
# https://stackoverflow.com/a/75671905
ENTRYPOINT /lambda-entrypoint.sh ${LAMBDA_HANDLER}
