FROM public.ecr.aws/ons-spp/python-lambda-poetry:3.12
ENTRYPOINT [ "/lambda-entrypoint.sh" ]

WORKDIR ${LAMBDA_TASK_ROOT}

# Copy pyproject.toml / poetry.loc
COPY pyproject.toml ${LAMBDA_TASK_ROOT}
COPY poetry.lock ${LAMBDA_TASK_ROOT}

# Install the specified packages
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev

ENV PYTHONPATH="$PYTHONPATH:${LAMBDA_TASK_ROOT}"

# Copy function code. This copies EVERYTHING inside the app folder to the lambda root.
COPY ./app ${LAMBDA_TASK_ROOT}/app

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
# Since we copied 
CMD [ "app.main.handler" ]