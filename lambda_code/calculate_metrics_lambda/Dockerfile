# bigger image
# FROM public.ecr.aws/sam/build-python3.10:latest
# smaller image
FROM public.ecr.aws/lambda/python:3.10

COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r requirements.txt

# COPY handler.py /asset-output
COPY handler.py ${LAMBDA_TASK_ROOT}
