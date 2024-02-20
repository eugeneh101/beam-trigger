import json

import cdk_ecr_deployment as ecr_deploy
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_ecr as ecr,
    aws_ecr_assets as ecr_assets,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_lambda as _lambda,
    aws_s3 as s3,
)
from constructs import Construct


class BeamTriggerStack(Stack):

    def __init__(
        self, scope: Construct, construct_id: str, environment: dict, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.s3_bucket = s3.Bucket(
            self,
            "S3Bucket",
            bucket_name=environment["S3_BUCKET_NAME"],
            event_bridge_enabled=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        self.eventbridge_rule_trigger_file_creation = events.Rule(
            self,
            "TriggerS3FileCreation",
            rule_name=f"{environment['CREATE_S3_FILE_LAMBDA']}-rule",  # hard coded pattern
            schedule=events.Schedule.rate(Duration.minutes(1)),
        )
        self.eventbridge_rule_file_created = events.Rule(
            self,
            "S3FileCreated",
            rule_name=f"{environment['CALCULATE_METRICS_LAMBDA_NAME']}-rule",  # hard coded pattern
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [environment["S3_BUCKET_NAME"]]},
                    "object": {"key": [{"prefix": environment["S3_INPUT_PREFIX"]}]},
                },
            ),
        )

        self.create_s3_file_lambda = _lambda.Function(
            self,
            "CreateS3FileLambda",
            function_name=environment["CREATE_S3_FILE_LAMBDA"],
            handler="handler.lambda_handler",
            memory_size=1024,  # depends on how large S3 file you want to write
            timeout=Duration.seconds(60),  # run for a minute
            runtime=_lambda.Runtime.PYTHON_3_10,
            environment={
                "S3_BUCKET_NAME": environment["S3_BUCKET_NAME"],
                "S3_INPUT_PREFIX": environment["S3_INPUT_PREFIX"],
                "CREATE_FILE_EVERY_X_SECONDS": json.dumps(
                    environment["CREATE_FILE_EVERY_X_SECONDS"]
                ),
                "NUM_LINES_IN_S3_FILE": json.dumps(environment["NUM_LINES_IN_S3_FILE"]),
            },
            code=_lambda.Code.from_asset(
                "lambda_code/create_s3_file_lambda",
                exclude=[".venv/*"],
            ),
        )

        # doesn't work as Apache Beam too large to be packaged in <250 MB
        # self.calculate_metrics_lambda = _lambda.Function(
        #     self,
        #     "CalculateMetricsLambda",
        #     function_name=environment["CALCULATE_METRICS_LAMBDA_NAME"],
        #     handler="handler.lambda_handler",
        #     memory_size=128,  # depends on the input file size
        #     timeout=Duration.seconds(60),  # depends on the input file size
        #     runtime=_lambda.Runtime.PYTHON_3_10,
        #     environment={
        # "S3_BUCKET_NAME": environment["S3_BUCKET_NAME"],
        # "S3_OUTPUT_PREFIX_LAMBDA": environment["S3_OUTPUT_PREFIX_LAMBDA"],
        #     },
        #     code=_lambda.Code.from_asset(
        #         "lambda_code/calculate_metrics_lambda",
        #         # exclude=[".venv/*"],  # seems to no longer do anything if use BundlingOptions
        #         bundling=BundlingOptions(
        #             image=_lambda.Runtime.PYTHON_3_10.bundling_image,
        #             command=[
        #                 "bash",
        #                 "-c",
        #                 " && ".join(
        #                     [
        #                         "pip install -r requirements.txt -t /asset-output",
        #                         "cp handler.py /asset-output",  # need to cp instead of mv
        #                         "sleep 100" ###
        #                     ]
        #                 ),
        #             ],
        #         ),
        #     ),
        # )
        # self.calculate_metrics_lambda = _lambda.DockerImageFunction(
        #     self,
        #     "CalculateMetricsLambda",
        #     function_name=environment["CALCULATE_METRICS_LAMBDA_NAME"],
        #     memory_size=2048,  # depends on the input file size
        #     timeout=Duration.seconds(60),  # depends on the input file size
        #     environment={
        #         "S3_BUCKET_NAME": environment["S3_BUCKET_NAME"],
        #         "S3_OUTPUT_PREFIX_LAMBDA": environment["S3_OUTPUT_PREFIX_LAMBDA"],
        #     },
        #     code=_lambda.DockerImageCode.from_image_asset(
        #         directory="lambda_code/calculate_metrics_lambda",
        #         cmd=["handler.lambda_handler"],
        #     ),
        # )
        self.calculate_metrics_lambda_repo = ecr.Repository(
            self,
            "CalculateMetricsLambdaRepo",
            repository_name=f"{environment['CALCULATE_METRICS_LAMBDA_NAME']}-repo",  # hard coded pattern
            auto_delete_images=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        task_asset = ecr_assets.DockerImageAsset(
            self,
            "CalculateMetricsLambdaEcrImage",
            directory="lambda_code/calculate_metrics_lambda",
        )  # uploads to `container-assets` ECR repo
        deploy_repo = ecr_deploy.ECRDeployment(  # upload to desired ECR repo
            self,
            "PushCalculateMetricsLambdaEcrImage",
            src=ecr_deploy.DockerImageName(task_asset.image_uri),
            dest=ecr_deploy.DockerImageName(
                self.calculate_metrics_lambda_repo.repository_uri
            ),
        )
        self.calculate_metrics_lambda = _lambda.DockerImageFunction(
            self,
            "CalculateMetricsLambda",
            function_name=environment["CALCULATE_METRICS_LAMBDA_NAME"],
            memory_size=2048,  # depends on the input file size
            timeout=Duration.seconds(60),  # depends on the input file size
            environment={
                "S3_BUCKET_NAME": environment["S3_BUCKET_NAME"],
                "S3_OUTPUT_PREFIX_LAMBDA": environment["S3_OUTPUT_PREFIX_LAMBDA"],
            },
            code=_lambda.DockerImageCode.from_ecr(
                self.calculate_metrics_lambda_repo,
                cmd=["handler.lambda_handler"],
            ),
        )
        self.calculate_metrics_lambda.node.add_dependency(
            deploy_repo  # deploy repo before Lambda
        )

        # connect AWS resources together
        self.s3_bucket.grant_write(self.create_s3_file_lambda)
        self.s3_bucket.grant_read_write(self.calculate_metrics_lambda)
        self.eventbridge_rule_trigger_file_creation.add_target(
            events_targets.LambdaFunction(self.create_s3_file_lambda)
        )
        self.eventbridge_rule_file_created.add_target(
            events_targets.LambdaFunction(self.calculate_metrics_lambda)
        )
        self.eventbridge_rule_trigger_file_creation.node.add_dependency(
            self.calculate_metrics_lambda
        )  # don't start Eventbridge rule before Lambda is created
