import os
import tempfile
import uuid

import apache_beam as beam
import boto3

S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
S3_OUTPUT_PREFIX_LAMBDA = os.environ["S3_OUTPUT_PREFIX_LAMBDA"]

s3_resource = boto3.resource("s3")


def write_file(content: str, dir_name: str) -> str:
    assert isinstance(content, str) and content, content
    filename = os.path.join(dir_name, f"beam_result_{uuid.uuid4()}.txt")
    with open(filename, "w") as f:
        f.write(content)
    return filename


def lambda_handler(event, context) -> None:
    assert event["source"] == "aws.s3"
    assert event["detail-type"] == "Object Created"
    assert event["detail"]["bucket"]["name"] == S3_BUCKET_NAME
    key = event["detail"]["object"]["key"]

    # s3_file = s3_resource.Object(bucket_name=S3_BUCKET_NAME, key=key)
    # file_content_bytes = s3_file.get()["Body"].read()
    # print(file_content_bytes)
    # [line for line in s3_file.get()["Body"].iter_lines(keepends=True)]  # generator uses less RAM

    with tempfile.TemporaryDirectory(prefix="beam_job_") as temp_dir:
        # pipeline_options = PipelineOptions(
        #     direct_running_mode="in_memory",
        #     direct_num_workers=1,
        #     num_workers=1,
        #     max_num_workers=1,
        #     parallelism=1,
        #     max_parallelism=1,
        # )
        # pipeline = beam.Pipeline(options=pipeline_options)
        pipeline = beam.Pipeline()
        word_count = (
            pipeline
            | "Read from S3" >> beam.io.ReadFromText(f"s3://{S3_BUCKET_NAME}/{key}")
            | "Lower and flatten" >> beam.FlatMap(lambda string: string.lower().split())
            | "Count words" >> beam.combiners.Count.PerElement()
            # sort word counts in descending order and word in ascending order, ie alphabetically
            | "Add useless key" >> beam.Map(lambda count: (None, count))
            | "Group by useless key" >> beam.GroupByKey()
            | "Sort counts"
            >> beam.Map(
                lambda result: sorted(result[1], key=lambda tup: (-tup[1], tup[0]))
            )
            # | "To string" >> beam.ToString.Element()
            | "To string" >> beam.Map(lambda lst: "\n".join(str(tup) for tup in lst))
            # | "Save results to S3" >> beam.io.WriteToText(  # is authenticated to write to S3
            #     file_path_prefix=f"s3://{S3_BUCKET_NAME}/{S3_OUTPUT_PREFIX_LAMBDA}/",
            #     file_name_suffix=os.path.basename(key),
            #     num_shards=1,  # AWS Lambda cannot use multiprocessing
            # )
            # | "Write to disk" >> beam.io.WriteToText(
            #     file_path_prefix=f"{temp_dir}/{S3_OUTPUT_PREFIX_LAMBDA}/",
            #     file_name_suffix=os.path.basename(key),
            #     num_shards=1,  # AWS Lambda cannot use multiprocessing
            # )
            | "Write to temp dir"  # intentional side effect
            >> beam.Map(lambda content: write_file(content=content, dir_name=temp_dir))
        )
        result = pipeline.run()
        result.wait_until_finish()  # may comment this out in streaming pipelines

        filenames = os.listdir(temp_dir)
        assert len(filenames) == 1  # due to GroupByKey with 1 key
        filename = os.path.join(temp_dir, filenames[0])
        results_key = f"{S3_OUTPUT_PREFIX_LAMBDA}/{os.path.basename(key)}"
        s3_resource.Bucket(S3_BUCKET_NAME).upload_file(filename, results_key)
        print(
            f"Read s3://{S3_BUCKET_NAME}/{key} and stored metrics at "
            f"s3://{S3_BUCKET_NAME}/{results_key}"
        )
