import aws_cdk as cdk

from beam_trigger import BeamTriggerStack


app = cdk.App()
environment = app.node.try_get_context("environment")
env = cdk.Environment(region=environment["AWS_REGION"])
BeamTriggerStack(
    app,
    "BeamTriggerStack",
    environment=environment,
    env=env,
)
app.synth()
