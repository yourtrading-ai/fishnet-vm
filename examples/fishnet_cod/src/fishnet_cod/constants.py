FISHNET_MESSAGE_CHANNEL = "FISHNET_TEST"
FISHNET_DEPLOYMENT_CHANNEL = "FISHNET_TEST_DEPLOYMENT"

EXECUTOR_MESSAGE_FILTER = [
    {
        "channel": FISHNET_MESSAGE_CHANNEL,
        "type": "POST",
        "post_type": ["Execution", "amend"],
    }
]
