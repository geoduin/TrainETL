class DeliveryPipeline:

    def __init__(self, api_endpoint: str):
        self.endpoint = api_endpoint

    def run(self):
        print("Running Delivery Pipeline")