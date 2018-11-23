class PipelineManagerBase:
    def __init__(self, config, input_stage):
        self.config = config
        self.input = input_stage[0]
        self.input_format = input_stage[1]

    def execute(self):
        pass
