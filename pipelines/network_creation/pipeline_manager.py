import logging
from ..pipeline_manager_base import PipelineManagerBase
from .create_network import CreateNetwork
from .pre_processing import PreProcessing
from .metrics import Metrics
from .create_graph import CreateGraph

logger = logging.getLogger(__name__)


class PipelineManager(PipelineManagerBase):
    def __init__(self, config, input_stage):
        super(PipelineManager, self).__init__(config, input_stage)
        logger.info(f'INIT pipeline for {self.config.dataset_name}')

    def execute(self):
        logger.info(f'EXEC pipeline for {self.config.dataset_name}')

        cn = CreateNetwork(self.config, self.input, self.input_format)
        cn_output, cn_output_format = cn.execute()

        pp = PreProcessing(self.config, cn_output, cn_output_format)
        pp_output, pp_output_format = pp.execute()

        cg = CreateGraph(self.config, pp_output, pp_output_format)
        cg_output, cg_output_format = cg.execute()

        m = Metrics(self.config, cg_output, cg_output_format)
        m_output, m_output_format = m.execute()

        logger.info(f'END pipeline for {self.config.dataset_name}')

        return m_output, m_output_format
