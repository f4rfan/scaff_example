from dataproc_sdk import DatioPysparkSession, DatioSchema
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
import exampleenginepythonqiyhbwvw.common.constants_cuestionario as c


class InitValuesCuestionario:

    def __init__(self):
        self.__logger = get_user_logger(InitValuesCuestionario.__qualname__)
        self.__datio_pyspark_session = DatioPysparkSession().get_or_create()

    def initialize_inputs(self, parameters):
        self.__logger.info("Using given configuration")
        t_fdev_phones_df = self.get_input_df(parameters, c.PHONES_PATH, c.PHONES_SCHEMA)
        t_fdev_customers_df = self.get_input_df(parameters, c.CUSTOMER_PATH, c.CUSTOMER_SCHEMA)

        output_path, output_schema = \
            self.get_config_by_name(parameters, c.OUTPUT_PATH, c.OUTPUT_SCHEMA)

        return t_fdev_phones_df, t_fdev_customers_df, output_path + c.FINAL_TABLE, output_schema

    def get_config_by_name(self, parameters, key_path, key_schema):
        self.__logger.info("Get config for " + key_path)
        io_path = parameters[key_path]
        io_schema = DatioSchema.getBuilder().fromURI(parameters[key_schema]).build()
        return io_path, io_schema

    def get_input_df(self, parameters, key_path, key_schema):
        self.__logger.info("Reading from " + key_path)
        io_path, io_schema = self.get_config_by_name(parameters, key_path, key_schema)
        return self.__datio_pyspark_session.read().datioSchema(io_schema)\
            .option(c.HEADER, c.TRUE_VALUE) \
            .parquet(io_path)
