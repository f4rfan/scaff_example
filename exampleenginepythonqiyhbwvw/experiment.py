
from typing import Dict
from dataproc_sdk import DatioPysparkSession
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from exampleenginepythonqiyhbwvw.bussines_logic.bussines_logic import BussinesLogic
from exampleenginepythonqiyhbwvw.io.init_values import InitValues
import exampleenginepythonqiyhbwvw.common.input as i
import exampleenginepythonqiyhbwvw.common.constants as c
import exampleenginepythonqiyhbwvw.common.output as o

from exampleenginepythonqiyhbwvw.bussines_logic.bussines_logic_answers import BussinesLogicAnswers
from exampleenginepythonqiyhbwvw.io.init_values_cuestionario import InitValuesCuestionario
import exampleenginepythonqiyhbwvw.common.input_cuestionario as ic
import exampleenginepythonqiyhbwvw.common.constants_cuestionario as cc
import exampleenginepythonqiyhbwvw.common.output_cuestionario as oc


class DataprocExperiment:
    """
    Just a wrapper class to ease the user code execution.
    """

    def __init__(self):
        """
        Constructor
        """
        self.__logger = get_user_logger(DataprocExperiment.__qualname__)
        self.__datio_pyspark_session = DatioPysparkSession().get_or_create()

    def run(self, **parameters: Dict) -> None:

        """
        Execute the code written by the user.

        Args:
            parameters: The config file parameters
        """

        self.__logger.info("Executing experiment")

        # LECTURA
        init_values = InitValues()
        clients_df, contracts_df, products_df, output_path, output_schema = init_values.initialize_inputs(parameters)

        #TRANSFORMACIONES
        logic = BussinesLogic()
        filtered_clients_df: DataFrame = logic.filter_by_age_and_vip(clients_df)
        joined_df: DataFrame = logic.join_tables(filtered_clients_df, contracts_df, products_df)
        filtered_by_contracts: DataFrame = logic.filter_by_number_of_contract(joined_df)
        hashed_df: DataFrame = logic.hash_column(filtered_by_contracts)

        final_df: DataFrame = hashed_df\
            .withColumn(o.hash_col.name, f.when(i.activo() == c.FALSE_VALUE, f.lit(c.ZERO_VALUE))
                        .otherwise(o.hash_col()))\
            .withColumn(i.fec_alta.name, i.fec_alta().cast(StringType()))\
            .withColumn(i.edad.name,  i.edad().cast(StringType()))\
            .filter(o.hash_col() == c.ZERO_VALUE)

        # ESCRITURA
        self.__datio_pyspark_session.write().mode(c.OVERWRITE)\
            .option(c.MODE, c.DYNAMIC)\
            .partition_by([i.cod_producto.name, i.activo.name])\
            .datio_schema(output_schema)\
            .parquet(logic.select_all_columns(final_df), output_path)


        # # LECTURA
        # init_values_cuestionario = InitValuesCuestionario()
        #
        # t_fdev_phones, t_fdev_customers, output_path_cuestionario, output_schema_cuestionario = \
        #     init_values_cuestionario.initialize_inputs(parameters)
        #
        # t_fdev_phones.show()
        # t_fdev_phones.printSchema()
        #
        # t_fdev_customers.show()
        # t_fdev_customers.printSchema()
        #
        # #TRANSFORMACIONES
        # logic_answers = BussinesLogicAnswers()
        # print("Respuesta 3 cantidad de registros")
        # print("Registros t_fdev_phones: {} \nRegistros t_fdev_customers: {} \n ".format(t_fdev_phones.count(),
        #                                                                                 t_fdev_customers.count()))
        #
        # phones_filteres = logic_answers.regla_1(t_fdev_phones)
        # print("Registros filtrados t_fdev_phones: {} ".format(phones_filteres.count()))
        #
        # customers_filtered = logic_answers.regla_2(t_fdev_customers)
        # print("Registros filtrados t_fdev_customers: {} ".format(customers_filtered.count()))
        #
        # regla_3 = logic_answers.regla_3(customers_filtered, phones_filteres)
        # print("Registros en regla 3: {} ".format(regla_3.count()))
        #
        # regla_4 = logic_answers.regla_4(regla_3)
        # print("Registros en regla 4: {} ".format(regla_4.filter(oc.customer_vip() == cc.YES_VALUE).count()))
        #
        # regla_5 = logic_answers.regla_5(regla_4)
        # print("Registros en regla 5: {} ".format(regla_5.filter(oc.extra_discount() > cc.ZERO_VALUE).count()))
        #
        # regla_6 = logic_answers.regla_6(regla_5)
        # regla_6.select(f.avg("final_price")).show()
        #
        # regla_7 = logic_answers.regla_7(regla_6)
        # print("Registros en regla 7: {} ".format(regla_7.count()))
        #
        # regla_8 = logic_answers.regla_8(regla_7)
        # print("Registros en regla 8: {} ".format(regla_7.filter(ic.nfc().isNull()).count()))
        #
        # regla_9 = logic_answers.regla_9(regla_8, parameters)
        # regla_9.show()
        #
        # regla_10 = logic_answers.regla_10(regla_9)
        # regla_10.show()
        #
        # # ESCRITURA
        # self.__datio_pyspark_session.write().mode(cc.OVERWRITE) \
        #     .option(cc.MODE, cc.DYNAMIC) \
        #     .partition_by([oc.jwk_date.name]) \
        #     .datio_schema(output_schema_cuestionario) \
        #     .parquet(logic_answers.select_all_columns(regla_10), output_path_cuestionario)
