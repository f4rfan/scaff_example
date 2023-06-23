from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Window
from typing import Dict
from pyspark.sql.types import StringType, IntegerType, DecimalType

import exampleenginepythonqiyhbwvw.common.input_cuestionario as i
import exampleenginepythonqiyhbwvw.common.constants_cuestionario as c
import exampleenginepythonqiyhbwvw.common.output_cuestionario as o

class BussinesLogicAnswers:
    """
    Just a wrapper class to ease the user code execution.
    """

    def __init__(self):
        """
        Constructor
        """
        self.__logger = get_user_logger(BussinesLogicAnswers.__qualname__)

    def regla_1(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtering by date, brand and country")
        return df.filter((i.cutoff_date() >= c.INIT_DATE)
                         & (i.cutoff_date() <= c.FINAL_DATE)
                         & ~(i.brand().isin(c.DELL, c.COOLPAD, c.CHEA, c.BQ, c.BLU))
                         & ~(i.country_code().isin(c.CH, c.IT, c.CZ, c.DK))
                         )

    def regla_2(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtering by date and credit card")
        return df.filter((i.gl_date() >= c.INIT_DATE)
                         & (i.gl_date() <= c.FINAL_DATE)
                         & (f.length(i.credit_card_number()) < c.SEVENTEEN_NUMBER)
                         )

    def regla_3(self, df1: DataFrame, df2: DataFrame) -> DataFrame:
        self.__logger.info("Joining previous results and filtering ")
        return df2.join(df1, [i.customer_id.name, i.delivery_id.name], c.INNER_TYPE)\
            .filter((i.customer_id().isNotNull()) | (i.delivery_id().isNotNull()))

    def regla_4(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Adding new column: customer_vip")
        return df.select(*df.columns,
                         f.when(((i.prime() == c.YES_VALUE) &
                                 (i.price_product() >= c.SEVENTY_FIVE_HUNDRED_NUMBER)), c.YES_VALUE)
                         .otherwise(c.NO_VALUE).alias(o.customer_vip.name))

    def regla_5(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Adding new column: extra_discount")
        return df.select(*df.columns,
                         f.when(((i.prime() == c.YES_VALUE)
                                 & (i.stock_number() < c.THIRTY_FIVE_NUMBER)
                                 & ~(i.brand().isin(c.XOLO, c.SIEMENS, c.PANASONIC, c.BLACKBERRY))),
                                i.price_product()*c.POINT_TEN_VALUE)
                         .otherwise(c.ZERO_VALUE)
                         .alias(o.extra_discount.name))

    def regla_6(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Adding new column: final_price")
        return df.select(*df.columns,
                         (i.price_product()
                          + i.taxes()
                          - i.discount_amount()
                          - o.extra_discount()).alias(o.final_price.name)
                         )

    def regla_7(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Adding new column: promo")
        return df.select(*df.columns,
                         f.dense_rank().over(Window
                                             .partitionBy(i.brand.name)
                                             .orderBy(f.desc(o.final_price.name))
                                             ).alias(o.brands_top.name))\
            .filter(o.brands_top() <= c.FIFTY_VALUE)

    def regla_8(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filling null values ")
        return df.na.fill(value=c.NO_VALUE, subset=[i.nfc.name])

    def regla_9(self, df: DataFrame, params: Dict) -> DataFrame:
        self.__logger.info("Adding new column: JWK_DATE")
        return df.select(*df.columns, f.lit(params[c.JWK_DATE]).alias(o.jwk_date.name))

    def regla_10(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Adding new column: AGE")
        return df.select(*df.columns, (f.months_between(f.current_date(), i.birth_date())/f.lit(c.TWELVE_VALUE))
                         .alias(o.age.name))

    def select_all_columns(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Output dataframe")
        return df.select(o.city_name().cast(StringType()),
                         o.street_name().cast(StringType()),
                         o.credit_card_number().cast(StringType()),
                         o.last_name().cast(StringType()),
                         o.first_name().cast(StringType()),
                         o.age().cast(IntegerType()),
                         o.brand().cast(StringType()),
                         o.model().cast(StringType()),
                         o.nfc().cast(StringType()),
                         o.country_code().cast(StringType()),
                         o.prime().cast(StringType()),
                         o.customer_vip().cast(StringType()),
                         o.taxes().cast(DecimalType(c.PRECISION, c.SCALE)),
                         o.price_product().cast(DecimalType(c.PRECISION, c.SCALE)),
                         o.discount_amount().cast(DecimalType(c.PRECISION, c.SCALE)),
                         o.extra_discount().cast(DecimalType(c.PRECISION, c.SCALE)),
                         o.final_price().cast(DecimalType(c.PRECISION, c.SCALE)),
                         o.brands_top().cast(IntegerType()),
                         f.to_date(o.jwk_date(), c.DATE_FORMAT).alias(o.jwk_date.name)
                         )
